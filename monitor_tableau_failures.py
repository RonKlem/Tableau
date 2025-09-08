import json
import logging
import requests
import sys

from datetime import datetime, timedelta
from retrying import retry
import tableauserverclient as TSC
from tableauserverclient import RequestOptions, Filter

# ─── Load Config & Logging ─────────────────────────────────────────────────────

cf = json.load(open('config.json'))

logging.basicConfig(
    filename = cf.get('LogFile', 'tableau_status_check.log'),
    level    = logging.INFO,
    format   = '%(asctime)s - %(levelname)s - %(message)s',
    filemode = 'a'
)

# ─── Constants ─────────────────────────────────────────────────────────────────

TIMEFRAME_HOURS    = cf.get('TimeframeHours', 4)
JOB_TYPE_EXTRACT   = 'RefreshExtract'
JOB_TYPE_FLOW      = 'RunFlow'

# ─── Authentication Helpers ────────────────────────────────────────────────────

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def sign_in(server_url, token_name, token_secret, site_id):
    server = TSC.Server(server_url, use_server_version=True)
    # Use your PEM path or boolean flag
    server.add_http_options({'verify': cf.get('http_options', True)})
    auth = TSC.PersonalAccessTokenAuth(token_name, token_secret, site_id)
    server.auth.sign_in(auth)
    logging.info(f"Signed in to {server_url} (site: '{site_id}')")
    return server

def sign_out(server):
    try:
        server.auth.sign_out()
        logging.info("Signed out")
    except Exception as e:
        logging.warning(f"Sign-out issue: {e}")

# ─── Project & Resource Discovery ─────────────────────────────────────────────

def collect_project_ids(server, root_names):
    """
    Return a list of project IDs for every project named in root_names
    plus all their descendant subprojects.
    """
    all_projects, _ = server.projects.get()
    # Map parent → [child_ids]
    children_map = {}
    for proj in all_projects:
        if proj.parent_id:
            children_map.setdefault(proj.parent_id, []).append(proj.id)

    selected_ids = set()
    for root_name in root_names:
        matches = [p for p in all_projects if p.name == root_name]
        if not matches:
            logging.warning(f"Project '{root_name}' not found on server")
            continue
        for root in matches:
            stack = [root.id]
            while stack:
                pid = stack.pop()
                if pid not in selected_ids:
                    selected_ids.add(pid)
                    stack.extend(children_map.get(pid, []))

    return selected_ids

def get_datasources_for_projects(server, project_ids):
    opts = RequestOptions()
    # If you only have one project_id, you can do:
    # opts.filter.add(Filter(
    #     field    = RequestOptions.Field.ProjectId,
    #     operator = RequestOptions.Operator.Equals,
    #     value    = project_id
    # ))

    # For multiple project IDs, fetch all and filter in Python:
    all_ds, _ = server.datasources.get(req_options=opts)
    return [ds for ds in all_ds if ds.project_id in project_ids]

def get_datasources_in_projects(server, projects):
    """
    Return all DataSourceItem objects whose project_id is in `projects`.
    """
    data_sources = []
    try:
        all_ds, _ = server.datasources.get()                  
        for ds in all_ds:
            if ds.project_id in {p.id for p in projects}:
                data_sources.append(ds)
    except Exception as e:
        logging.error(f"Error retrieving data sources: {e}")
    return data_sources
    
def get_prep_flows_in_projects(server, projects):
    """
    Return all Prep Flow (FlowItem) objects in each project in `projects`.
    """
    prep_flows = []
    try:
        all_flows, _ = server.flows.get()                     
        for flow in all_flows:
            if flow.project_id in {p.id for p in projects}:
                prep_flows.append(flow)
    except Exception as e:
        logging.error(f"Error retrieving prep flows: {e}")
    return prep_flows

def get_flows_for_projects(server, project_ids):
    opts = RequestOptions()
    all_flows, _ = server.flows.get(req_options=opts)
    return [flow for flow in all_flows if flow.project_id in project_ids]

# ─── Job-Failure Retrieval ─────────────────────────────────────────────────────

def get_failed_jobs_for_resource(server, job_type, resource_item):
    """
    Fetches failed jobs of `job_type` for a specific resource (datasource or flow).
    Returns jobs with nonzero finish_code created within TIMEFRAME_HOURS.
    """
    cutoff = datetime.utcnow() - timedelta(hours=TIMEFRAME_HOURS)

    # Build filters: by job type + by resource ID
    opts = TSC.RequestOptions()
    opts.filter.add(TSC.Filter(
        field    = TSC.RequestOptions.Field.Type,
        operator = TSC.RequestOptions.Operator.Equals,
        value    = job_type
    ))

    # Choose the correct field for resource matching
    field = (TSC.RequestOptions.Field.DatasourceId
             if job_type == JOB_TYPE_EXTRACT
             else TSC.RequestOptions.Field.FlowId)
    opts.filter.add(TSC.Filter(
        field    = field,
        operator = TSC.RequestOptions.Operator.Equals,
        value    = resource_item.id
    ))

    # Newest first
    opts.sort.add(TSC.Sort(
        field     = TSC.RequestOptions.Field.CreatedAt,
        direction = TSC.RequestOptions.Direction.Desc
    ))

    all_jobs, _ = server.jobs.get(req_options=opts)
    failures = []

    for job in all_jobs:
        created = job.created_at.replace(tzinfo=None)
        if job.finish_code != 0 and created >= cutoff:
            # Pull error detail
            details = server.jobs.get_job_details(job.id)
            failures.append({
                'resource_type': 'Datasource' if job_type == JOB_TYPE_EXTRACT else 'Prep Flow',
                'name'         : resource_item.name,
                'project'      : getattr(resource_item, 'project_name', 'Unknown'),
                'id'           : job.id,
                'finish_code'  : job.finish_code,
                'created_at'   : created.strftime('%Y-%m-%d %H:%M:%S'),
                'error'        : getattr(details, 'error_detail', 'No error message')
            })
            break  # only the most recent failure per resource

    return failures

def get_failed_data_source_refreshes(server, data_sources, lookback_hours=4):
    """
    For each DataSourceItem in `data_sources`, use the Jobs API to fetch
    extract-refresh jobs in the last `lookback_hours`. Return any failed job.
    """
    cutoff = datetime.utcnow() - timedelta(hours=lookback_hours)
    failures = []

    for ds in data_sources:
        # Build filter: job type + this datasource
        opts = TSC.RequestOptions()
        opts.filter.add(TSC.Filter(
            field    = TSC.RequestOptions.Field.Type,
            operator = TSC.RequestOptions.Operator.Equals,
            value    = "RefreshExtract"
        ))
        opts.filter.add(TSC.Filter(
            field    = TSC.RequestOptions.Field.DatasourceId,
            operator = TSC.RequestOptions.Operator.Equals,
            value    = ds.id
        ))
        opts.sort.add(TSC.Sort(
            field     = TSC.RequestOptions.Field.CreatedAt,
            direction = TSC.RequestOptions.Direction.Desc
        ))

        jobs, _ = server.jobs.get(req_options=opts)
        for job in jobs:
            created = job.created_at.replace(tzinfo=None)
            if job.finish_code != 0 and created >= cutoff:
                details = server.jobs.get_job_details(job.id)
                failures.append({
                    'name'         : ds.name,
                    'project'      : ds.project_name,
                    'job_id'       : job.id,
                    'finish_code'  : job.finish_code,
                    'created_at'   : created.strftime('%Y-%m-%d %H:%M:%S'),
                    'error'        : getattr(details, 'error_detail', 'Unknown error'),
                    'link'         : f"{server.server_info.content_url}/#/site/"
                                    f"{server.auth.site_id}/views/datasources/{ds.id}"
                })
                break  # stop after the most recent failure

    return failures

def get_failed_prep_flow_runs(server, prep_flows):
    """
    For each FlowItem, pull its run history via the FlowRuns endpoint,
    then record the most recent FAILED run.
    """
    failed_runs = []
    try:
        for flow in prep_flows:
            runs, _ = server.flow_runs.get(flow.id)             
            for run in runs:
                if run.status == run.RunStatus.FAILED:
                    failed_runs.append({
                        'name':      flow.name,
                        'project':   flow.project_name,
                        'timestamp': run.end_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'error':     run.error_message,
                        'link':      f"{server.server_info.content_url}/#/site/"
                                     f"{server.auth.site_id}/flows/{flow.id}/run/{run.id}"
                    })
                    break
    except Exception as e:
        logging.error(f"Error retrieving failed prep flow runs: {e}")
    return failed_runs

def send_teams_notification(failures, env_label):
    """
    Sends a summary card and, if failures exist, a details card
    to the Teams webhook.
    """
    webhook_url = cf['TeamsWebhookURL']
    title       = f"{env_label} – Tableau Failures"

    # Build summary
    if not failures:
        summary_text = f"No failures in the last {TIMEFRAME_HOURS} hours."
    else:
        counts = {}
        for f in failures:
            counts[f['resource_type']] = counts.get(f['resource_type'], 0) + 1
        parts = [f"{cnt} {rtype.lower()}(s) failed" for rtype, cnt in counts.items()]
        summary_text = ' | '.join(parts)

    summary_payload = {
        "@type"     : "MessageCard",
        "@context"  : "http://schema.org/extensions",
        "themeColor": "0076D7",
        "title"     : title,
        "text"      : summary_text
    }
    resp = requests.post(webhook_url, json=summary_payload)
    if resp.status_code != 200:
        logging.error(f"{env_label} summary failed: {resp.status_code} {resp.text}")
    else:
        logging.info(f"{env_label} summary sent")

    # If there are failures, send details
    if failures:
        lines = []
        for f in failures:
            lines.append(
                f"• [{f['resource_type']}] **{f['name']}** (Project: {f['project']})  \n"
                f"  Job ID: {f['id']}  |  Code {f['finish_code']}  |  "
                f"Time: {f['created_at']} UTC  |  Error: {f['error']}"
            )

        detail_payload = {
            "@type"     : "MessageCard",
            "@context"  : "http://schema.org/extensions",
            "themeColor": "D70076",
            "title"     : title + " (Details)",
            "text"      : "\n\n".join(lines)
        }
        resp = requests.post(webhook_url, json=detail_payload)
        if resp.status_code != 200:
            logging.error(f"{env_label} details failed: {resp.status_code} {resp.text}")
        else:
            logging.info(f"{env_label} details sent")


# ─── Main Environment Check ────────────────────────────────────────────────────

def check_environment(is_cloud=False):
    prefix = 'Cloud' if is_cloud else 'OnPrem'
    server = None

    try:
        server = sign_in(
            cf[f"{prefix}ServerURL"],
            cf[f"{prefix}TokenName"],
            cf[f"{prefix}TokenSecret"],
            cf.get(f"{prefix}SiteID", "")
        )

        # 1. Scope to ProjectsToCheck (and subprojects)
        project_ids = collect_project_ids(server, cf['ProjectsToCheck'])
        projects    = [p for p in server.projects.get()[0] if p.id in project_ids]

        # 2. Grab datasources and flows
        datasources = get_datasources_in_projects(server, projects)
        prep_flows   = get_prep_flows_in_projects(server, projects)

        # 3. Check each for failures
        failures = []
        failures += get_failed_data_source_refreshes(server, datasources)
        failures += get_failed_prep_flow_runs  (server, prep_flows)

        # 4. Alert
        send_teams_notification(failures, 'Tableau Cloud' if is_cloud else 'Tableau On-Prem')

    except Exception as e:
        logging.error(f"Issue in {prefix}: {e}")

    finally:
        if server:
            sign_out(server)

# ─── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    check_environment(is_cloud=True)
    check_environment(is_cloud=False)
    sys.exit(0)
