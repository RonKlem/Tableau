import json
import logging
import requests
import sys

from datetime import datetime, timedelta
from retrying import retry
import tableauserverclient as TSC

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
    """
    Return all DataSourceItem objects whose project_id is in project_ids.
    """
    all_dss, _ = server.data_sources.get()
    return [ds for ds in all_dss if ds.project_id in project_ids]

def get_flows_for_projects(server, project_ids):
    """
    Return all PrepFlowItem objects whose project_id is in project_ids.
    """
    all_flows, _ = server.prep_conductors.get_flows()
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
    """
    Signs in, finds all datasources & flows under ProjectsToCheck (and subprojects),
    collects failures, sends Teams alerts, and signs out.
    """
    prefix   = 'Cloud' if is_cloud else 'OnPrem'
    server_url = cf[f"{prefix}ServerURL"]
    token_name = cf[f"{prefix}TokenName"]
    token_sec  = cf[f"{prefix}TokenSecret"]
    site_id    = cf.get(f"{prefix}SiteID", "")
    env_label  = "Tableau Cloud" if is_cloud else "Tableau On-Prem"
    server     = None

    try:
        server = sign_in(server_url, token_name, token_sec, site_id)
        logging.info(f"Connected to {env_label}")

        # 1. Collect project IDs (roots + subprojects)
        project_ids = collect_project_ids(server, cf['ProjectsToCheck'])

        # 2. Fetch resources in those projects
        datasources = get_datasources_for_projects(server, project_ids)
        flows       = get_flows_for_projects(server, project_ids)

        # 3. Gather failures per resource
        failures = []
        for ds in datasources:
            failures += get_failed_jobs_for_resource(server, JOB_TYPE_EXTRACT, ds)
        for flow in flows:
            failures += get_failed_jobs_for_resource(server, JOB_TYPE_FLOW, flow)

        # 4. Send Teams notifications
        send_teams_notification(failures, env_label)

    except Exception as e:
        logging.error(f"Issue in {env_label}: {e}")

    finally:
        if server:
            sign_out(server)


# ─── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    check_environment(is_cloud=True)
    check_environment(is_cloud=False)
    sys.exit(0)
