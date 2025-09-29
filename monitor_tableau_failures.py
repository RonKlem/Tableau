import json
import logging
import requests
import sys
from zoneinfo import ZoneInfo

from datetime import datetime, timedelta, timezone
from retrying import retry
import tableauserverclient as TSC
from tableauserverclient import RequestOptions, Filter, Sort

# ─── Load Config & Logging ─────────────────────────────────────────────────────

cf = json.load(open('config.json'))

logging.basicConfig(
    filename = cf.get('LogFile', 'tableau_status_check.log'),
    level    = logging.INFO,
    format   = '%(asctime)s - %(levelname)s - %(message)s',
    filemode = 'a'
)

# ─── Constants ─────────────────────────────────────────────────────────────────

TIMEFRAME_HOURS = cf.get('TimeframeHours', 4)

def cutoff_time():
    """
        Calculate the cutoff time for job lookback.

        Returns:
            str: The UTC cutoff time formatted for Tableau REST API queries (YYYY-MM-DDTHH:MM:SSZ).
    """
    try:
        # Get the current time in UTC
        utc_time = datetime.now(timezone.utc)

        # Use a timedelta to calculate the cutoff
        cutoff = utc_time - timedelta(hours=TIMEFRAME_HOURS)
        return cutoff.strftime('%Y-%m-%dT%H:%M:%SZ')

    except Exception as e:
        logging.error(f"Error calculating cutoff time: {e}")
        raise

# ─── Authentication Helpers ────────────────────────────────────────────────────

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def sign_in(server_url, token_name, token_secret, site_id):
    """
    Signs in to Tableau Server or Tableau Cloud using a Personal Access Token.

    Args:
        server_url (str): The Tableau server URL.
        token_name (str): The name of the personal access token.
        token_secret (str): The secret value of the personal access token.
        site_id (str): The Tableau site ID (empty string for default site).

    Returns:
        TSC.Server: Authenticated Tableau server object.
    """

    try:
        server = TSC.Server(server_url)         #, use_server_version=True)
        # Use your PEM path or boolean flag
        server.add_http_options({'verify': cf.get('http_options', True)})
        server.version = '3.21'
        auth = TSC.PersonalAccessTokenAuth(token_name, token_secret, site_id)
        server.auth.sign_in(auth)
        logging.info(f"Signed in to {server_url} (site: '{site_id}')")
        logging.info(f"Server version: {server.version}")
        return server
    
    except Exception as e:

        logging.error(f"Sign-in issue: {e}")
        raise

def sign_out(server):
    """
    Signs out from the Tableau server session.

    Args:
        server (TSC.Server): Authenticated Tableau server object.
    """
    try:
        server.auth.sign_out()
        logging.info("Signed out")
    except Exception as e:
        logging.warning(f"Sign-out issue: {e}")

# ─── Project & Resource Discovery ─────────────────────────────────────────────

def collect_project_ids(server, root_names):
    """
    Collects all project IDs for the given root project names and their descendant subprojects.

    Args:
        server (TSC.Server): Authenticated Tableau server object.
        root_names (list): List of root project names to search for.

    Returns:
        set: Set of project IDs including all descendants.
    """
    try:
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
    
    except Exception as e:

        logging.error(f"Error collecting project IDs: {e}")
        raise


def get_prep_flows_in_projects(server, projects):
    """
    Retrieves all Tableau Prep Flow (FlowItem) objects that belong to the specified projects.

    Args:
        server (TSC.Server): Authenticated Tableau server object.
        projects (list): List of ProjectItem objects to filter flows by.

    Returns:
        list: List of FlowItem objects in the specified projects.
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


# ─── Job-Failure Retrieval ─────────────────────────────────────────────────────



def get_failed_extract_refresh_tasks_for_projects(server, project_ids):
    """
    Retrieve failed ExtractRefresh tasks for datasources belonging to specified Tableau project IDs.
    
    This function:
    - Fetches all datasources and builds a lookup by datasource ID.
    - Retrieves all tasks from Tableau Server.
    - Filters for ExtractRefresh tasks with consecutive failures.
    - Checks if the associated datasource belongs to one of the provided project_ids.
    - Returns a list of dictionaries, each containing metadata for a failed datasource refresh task.
    
    Args:
        server (TSC.Server): Authenticated Tableau Server client instance.
        project_ids (list[str]): List of Tableau project IDs to filter datasource tasks.
    
    Returns:
        list[dict]: List of records for failed ExtractRefresh datasource tasks.
    
    Raises:
        RuntimeError: If fetching datasources or tasks fails.
    """
    failures = []
    try:
        all_ds, _ = server.datasources.get()
        ds_map = {ds.id: ds for ds in all_ds}
    except Exception as e:
        logging.error(f"Failed to fetch datasources: {e}")
        raise RuntimeError(f"Failed to fetch datasources: {e}")

    try:
        all_tasks, _ = server.tasks.get()
    except Exception as e:
        logging.error(f"Failed to fetch tasks: {e}")
        raise RuntimeError(f"Failed to fetch tasks: {e}")

    for task in all_tasks:
        try:
            # Only consider extract refresh tasks - use enum for robustness
            if task.task_type != TSC.TaskItem.TaskType.ExtractRefresh:
                continue
            
            # Skip tasks with zero failures - handle both attribute name variations
            consecutive_fails = (getattr(task, 'consecutive_failed_count', 0) or 
                               getattr(task, 'consecutive_fails_count', 0))
            if not consecutive_fails:
                continue

            # Check if this is a datasource task
            if not hasattr(task, 'data_source_id') or not task.data_source_id:
                continue
                
            ds = ds_map.get(task.data_source_id)
            if not ds or ds.project_id not in project_ids:
                continue

            # Build the URL using more robust server attributes
            try:
                site_id = getattr(server, 'site_id', '') or getattr(server.auth, 'site_id', '')
                base_url = getattr(server, 'server_address', None) or getattr(server.server_info, 'content_url', '')
            except AttributeError:
                site_id = ''
                base_url = ''

            record = {
                'task_id':        task.id,
                'task_type':      task.task_type,
                'fails_in_a_row': consecutive_fails,
                'last_run_at':    getattr(task, 'last_run_at', None),
                'resource_type':  'Datasource',
                'name':           ds.name,
                'project_name':   ds.project_name,
                'resource_id':    ds.id,
                'url':            f"{base_url}/#/site/{site_id}/datasources/{ds.id}" if base_url else ''
            }
            failures.append(record)
        except Exception as e:
            logging.warning(f"Failed to process task {getattr(task, 'id', 'UNKNOWN')}: {e}")
            continue

    return failures


def get_failed_prep_flow_runs(server, prep_flows):
    """
    For each Tableau Prep Flow in the provided list, retrieves its run history using the FlowRuns endpoint.
    Identifies the most recent run for each flow and, if it failed, records detailed information about the failure.
    Includes flow name, project, run ID, finish code, timestamps, error message, Tableau link, and owner name.
    Returns a list of dictionaries, each representing a failed flow run.

    Args:
        server (TSC.Server): Authenticated Tableau server object.
        prep_flows (list): List of FlowItem objects to check for failures.

    Returns:
        list: List of dictionaries containing details about each failed flow run.
    """
    failed_runs = []
    try:
        for flow in prep_flows:
            # Create a RequestOptions object to filter by flow ID and sort by end time in descending order
            # to get the most recent run first.
            req_options = TSC.RequestOptions()
            req_options.filter.add(
                TSC.Filter(
                    TSC.RequestOptions.Field.FlowId,
                    TSC.RequestOptions.Operator.Equals,
                    flow.id
                )
            )
            # Sort by end time in descending order to get the latest run first.
            req_options.sort.add(
                TSC.Sort(
                    TSC.RequestOptions.Field.CompletedAt,
                    TSC.RequestOptions.Direction.Desc
                )
            )
            # Get only the most recent run (page size of 1).
            req_options.pagesize = 1

            # The get() method returns a tuple: (list of FlowRunItems, PaginationItem).
            result = server.flow_runs.get(req_options)

            # Robustly handle tuple, list, or empty result
            if isinstance(result, tuple):
                runs = result[0] if len(result) > 0 else []
            elif result is None:
                runs = []
            else:
                runs = result

            # The 'runs' variable is a list, so check if it is not empty.
            if runs:
                most_recent_run = runs[0]
                status = getattr(most_recent_run, 'status', None)
                failed_statuses = [getattr(most_recent_run, 'RunStatus', None), 'Failed', 'FAILURE', 'failure', 'failed']
                if status and (status == getattr(most_recent_run, 'RunStatus', None) or str(status).lower() in [str(s).lower() for s in failed_statuses]):
                    # Get owner name
                    owner_name = None
                    try:
                        owner = server.users.get_by_id(flow.owner_id)
                        owner_name = getattr(owner, 'name', None) or getattr(owner, 'full_name', None) or getattr(owner, 'email', None)
                    except Exception as e:
                        logging.warning(f"Could not get owner for flow {flow.name}: {e}")
                    failed_runs.append({
                        'resource_type': 'Prep Flow',
                        'name':      flow.name,
                        'id': most_recent_run.id,
                        'finish_code': getattr(most_recent_run, 'finish_code', 'N/A'),
                        'created_at': most_recent_run.end_time.strftime('%Y-%m-%d %H:%M:%S') if hasattr(most_recent_run, 'end_time') and most_recent_run.end_time else "N/A",
                        'project':   flow.project_name,
                        'timestamp': most_recent_run.end_time.strftime('%Y-%m-%d %H:%M:%S') if hasattr(most_recent_run, 'end_time') and most_recent_run.end_time else "N/A",
                        'error':     getattr(most_recent_run, 'error_message', None),
                        'link':      getattr(flow, '_webpage_url', f"{cf.get('OnPremServerURL', server.baseurl)}#/site/{server.site_id}/flows/{flow.id}/"),
                        'owner':     owner_name
                    })
            else:
                logging.debug(f"No run history found for flow '{flow.name}' (ID: {flow.id}).")

    except Exception as e:
        logging.error(f"Error retrieving failed prep flow runs: {e}")
        logging.exception("Exception occurred while processing a flow run.")
        
    return failed_runs

def send_teams_notification(failures, env_label):
    """
     Sends notifications to a Microsoft Teams webhook summarizing Tableau job failures.

    This function sends a summary message card to the specified Teams webhook URL,
    indicating the number and types of failures detected in Tableau jobs within a given timeframe.
    If failures are present, it also sends a detailed message card listing each failed job
    with relevant metadata.

    Args:
        failures (list of dict): A list of dictionaries, each representing a failed Tableau job.
            Each dictionary should contain the following keys:
                - 'resource_type': Type of Tableau resource (e.g., Workbook, Data Source).
                - 'name': Name of the failed resource.
                - 'project': Project name associated with the resource.
                - 'owner': Owner of the resource.
                - 'id': Job ID.
                - 'finish_code': Finish code of the job.
                - 'created_at': UTC timestamp of job creation.
                - 'error': Error message or description.
                - 'link': URL to view the resource in Tableau.
        env_label (str): Label indicating the environment (e.g., "Production", "Staging") for context in notifications.

    Raises:
        Exception: Propagates any exception encountered during the notification process.

    Side Effects:
        - Sends HTTP POST requests to the Teams webhook URL configured in `cf['TeamsWebhookURL']`.
        - Logs success or failure of notification delivery using the `logging` module.

    Example:
        send_teams_notification(failures, "Production")
    """
    try:
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
                    f"• [{f['resource_type']}] **{f['name']}** (Project: {f['project']}) | Owner: {f['owner']}  \n"
                    f"  Job ID: {f['id']}  |  Code {f['finish_code']}  |  "
                    f"Time: {f['created_at']} UTC  |  Error: {f['error']} \n\n"
                    f"  [View in Tableau]({f['link']})"
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

    except Exception as e:

        logging.error(f"Error sending Teams notification: {e}")
        raise

# ─── Main Environment Check ────────────────────────────────────────────────────

def check_environment(is_cloud=False):
    """
    Checks the Tableau environment (Cloud or On-Prem) for failed prep flow runs and sends notifications.

    This function performs the following steps:
        1. Signs in to the Tableau server using credentials from the configuration.
        2. Collects project IDs for the specified projects and their subprojects.
        3. Retrieves datasources and prep flows within those projects.
        4. Checks for failed prep flow runs (data source refresh failures are currently commented out).
        5. Sends a notification with the list of failures to Microsoft Teams.
        6. Signs out from the Tableau server.
    
    Args:
        is_cloud (bool, optional): 
            If True, checks Tableau Cloud environment; otherwise, checks Tableau On-Premises environment.
            Defaults to False.

    Raises:
        Logs any exceptions encountered during the process.
    
    Side Effects:
        - Sends notifications to Microsoft Teams.
        - Prints the list of failures to stdout.
        - Logs errors using the logging module.
        - Signs in and out of Tableau server.
    """
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

        # 2. Grab flows for prep flow checking (datasources are fetched within the tasks function)
        prep_flows = get_prep_flows_in_projects(server, projects)

        # 3. Check each for failures
        failures = []
        failures += get_failed_extract_refresh_tasks_for_projects(server, project_ids)
        #failures += get_failed_prep_flow_runs  (server, prep_flows)
        print(failures)

        # 4. Alert
        #send_teams_notification(failures, 'Tableau Cloud' if is_cloud else 'Tableau On-Prem')

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
