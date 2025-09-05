# Helper: Get project ID to name mapping
def get_project_id_to_name(server: TSC.Server) -> Dict[str, str]:
    all_projects = server.projects.get()
    if isinstance(all_projects, tuple):
        all_projects = all_projects[0]
    return {p.id: p.name for p in all_projects}

# Helper: Get latest run from a list of runs
def get_latest_run(runs: List[Any]) -> Optional[Any]:
    if not runs:
        return None
    return max(
        runs,
        key=lambda r: getattr(r, '_completed_at', None) or getattr(r, '_created_at', None) or ''
    )

# Helper: Get all project IDs to check, including sub-projects
def get_project_ids_to_check(server: TSC.Server, projects_to_check: Optional[List[str]]) -> Optional[set]:
    try:
        all_projects = server.projects.get()
        if isinstance(all_projects, tuple):
            all_projects = all_projects[0]
        name_to_project = {p.name: p for p in all_projects}
        parent_to_children = {}
        for p in all_projects:
            parent_to_children.setdefault(p.parent_id, []).append(p)
        ids_to_check = set()
        def add_with_children(project):
            ids_to_check.add(project.id)
            for child in parent_to_children.get(project.id, []):
                add_with_children(child)
        if not projects_to_check:
            return None
        for name in projects_to_check:
            project = name_to_project.get(name)
            if project:
                add_with_children(project)
        return ids_to_check
    except Exception as e:
        logging.error(f"Error in get_project_ids_to_check: {e}")
        return None


import tableauserverclient as TSC
import json
import requests
import logging
from retrying import retry
from typing import List, Dict, Any, Optional
# Helper: Get project ID to name mapping
def get_project_id_to_name(server: TSC.Server) -> Dict[str, str]:
    all_projects = server.projects.get()
    if isinstance(all_projects, tuple):
        all_projects = all_projects[0]
    return {p.id: p.name for p in all_projects}

# Helper: Get latest run from a list of runs
def get_latest_run(runs: List[Any]) -> Optional[Any]:
    if not runs:
        return None
    return max(
        runs,
        key=lambda r: getattr(r, '_completed_at', None) or getattr(r, '_created_at', None) or ''
    )

# Load configuration settings from config.json
# This file contains Tableau login info and other settings

with open('config.json') as config_file:
    cf = json.load(config_file)



# Support for single or multiple projects, including sub-projects
projects_to_check = cf.get('ProjectsToCheck')
if not projects_to_check:
    projects_to_check = None  # None means check all


@retry(stop_max_attempt_number=3, wait_fixed=2000)
def sign_in(server_url, token_name, token_secret, site_id):
    """
    Signs into Tableau using a personal access token.
    Retries up to 3 times if login fails.

    Args:
        server_url (str): Tableau server URL
        token_name (str): Personal access token name
        token_secret (str): Personal access token secret
        site_id (str): Tableau site ID

    Returns:
        TSC.Server: Authenticated Tableau server object
    """
    try:
        logging.info(f"Signing into Tableau Server Site: {server_url}")
        server = TSC.Server(server_url, use_server_version=True)
        server.version = "3.10"
        server.add_http_options({'verify': cf['http_options']})
        auth = TSC.PersonalAccessTokenAuth(token_name, token_secret, site_id)
        server.auth.sign_in(auth)
        logging.info(f"Logged into {server_url} Successfully!")
        return server
    
    except TSC.ServerResponseError as e:

        logging.error(f"Failed to sign into {server_url} due to server response error: {e}")

    # Get all projects
    all_projects = server.projects.get()

    # Check if the response is a tuple
    if isinstance(all_projects, tuple):

        # Unpack the tuple
        all_projects = all_projects[0]

    # Build a mapping of project name to project object
    name_to_project = {p.name: p for p in all_projects}

    # Build a mapping of parent_id to list of child projects
    parent_to_children = {}

    # Map parent IDs to their child projects
    for p in all_projects:
        parent_to_children.setdefault(p.parent_id, []).append(p)

    # Find all project objects to check (including sub-projects)
    ids_to_check = set()

    # Recursive function to add project IDs and their children
    def add_with_children(project):
        """
        Adds a project ID and all its child project IDs to the set.
        """

        # Add the project ID
        ids_to_check.add(project.id)

        # Recursively add child project IDs
        for child in parent_to_children.get(project.id, []):

            # Recursively add child project IDs
            add_with_children(child)

    # Find all project objects to check (including sub-projects)
    for name in projects_to_check:

        # Get the project object by name
        project = name_to_project.get(name)

        # Check if the project exists
        if project:

            # Add the project and its children
            add_with_children(project)

    return ids_to_check

def sign_out(server):
    """
    Signs out of Tableau server to end the session.
    Args:
        server (TSC.Server): Authenticated Tableau server object
    """
    try:
        server.auth.sign_out()
        logging.info("Signed out of server.")
    except Exception as e:
        logging.error(f"Failed to sign out of server: {e}")

def get_all_project_ids_to_check(server, projects_to_check):
    """
    Returns a set of project IDs to check, including sub-projects, or None to check all projects.
    """
    try:
        all_projects = server.projects.get()
        if isinstance(all_projects, tuple):
            all_projects = all_projects[0]
        name_to_project = {p.name: p for p in all_projects}
        parent_to_children = {}
        for p in all_projects:
            parent_to_children.setdefault(p.parent_id, []).append(p)
        ids_to_check = set()
        def add_with_children(project):
            ids_to_check.add(project.id)
            for child in parent_to_children.get(project.id, []):
                add_with_children(child)
        if not projects_to_check:
            return None
        for name in projects_to_check:
            project = name_to_project.get(name)
            if project:
                add_with_children(project)
        return ids_to_check
    except Exception as e:
        logging.error(f"Error in get_all_project_ids_to_check: {e}")
        return None

def send_teams_notification(failed_datasources, failed_flows):
    """
    Sends a notification to a Microsoft Teams channel using an Incoming Webhook.
    You must set up a Teams webhook and provide the URL below.

    Args:
        failed_datasources (list): Names of failed datasources
        failed_flows (list): Names of failed prep flows
    """

    # Read webhook URL from config.json
    webhook_url = cf.get('TeamsWebhookURL', 'https://outlook.office.com/webhook/your-webhook-url')

    # Notification title
    title = 'Tableau Failure Alert'

    # Summary count of failures
    summary_lines = []

    # Count failures
    if failed_datasources:
        summary_lines.append(f"{len(failed_datasources)} datasource(s) failed.")
    if failed_flows:
        summary_lines.append(f"{len(failed_flows)} prep flow(s) failed.")
    if not summary_lines:
        summary_lines = ['No failures detected in datasources or prep flows.']
    summary_body = '\n'.join(summary_lines)

    # Send summary notification first
    summary_payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": title,
        "themeColor": "0076D7",
        "title": title,
        "text": summary_body
    }

    try:

        # Send summary notification
        response = requests.post(webhook_url, json=summary_payload)
        if response.status_code == 200:
            logging.info('Teams summary notification sent.')
        else:
            logging.error(f'Failed to send Teams summary notification: {response.status_code} {response.text}')

    except Exception as e:

        logging.error(f'Exception sending Teams summary notification: {e}')

    # Send detailed findings if there are failures
    if failed_datasources or failed_flows:
        details = ''

        # Add datasource details
        for ds_info in failed_datasources:
            # ds_info should be a dict: {'name': ..., 'project': ..., 'timestamp': ..., 'error': ..., 'link': ...}
            details += f"**Datasource:** [{ds_info['name']}]({ds_info.get('link', '#')})\n"
            details += f"Project: {ds_info.get('project', 'Unknown')}\n"
            details += f"Last Run: {ds_info.get('timestamp', 'Unknown')}\n"
            details += f"Error: {ds_info.get('error', 'Unknown')}\n\n"

        # Add prep flow details
        for flow_info in failed_flows:
            # flow_info should be a dict: {'name': ..., 'project': ..., 'timestamp': ..., 'error': ..., 'link': ...}
            details += f"**Prep Flow:** [{flow_info['name']}]({flow_info.get('link', '#')})\n"
            details += f"Project: {flow_info.get('project', 'Unknown')}\n"
            details += f"Last Run: {flow_info.get('timestamp', 'Unknown')}\n"
            details += f"Error: {flow_info.get('error', 'Unknown')}\n\n"
        
        # Add any additional context or information here
        details_payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "summary": title,
            "themeColor": "D70076",
            "title": f"Tableau Failure Details",
            "text": details
        }

        try:
            # Send detailed findings 
            response = requests.post(webhook_url, json=details_payload)

            if response.status_code == 200:
                logging.info('Teams details notification sent.')

            else:
                logging.error(f'Failed to send Teams details notification: {response.status_code} {response.text}')

        except Exception as e:

            logging.error(f'Exception sending Teams details notification: {e}')

def check_datasource_refreshes(server: TSC.Server) -> List[Dict[str, Any]]:
    """
    Checks the refresh status of all Tableau datasources accessible via the provided server object.
    This function retrieves all datasources and their associated refresh tasks, then identifies those
    whose latest refresh attempt has failed. It collects relevant details about each failed datasource,
    including its name, project, timestamp of the failure, error message, and a Tableau link to the datasource.
    Args:
        server (TSC.Server): An authenticated Tableau Server Client (TSC) server object used to interact
            with Tableau REST API endpoints.
    Returns:
        list of dict: A list of dictionaries, each representing a datasource with a failed refresh.
            Each dictionary contains the following keys:
                - 'name' (str): The name of the datasource.
                - 'project' (str): The name of the project containing the datasource.
                - 'timestamp' (str): The timestamp of the latest failed refresh attempt.
                - 'error' (str): The error message or status associated with the failed refresh.
                - 'link' (str or None): A direct Tableau URL to the datasource, if available.
    Raises:
        Logs exceptions internally and returns an empty list if an error occurs during processing.
    Notes:
        - The function relies on external variables and functions such as `projects_to_check`, 
          `get_all_project_ids_to_check`, and `cf` for configuration and project filtering.
        - Only datasources within the specified projects (or all if not specified) are checked.
        - The function assumes the server object is properly authenticated and has the necessary permissions.
    """

    # Initialize a list to collect failed datasources
    failed: List[Dict[str, Any]] = []
    try:
        req_options = TSC.RequestOptions()
        all_datasources = server.datasources.get(req_options)
        if isinstance(all_datasources, tuple):
            all_datasources = all_datasources[0]
        project_ids_to_check = get_project_ids_to_check(server, projects_to_check)
        id_to_project = get_project_id_to_name(server)
        for ds in all_datasources:
            if (project_ids_to_check is None) or (hasattr(ds, 'project_id') and ds.project_id in project_ids_to_check):
                tasks = server.tasks.get()
                if isinstance(tasks, tuple):
                    tasks = tasks[0]
                ds_tasks = [task for task in tasks if hasattr(task, 'datasource_id') and task.datasource_id == ds.id]
                latest_task = get_latest_run(ds_tasks)
                if latest_task and hasattr(latest_task, 'status') and latest_task.status == 'Failed':
                    ds_link = getattr(ds, '_webpage_url', None)
                    failed.append({
                        'name': ds.name,
                        'project': id_to_project.get(ds.project_id, 'Unknown'),
                        'timestamp': getattr(latest_task, 'updated_at', None) or getattr(latest_task, 'created_at', None) or 'Unknown',
                        'error': getattr(latest_task, 'error_message', None) or getattr(latest_task, 'status', 'Unknown'),
                        'link': ds_link
                    })
    except Exception as e:
        logging.error(f'Error checking datasource refreshes: {e}')
    return failed

def check_prep_flows(server: TSC.Server) -> List[Dict[str, Any]]:
    """
    Checks the status of Tableau Prep flows on the specified Tableau server and returns a list of failed flows with details.
    This function queries all flows available on the Tableau server, filters them based on specified project IDs (including sub-projects),
    and inspects their run history to identify flows whose latest run has failed. For each failed flow, it collects relevant information
    such as the flow name, project name, timestamp of the latest run, error message, and a direct Tableau link to the flow.
    Args:
        server (tableauserverclient.Server): An authenticated Tableau Server client instance used to query flows, projects, and flow runs.
    Returns:
        list[dict]: A list of dictionaries, each representing a failed flow with the following keys:
            - 'name' (str): The name of the failed flow.
            - 'project' (str): The name of the project containing the flow.
            - 'timestamp' (str): The timestamp when the latest run was created.
            - 'error' (str): The error message from the latest run, or the run status if no error message is available.
            - 'link' (str or None): A direct Tableau Cloud link to the flow, if available.
    Raises:
        Logs exceptions internally and returns an empty list if an error occurs during the process.
    Note:
        - Requires global variables or configuration such as `projects_to_check`, `get_all_project_ids_to_check`, `cf`, and `logging` to be defined elsewhere.
        - Assumes the server object provides `.flows.get()`, `.projects.get()`, and `.flow_runs.get()` methods.
    """

    failed = []
    try:
        logging.info('Querying all flows on site')
        failed: List[Dict[str, Any]] = []
        try:
            logging.info('Querying all flows on site')
            flows = server.flows.get()
            if isinstance(flows, tuple):
                flows = flows[0]
            project_ids_to_check = get_project_ids_to_check(server, projects_to_check)
            id_to_project = get_project_id_to_name(server)
            for flow in flows:
                if (project_ids_to_check is None) or (hasattr(flow, 'project_id') and flow.project_id in project_ids_to_check):
                    logging.info('Querying all flow runs on site')
                    logging.info(f"Flow attributes: {vars(flow)}")
                    logging.info(f"Using flow.name: {getattr(flow, 'name', None)}, flow.id: {getattr(flow, 'id', None)}")
                    runs = server.flow_runs.get()
                    if isinstance(runs, tuple):
                        runs = runs[0]
                    flow_runs = [run for run in runs if run.flow_id == flow.id]
                    latest_run = get_latest_run(flow_runs)
                    if latest_run and latest_run.status == 'Failed':
                        logging.info(f"Latest run attributes: {vars(latest_run)}")
                        flow_link = getattr(flow, '_webpage_url', None)
                        failed.append({
                            'name': getattr(flow, 'name', None),
                            'project': id_to_project.get(flow.project_id, 'Unknown'),
                            'timestamp': getattr(latest_run, '_completed_at', None) or getattr(latest_run, '_created_at', None) or 'Unknown',
                            'error': getattr(latest_run, 'error_message', None) or getattr(latest_run, 'status', 'Unknown'),
                            'link': flow_link
                        })
        except Exception as e:
            logging.error(f'Error checking prep flows: {e}')
        return failed
    # ...existing code...
# Entry point for the script
def main():
    """
    Main function to check Tableau datasources and prep flows for failures.
    If any failures are found, sends a Microsoft Teams notification to the team.
    Steps:
        1. Sign in to Tableau Cloud using credentials from config.json
        2. Check all datasources for failed refreshes
        3. Check all prep flows for failed runs
        4. Send a Teams notification with the results
        5. Sign out of Tableau Cloud
    """
    try:
        # Step 1: Sign in to Tableau Cloud
        cloud_server = sign_in(cf['CloudServerURL'], cf['CloudTokenName'], cf['CloudTokenSecret'], cf['CloudSiteID'])

        # Step 2: Check all datasources for failed refreshes
        failed_datasources = check_datasource_refreshes(cloud_server)

        # Step 3: Check all prep flows for failed runs
        failed_flows = check_prep_flows(cloud_server)

        # Step 4: Send Teams notification (always, with results)
        send_teams_notification(failed_datasources, failed_flows)

        # Step 5: Sign out
        sign_out(cloud_server)

    except Exception as e:
        logging.error(f"Failed to complete Tableau check: {e}")

if __name__ == "__main__":
    main()

# Entry point for the script
if __name__ == "__main__":
    main()
