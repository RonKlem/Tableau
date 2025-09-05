import tableauserverclient as TSC
import json
import requests
import logging
from datetime import datetime, timedelta
from typing import List, Dict
from retrying import retry

def load_config(config_path):
    with open(config_path) as config_file:
        return json.load(config_file)

cf = load_config('config.json')

def setup_logging(log_file='tableau_status_check.log'):
    logging.basicConfig(
        filename=log_file,
        filemode='a',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

setup_logging()

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def sign_in(server_url: str, token_name: str, token_secret: str, site_id: str) -> TSC.Server:
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
        server.add_http_options({'verify': cf.get('http_options', True)})
        auth = TSC.PersonalAccessTokenAuth(token_name, token_secret, site_id)
        server.auth.sign_in(auth)
        logging.info(f"Logged into {server_url} Successfully!")
        return server
    except TSC.ServerResponseError as e:
        logging.error(f"Failed to sign into {server_url} due to server response error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during sign-in to {server_url}: {e}")
        raise

def sign_out(server: TSC.Server) -> None:
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

def get_projects_by_name(server, project_names):
    """
    Retrieves projects by name.

    Args:
        server (TSC.Server): Tableau server object
        project_names (list): List of project names to retrieve

    Returns:
        list: List of project objects

    Reference: https://tableau.github.io/server-client-python/docs/api-ref#projects
    """
    projects = []
    try:
        all_projects = server.projects.get()
        # Filter only ProjectItem objects
        filtered_projects = [p for p in all_projects if isinstance(p, TSC.ProjectItem)]
        logging.info(f"Retrieved {len(filtered_projects)} projects from the server.")
        for project in filtered_projects:
            if project.name in project_names:
                projects.append(project)
        # Log any unexpected types
        unexpected = [p for p in all_projects if not isinstance(p, TSC.ProjectItem)]
        for item in unexpected:
            logging.warning(f"Unexpected project type: {type(item).__name__} - {item}")
    except Exception as e:
        logging.error(f"Error retrieving projects: {e}")
    return projects

def get_data_sources_in_projects(server, projects):
    data_sources = []
    try:
        for project in projects:
            for data_source in server.data_sources.get(TSC.PageRequest(filter=TSC.Filter.and_(TSC.Filter.project_id_equal(project.id)))):
                data_sources.append(data_source)
    except Exception as e:
        logging.error(f"Error retrieving data sources: {e}")
    return data_sources

def get_prep_flows_in_projects(server, projects):
    prep_flows = []
    try:
        for project in projects:
            for prep_flow in server.prep_conductors.get_flows(TSC.PageRequest(filter=TSC.Filter.and_(TSC.Filter.project_id_equal(project.id)))):
                prep_flows.append(prep_flow)
    except Exception as e:
        logging.error(f"Error retrieving prep flows: {e}")
    return prep_flows

def get_failed_data_source_refreshes(server, data_sources):
    failed_refreshes = []
    try:
        for data_source in data_sources:
            refresh_history, pagination_item = server.data_sources.get_refresh_history(data_source.id)
            for refresh in refresh_history:
                if refresh.status == TSC.RefreshHistoryItem.RefreshStatus.FAILED:
                    failed_refreshes.append({
                        'name': data_source.name,
                        'project': data_source.project_name,
                        'timestamp': refresh.end_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'error': refresh.error_message,
                        'link': f"{server.server_info.content_url}/#/site/{server.auth.site_id}/views/datasources/{data_source.id}"
                    })
                    break  # Only get the most recent failure
    except Exception as e:
        logging.error(f"Error retrieving failed data source refreshes: {e}")
    return failed_refreshes

def get_failed_prep_flow_runs(server, prep_flows):
    failed_runs = []
    try:
        for prep_flow in prep_flows:
            run_history, pagination_item = server.prep_conductors.get_flow_runs(prep_flow.id)
            for run in run_history:
                if run.status == TSC.PrepFlowRunItem.RunStatus.FAILED:
                    failed_runs.append({
                        'name': prep_flow.name,
                        'project': prep_flow.project_name,
                        'timestamp': run.end_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'error': run.error_message,
                        'link': f"{server.server_info.content_url}/#/site/{server.auth.site_id}/flows/{prep_flow.id}/run/{run.id}"
                    })
                    break  # Only get the most recent failure
    except Exception as e:
        logging.error(f"Error retrieving failed prep flow runs: {e}")
    return failed_runs

def send_teams_notification(failed_datasources: List[Dict[str, str]], failed_flows: List[Dict[str, str]]) -> None:
    webhook_url = cf.get('TeamsWebhookURL', 'https://outlook.office.com/webhook/your-webhook-url')
    title = 'Tableau Failure Alert'
    summary_lines = []

    if failed_datasources:
        summary_lines.append(f"{len(failed_datasources)} datasource(s) failed.")
    if failed_flows:
        summary_lines.append(f"{len(failed_flows)} prep flow(s) failed.")
    if not summary_lines:
        summary_lines = ['No failures detected in datasources or prep flows.']
    summary_body = '\n'.join(summary_lines)

    summary_payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": title,
        "themeColor": "0076D7",
        "title": title,
        "text": summary_body
    }

    try:
        response = requests.post(webhook_url, json=summary_payload)
        if response.status_code == 200:
            logging.info('Teams summary notification sent.')
        else:
            logging.error(f'Failed to send Teams summary notification: {response.status_code} {response.text}')
    except Exception as e:
        logging.error(f'Exception sending Teams summary notification: {e}')

    if failed_datasources or failed_flows:
        details = ''

        for ds_info in failed_datasources:
            details += f"**Datasource:** [{ds_info['name']}]({ds_info.get('link', '#')})\n"
            details += f"Project: {ds_info.get('project', 'Unknown')}\n"
            details += f"Last Run: {ds_info.get('timestamp', 'Unknown')}\n"
            details += f"Error: {ds_info.get('error', 'Unknown')}\n\n"

        for flow_info in failed_flows:
            details += f"**Prep Flow:** [{flow_info['name']}]({flow_info.get('link', '#')})\n"
            details += f"Project: {flow_info.get('project', 'Unknown')}\n"
            details += f"Last Run: {flow_info.get('timestamp', 'Unknown')}\n"
            details += f"Error: {flow_info.get('error', 'Unknown')}\n\n"

        details_payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "summary": title,
            "themeColor": "D70076",
            "title": f"Tableau Failure Details",
            "text": details
        }

        try:
            response = requests.post(webhook_url, json=details_payload)
            if response.status_code == 200:
                logging.info('Teams details notification sent.')
            else:
                logging.error(f'Failed to send Teams details notification: {response.status_code} {response.text}')
        except Exception as e:
            logging.error(f'Exception sending Teams details notification: {e}')

def check_for_failures(config, is_cloud):

    server_url = config['CloudServerURL'] if is_cloud else config['OnPremServerURL']
    token_name = config['CloudTokenName'] if is_cloud else config['TokenName']
    token_secret = config['CloudTokenSecret'] if is_cloud else config['TokenSecret']
    site_id = config['CloudSiteID'] if is_cloud else config['OnPremSiteID']

    try:
        server = sign_in(server_url, token_name, token_secret, site_id)
        logging.info(f"Connected to {'Cloud' if is_cloud else 'On-Prem'} Tableau Server")

        projects_to_check = config['ProjectsToCheck']
        projects = get_projects_by_name(server, projects_to_check)
        
        if not projects:
            logging.info("No projects found to check.")
            return

        data_sources = get_data_sources_in_projects(server, projects)
        prep_flows = get_prep_flows_in_projects(server, projects)

        failed_data_source_refreshes = get_failed_data_source_refreshes(server, data_sources)
        failed_prep_flow_runs = get_failed_prep_flow_runs(server, prep_flows)

        send_teams_notification(failed_data_source_refreshes, failed_prep_flow_runs)

    except Exception as e:
        logging.error(f"Exception during failure check: {e}")
    finally:
        sign_out(server)
        logging.info(f"Signed out from {'Cloud' if is_cloud else 'On-Prem'} Tableau Server")

def main() -> None:
    """
    Main function to check Tableau datasources and prep flows for failures.
    If any failures are found, sends out a teams notification.
    """
    try:

        # Check for failures on Cloud and On-Prem servers
        check_for_failures(cf, is_cloud=True)
        check_for_failures(cf, is_cloud=False)

    except Exception as e:
        logging.error(f"Failed to complete Tableau check: {e}")


# Entry point for the script
if __name__ == "__main__":
    main()
