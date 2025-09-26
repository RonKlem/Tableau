import tableauserverclient as TSC
import logging

def get_failed_extract_refresh_tasks_for_projects(server, project_ids):
    """
    Retrieve failed ExtractRefresh tasks for datasources belonging to specified Tableau project IDs.

    This function:
    - Fetches all datasources and builds a lookup by datasource ID.
    - Retrieves all tasks from Tableau Server.
    - Filters for ExtractRefresh tasks with consecutive failures (consecutive_fails_count > 0).
    - Checks if the associated datasource belongs to one of the provided project_ids.
    - Returns a list of dictionaries, each containing metadata for a failed datasource refresh task.

    Args:
        server (TSC.Server): Authenticated Tableau Server client instance.
        project_ids (list[str]): List of Tableau project IDs to filter datasource tasks.

    Returns:
        list[dict]: List of records for failed ExtractRefresh datasource tasks. Each record contains:
            - task_id (str): Unique ID of the task.
            - task_type (str): Type of the task (should be ExtractRefresh).
            - fails_in_a_row (int): Number of consecutive failures.
            - last_run_at (datetime or None): Time of last run, if available.
            - resource_type (str): 'Datasource'.
            - name (str): Name of the datasource.
            - project_name (str): Name of the project containing the datasource.
            - resource_id (str): ID of the datasource.
            - url (str): Direct Tableau URL to the datasource.

    Raises:
        RuntimeError: If fetching datasources or tasks fails.
    """
    failures = []
    try:
        all_ds, _ = server.datasources.get()   # list[DataSourceItem]
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
            # Only consider extract refresh tasks
            if task.task_type != TSC.TaskItem.TaskType.ExtractRefresh:
                continue
            # Skip tasks with zero failures
            if not getattr(task, 'consecutive_fails_count', 0):
                continue

            ds = ds_map.get(task.data_source_id)
            if not ds or ds.project_id not in project_ids:
                continue

            record = {
                'task_id':        task.id,
                'task_type':      task.task_type,
                'fails_in_a_row': task.consecutive_fails_count,
                'last_run_at':    getattr(task, 'last_run_at', None),
                'resource_type':  'Datasource',
                'name':           ds.name,
                'project_name':   ds.project_name,
                'resource_id':    ds.id,
                'url':            f"{server.server_info.content_url}/#/site/"
                                  f"{server.auth.site_id}/views/datasources/{ds.id}"
            }
            failures.append(record)
        except Exception as e:
            logging.warning(f"Failed to process task {getattr(task, 'id', 'UNKNOWN')}: {e}")
            continue

    return failures
