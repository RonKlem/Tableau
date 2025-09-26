import tableauserverclient as TSC

def get_failed_extract_refresh_tasks_for_projects(server, project_ids):
    """
    Pulls all tasks via server.tasks.get(), then returns those ExtractRefresh tasks
    where consecutive_fails_count > 0 and whose datasource belongs to one of the given project_ids.
    """
    # Build lookup map for datasources so we can test project_id
    all_ds, _ = server.datasources.get()   # list[DataSourceItem]
    ds_map = {ds.id: ds for ds in all_ds}

    failures = []

    # Fetch every task
    all_tasks, _ = server.tasks.get()
    for task in all_tasks:
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

    return failures
