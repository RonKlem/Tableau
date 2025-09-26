import tableauserverclient as TSC

def get_failed_tasks_for_projects(server, project_ids):
    """
    Pulls all tasks via server.tasks.get(), then returns those tasks
    where consecutive_fails_count > 0 and whose datasource or flow
    belongs to one of the given project_ids.
    """
    # 1) Build lookup maps for datasources & flows so we can test project_id
    all_ds, _   = server.datasources.get()   # list[DataSourceItem]
    ds_map      = {ds.id: ds for ds in all_ds}
    all_flows, _ = server.flows.get()        # list[FlowItem]
    flow_map    = {flow.id: flow for flow in all_flows}

    failures = []

    # 2) Fetch every task
    all_tasks, _ = server.tasks.get()
    for task in all_tasks:
        # skip tasks with zero failures
        if not getattr(task, 'consecutive_fails_count', 0):
            continue

        record = {
            'task_id':              task.id,
            'task_type':            task.task_type,  
            'fails_in_a_row':       task.consecutive_fails_count,
            'next_run_at':          getattr(task, 'next_run_at', None),
            'last_run_at':          getattr(task, 'last_run_at', None),
        }

        # 3) Drill into the resource behind the task
        if task.task_type == TSC.TaskItem.TaskType.ExtractRefresh:
            ds = ds_map.get(task.data_source_id)
            if not ds or ds.project_id not in project_ids:
                continue
            record.update({
                'resource_type':  'Datasource',
                'name':           ds.name,
                'project_name':   ds.project_name,
                'resource_id':    ds.id,
                'url':            f"{server.server_info.content_url}/#/site/"
                                  f"{server.auth.site_id}/views/datasources/{ds.id}"
            })

        elif task.task_type == TSC.TaskItem.TaskType.FlowRun:
            flow = flow_map.get(task.flow_id)
            if not flow or flow.project_id not in project_ids:
                continue
            record.update({
                'resource_type':  'Prep Flow',
                'name':           flow.name,
                'project_name':   flow.project_name,
                'resource_id':    flow.id,
                'url':            f"{server.server_info.content_url}/#/site/"
                                  f"{server.auth.site_id}/flows/{flow.id}"
            })
        else:
            # ignore unrelated tasks (subscriptions, etc.)
            continue

        failures.append(record)

    return failures

