#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


# Этот класс позволяет отправлять задачу в очередь с названием, текст которого стоит в параметре name декоратора @celery.task, @shared_task, ... до двоеточия
# Например, "name=queue_name:task_name" отправит задачу в очередь queue_name
class TaskRouter(object):
    def route_for_task(self, task, *args, **kwargs):
        if ":" not in task:
            return {"queue": "default"}
        namespace, _ = task.split(":")
        print(f'Send task to {namespace} queue')
        return {"queue": namespace}
