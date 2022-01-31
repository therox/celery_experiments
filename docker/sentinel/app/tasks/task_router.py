#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

class TaskRouter(object):
    def route_for_task(self, task, *args, **kwargs):
        if ":" not in task:
            return {"queue": "default"}
        namespace, _ = task.split(":")
        print(f'Send task to {namespace} queue')
        return {"queue": namespace}
