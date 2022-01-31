#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

# from __future__ import absolute_import, print_function, unicode_literals

import config
from celery import Celery
# from celery.schedules import crontab

app = Celery(__name__)
app.conf.update(
    {
        "broker_url": config.CELERY_BROKER_URL,
        "result_backend": config.CELERY_BACKEND_URL,
        "imports": (
            "tasks.worker",
            "tasks.task_router"
        ),
        "task_routes": ("tasks.task_router.TaskRouter",)
        # "imports": (
        #     "tasks", "task_router",
        # ),
        #    "worker_max_tasks_per_child":1,
        # "task_routes": ("task_router.TaskRouter",),
        # "task_serializer": "json",
        # "result_serializer": "json",
        # "accept_content": ["json"]
    }
)

# app.conf.beat_schedule = {
#     'search-meta': {
#         'task': 'scheduler:viirs',
#         'schedule': crontab(hour=0, minute=1,),
#         'args': (["auto"]),
#     }
# }
