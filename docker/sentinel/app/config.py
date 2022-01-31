import os


try:
    CELERY_BROKER_URL = os.environ["CELERY_BROKER_URL"]
except:
    raise RuntimeError("CELERY_BROKER_URL not defined")
try:
    CELERY_BACKEND_URL = os.environ["CELERY_BACKEND_URL"]
except:
    CELERY_BACKEND_URL = None
