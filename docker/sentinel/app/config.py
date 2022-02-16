from datetime import timedelta
import os


try:
    CELERY_BROKER_URL = os.environ["CELERY_BROKER_URL"]
except:
    raise RuntimeError("CELERY_BROKER_URL not defined")
try:
    CELERY_BACKEND_URL = os.environ["CELERY_BACKEND_URL"]
except:
    CELERY_BACKEND_URL = None

# DB ===================================================================================================================
DB_NAME = os.environ.get('DB_NAME_SATELLITE')
DB_IP = os.environ.get('DB_IP')
DB_PORT = os.environ.get('DB_PORT')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')

DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_IP}:{DB_PORT}/{DB_NAME}'

MAX_QUERY_RETRY_COUNT = 2

DB_CACHE_TTL = timedelta(seconds=60)

DB_DEFAULT_MAX_STR_LEN = 5000
DB_MAX_NAME_STR_LEN = 255
