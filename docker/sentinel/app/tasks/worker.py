from time import sleep
from celery import shared_task
from db_service import DBConnection
from sentinel.downloader import download_dataset

try:
    from tasks.celery_app import app, config
except:
    from .celery_app import app, config

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

db_connection = DBConnection(dbname=config.DB_NAME,
                             user=config.DB_USER,
                             password=config.DB_PASSWORD,
                             host=config.DB_IP,
                             port=config.DB_PORT)


@shared_task(bind=True, name="downloader:sentinel", acks_late=True, reject_on_worker_lost=True,
             autoretry_for=(RuntimeError,), retry_kwargs={"countdown": 2, "max_retries": 3})
def downloader(self, dataset_guid: str, dataset_title: str):
    # Скачиваем датасет
    logger.info(
        f'now I\'m downloading dataset {dataset_title} with GUID {dataset_guid} and i am {self.request.id} - {self.name}')
    download_dataset(self,
                     dataset_guid, dataset_title, config.COPERNICUS_CREDENTIALS, "/data", logger)
    # logger.info('Connect to db?')
    # try:
    #     res = db_connection.fetch_one(
    #         "SELECT  count(*) as a FROM datasets;", True)
    #     logger.info(f"Ok: {res['a']}")
    # except Exception as e:
    #     logger.info(f'Error: {e}')

    return


@app.task
def searcher(field_uuid: str, date: str):
    print(f'Now I\'m searching data for field {field_uuid} and date {date}')
