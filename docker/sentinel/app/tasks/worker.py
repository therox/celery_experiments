from time import sleep
from celery import shared_task
try:
    from tasks.celery_app import app
except:
    from .celery_app import app

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

@shared_task(bind=True, name="downloader:sentinel", acks_late=True, reject_on_worker_lost=True,
             autoretry_for=(RuntimeError,), retry_kwargs={"countdown": 2, "max_retries": 3})
def downloader(self, text: str):
    sleep(2)
    logger.info(
        f'now I\'m downloading file {text} and i am {self.request.id} - {self.name}')
    return text


@app.task
def searcher(field_uuid: str, date: str):
    print(f'Now I\'m searching data for field {field_uuid} and date {date}')
