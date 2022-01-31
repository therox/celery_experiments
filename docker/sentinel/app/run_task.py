from time import sleep

try:
    # from tasks.worker import downloader
    from tasks.celery_app import app
except:
    # from .tasks.worker import downloader
    from .tasks.celery_app import app


if __name__ == '__main__':
    sleep(3)
    for i in range(1):
        app.send_task(name='downloader:sentinel',
                      args=['dl_test'], queue='sentinel')
        # downloader.apply_async(args=['test'], queue='sentinel')
