from time import sleep

try:
    from tasks.worker import downloader
    from tasks.celery_app import app
except:
    from .tasks.worker import downloader
    from .tasks.celery_app import app


if __name__ == '__main__':
    sleep(3)
    for i in range(10):
        app.send_task(name='downloader:sentinel',
                      args=[f'Test send_task {i+1}'])
        downloader.apply_async(args=[f'Test .aply_async {i+1}'])
        downloader.delay(f'Test .delay {i+1}')
