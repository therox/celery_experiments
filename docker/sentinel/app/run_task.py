import config


try:
    from tasks.worker import downloader
    from tasks.celery_app import app
    from db_service import DBConnection
except:
    from .tasks.worker import downloader
    from .tasks.celery_app import app
    from .db_service import DBConnection


if __name__ == '__main__':

    db_connection = DBConnection(dbname=config.DB_NAME,
                                 user=config.DB_USER,
                                 password=config.DB_PASSWORD,
                                 host=config.DB_IP,
                                 port=config.DB_PORT)

    # Вытаскиваем данные из БД по нескаченным датасетам
    query = "SELECT guid, title FROM datasets;"
    res = db_connection.fetch_all(query, True)
    print(f'Got {len(res)} datasets to')
    for i in range(0, 2):
        app.send_task(name='downloader:sentinel',
                      args=[res[i]['guid'], res[i]['title']])

    # app.send_task(name='downloader:sentinel',
    #               args=[res['guid'], res['title']])
    # downloader.apply_async(args=[f'Test .aply_async {i+1}'])
    # downloader.delay(f'Test .delay {i+1}')
