import datetime
import os
import time
import requests

from tools.web import get_filename_from_content_disposition


def download_dataset(worker, product_guid: str, product_title: str, cred: str, tmp_dir: str, logger):
    _PRODUCT_URL = "https://scihub.copernicus.eu/dhus/odata/v1/Products('{}')"
    """ Функция выкачивает и сохраняет датасет на S3

    :param product: - датасет
    :param cred:  - авторизационные данные для работы с сервисом scihub.copernucus.eu
    :param tmp_dir: - временный каталог для сохранения и обработки файлов
    :return:
    """
    logger.info(f'Processing {product_title}')
    start = int(round(time.time()))
    # Собираем название файла на локальной ФС

    download_url = '/'.join([_PRODUCT_URL, '$value'])
    session = requests.Session()
    creds = cred.split(':')
    session.auth = (creds[0], creds[1])
    # Проверяем, можно ли скачать датасет
    resp = session.get("https://ya.ru")
    resp.close()
    if not _is_online(session, product_guid, _PRODUCT_URL, logger):
        logger.info(f'[{product_guid}] File is not online yet')
        with session.get(download_url.format(product_guid)) as resp:
            if resp.status_code == 202:
                # Если датасет не в онлайне, то триггерим задачу на скачку
                logger.info(
                    f'[{datetime.datetime.now().time()}] Триггернули задачу {product_guid} на скачивание файла ')
            elif resp.status_code == 200:
                logger.info(
                    f'[{datetime.datetime.now().time()}] Ждали 202, а получили 200. Нипанятна.')
            else:
                logger.info(
                    f'[{datetime.datetime.now().time()}] Получили {resp.status_code}, {resp.text}')
            raise Exception(f'ошибка получения данных по файлу {product_guid}')
    # lp.print(f'Пытаемся выкачать с сайта, вроде должен быть онлайн')
    # logger.error('exception raised, it would be retry after 5 seconds')
    # raise worker.retry(exc='Error!!!!!!!!', countdown=10)
    dataset_filename = None
    # Качаем датасет
    with session.get(download_url.format(product_guid), allow_redirects=True, stream=True) as resp:
        size = 0
        if resp.status_code != 200:
            # lp.print(f'Ошибка: {resp.text} ({resp.status_code})')
            raise Exception(f'ошибка получения файла {product_guid}')
        if "Content-Disposition" in resp.headers.keys():
            # Вытаскиваем название файла
            dataset_filename = get_filename_from_content_disposition(
                resp.headers.get('content-disposition'))
            # Processing dataset title for directories S2A_MSIL2A_20210913T083601_N0301_R064_T37UCS_20210913T113119
            dir_path = ''
            title_parts = product_title.split('_')
            if title_parts[1] == 'MSIL2A':
                dir_path += 'L2/'
                dir_path += os.path.join(title_parts[2][0:4],
                                         title_parts[2][4:6], title_parts[2][6:8])
            dataset_path = os.path.join(tmp_dir, dir_path)
            os.makedirs(dataset_path, exist_ok=True)
            dataset_filename = '/'.join([dataset_path, dataset_filename])
            # lp.print(f'Нашли название файла в хедерах: {dataset_filename}')
        if "content-range" in resp.headers.keys():
            # Вытаскиваем размер файла
            size = float(resp.headers.get('content-range').split('/')[1])
            logger.info(
                f'[{datetime.datetime.now().time()}][{datetime.datetime.now().time()}] Нашли размер файла: {int(size)}')
            # set_dataset_size(product.guid, int(size))

        with open(dataset_filename, 'wb') as f:
            current_size = 0.0

            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                chunk_size = len(chunk)
                if int((current_size + chunk_size) / (10 * 1024 * 1024)) > int(
                        current_size / (10 * 1024 * 1024)):
                    if size == 0:
                        logger.info(
                            f'[{dataset_filename}] Size: {current_size}')
                    else:
                        logger.info(
                            f'[{dataset_filename}] {current_size / size * 100.0:.2f} %')
                current_size += chunk_size

    dl_end_ = int(round(time.time()))
    logger.info(
        f'[{dataset_filename}] Загрузка завершена за: {dl_end_ - start} s')

    # # Если продукт типа S2MSI1C, Конвертим его
    # print('Current product type: ', product.producttype)
    # if product.producttype == 'S2MSI1C':
    #     # Конвертируем и забываем
    #     print('Converting')
    #     dataset_filename = convert_1c_2a(dataset_filename, tmp_dir)
    #     print(1)
    #     product = product._replace(producttype='S2MSI2A')
    #     print(2)

    # print('Upload to S3')
    # Загружаем файл на S3
    # dataset_path = upload_file_to_s3(file_name=dataset_filename, bucket=config.S3_GEOSERVER_BUCKET,
    #                                  upload_folder='ZIP')
    # Регистрируем датасет в БД
    # logger.info('Регистрируем датасет в БД')
    # set_dataset_uploaded(product.guid, dataset_path)
    logger.info('Закрываем сессию')
    session.close()

    # return dataset_path
    return


def _is_online(session: requests.Session, id: str, product_url: str, logger) -> bool:
    """ Функция запрашивает состояние датасета на сервере copernicus.eu

    :param id: идентификатор датасета
    :param cred: авторизационные данные для работы с сервером copernicus.eu
    :return:
    """
    logger.info(f'ID: {id}')

    check_url = '/'.join([product_url, 'Online/$value'])
    logger.info(f'Проверка файла {check_url.format(id)} на онлайн')

    with session.get(check_url.format(id),
                     allow_redirects=True,
                     stream=True) as resp:
        return resp.text == 'true'
