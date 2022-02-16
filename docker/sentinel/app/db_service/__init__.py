from typing import Union, List, Optional

import psycopg2.extensions
import psycopg2.extras
from psycopg2 import sql


class DBConnection:
    """
    Класс взаимодействия с БД.
    """

    def __init__(self,
                 dbname: str,
                 user: str,
                 password: str,
                 host: str,
                 port: int):
        self.__dbname = dbname
        self.__user = user
        self.__password = password
        self.__host = host
        self.__port = port
        self.__cursor: Optional[psycopg2.extensions.cursor] = None
        self.__is_transaction_opened = False
        self.__connect()

    def __del__(self):
        self.__connection.close()

    def __connect(self):
        """
        Функция подключения к БД.
        """
        self.__connection = psycopg2.connect(dbname=self.__dbname,
                                             user=self.__user,
                                             password=self.__password,
                                             host=self.__host,
                                             port=self.__port)

    # Transactions =====================================================================================================

    def __get_cursor(self, dict_responses: bool = False) -> psycopg2.extensions.cursor:
        """
        Функция получения курсора для выполнения запросов к БД.

        :param dict_responses: флаг настройки курсора (True - функции fetch_* будут возвращать словарь,
                                                       False - функции fetch_* будут возвращать кортеж)

        :return: курсор
        """
        return self.__connection.cursor(cursor_factory=(psycopg2.extras.RealDictCursor
                                                        if dict_responses else None))

    def __check_cursor(self, dict_responses: bool = False):
        """
        Функция проверки подключения к БД.
        При презагрузке БД бэк не знает об этом, а при попытке обращения к БД, запрос падает с ошибкой.
        Чтобы избежать падения запроса на бэк, класс 1 раз пытается переподключиться к БД, и снова выполняет тестовый
        запрос. Если этот запрос не прошёл - значит БД упала и запрос к бэку упадёт с 500 ошибкой.

        :param dict_responses: флаг настройки курсора (True - функции fetch_* будут возвращать словарь,
                                                       False - функции fetch_* будут возвращать кортеж)
        """

        def ping_db():
            """
            Тестовый запрос к БД.
            """
            self.__cursor.execute('SELECT 1')
            self.rollback_transaction()

        try:
            ping_db()
        except Exception as e:
            print(e)
            self.__connect()
            self.__cursor = self.__get_cursor(dict_responses=dict_responses)
            ping_db()

    def __check_transaction_opened(self):
        """
        Функция проверки открытой транзакции.
        Если транзакция не открыта, попытка выполнения транзакционных функций приведёт к ошибке.
        """
        if not self.__is_transaction_opened:
            raise Exception('Transaction not opened')

    def open_transaction(self, dict_responses: bool = False):
        """
        Функция, открывающая транзакцию.

        :param dict_responses: флаг настройки курсора (True - функции fetch_* будут возвращать словарь,
                                                       False - функции fetch_* будут возвращать кортеж)
        """
        self.__cursor = self.__get_cursor(dict_responses=dict_responses)
        self.__check_cursor(dict_responses=dict_responses)
        self.__is_transaction_opened = True

    def commit_transaction(self):
        """
        Функция делает коммит транзакции, но не закрывает её.
        """
        self.__connection.commit()

    def rollback_transaction(self):
        """
        Функция откатывает транзакцию.
        """
        self.__connection.rollback()

    def close_transaction(self):
        """
        Функция делает коммит транзакции и закрывает её.
        :return:
        """
        self.commit_transaction()
        self.__cursor.close()
        self.__is_transaction_opened = False

    # ==================================================================================================================

    def execute(self,
                query: Union[str, List[str]],
                in_transaction: bool = False):
        """
        Функция выполняет запрос, который не требует возврата (INSERT, UPDATE, DELETE).
        Функция так же обрабатывает группу запросов. В этом случае, все запросы либо выполнятся одной транзакцией, если
        in_transaction == False, либо добавятся в текущую транзакцию, если in_transaction == True.

        :param query: запрос или группа SQL запросов к БД
        :param in_transaction: флаг выполнения запроса
                               (True - в транзакции (функция упадёт, если транзакция не открыта),
                                False - не в транзакции)
        """
        if in_transaction:
            self.__execute_in_transaction(query)
        else:
            try:
                self.__execute(query)
            except psycopg2.InterfaceError:
                self.__connect()
                self.__execute(query)

    def fetch_one(self,
                  query: str,
                  as_dict: bool = False,
                  in_transaction: bool = False) -> Union[psycopg2.extras.RealDictRow, tuple]:
        """
        Функция выполняет запрос и возвращает первое полученное в ответе от БД значение.
        Формат возврата зависит от настроек курсора, если in_transaction == True, иначе зависит от аргумента as_dict.

        :param query: SQL запрос к БД
        :param as_dict: флаг формата возврата (True - вернётся словарь
                                               False - вернётся кортеж)
        :param in_transaction: флаг выполнения запроса
                               (True - в транзакции (функция упадёт, если транзакция не открыта),
                                False - не в транзакции)

        :return: первое полученное из БД значение
        """
        if in_transaction:
            return self.__fetch_one_in_transaction(query)
        else:
            try:
                return self.__fetch_one_no_transaction(query, as_dict)
            except psycopg2.InterfaceError:
                self.__connect()
                return self.__fetch_one_no_transaction(query, as_dict)

    def fetch_all(self,
                  query: str,
                  as_dict: bool = False,
                  in_transaction: bool = False) -> List[Union[psycopg2.extras.RealDictRow, tuple]]:
        """
        Функция выполняет запрос и возвращает все полученные из БД значения.
        Формат возврата зависит от настроек курсора, если in_transaction == True, иначе зависит от аргумента as_dict.

        :param query: SQL запрос к БД
        :param as_dict: флаг формата возврата (True - вернётся словарь
                                               False - вернётся кортеж)
        :param in_transaction: флаг выполнения запроса
                               (True - в транзакции (функция упадёт, если транзакция не открыта),
                                False - не в транзакции)

        :return: все полученные из БД значения
        """
        if in_transaction:
            return self.__fetch_all_in_transaction(query)
        else:
            try:
                return self.__fetch_all_no_transaction(query, as_dict)
            except psycopg2.InterfaceError:
                self.__connect()
                return self.__fetch_all_no_transaction(query, as_dict)

    def __execute(self, query: Union[str, List[str]]):
        """
        Функция выполняет запрос, который не требует возврата (INSERT, UPDATE, DELETE).
        Функция так же обрабатывает группу запросов. Если запросов несколько, они объединяются одну в транзакцию,
        выполняются и транзауия закрывается.

        :param query: запрос или группа SQL запросов к БД
        """
        with self.__connection.cursor() as cursor:
            try:
                if isinstance(query, str):
                    sql_query = sql.SQL(query)
                    cursor.execute(sql_query)
                elif isinstance(query, sql.Composed):
                    cursor.execute(query)
                elif isinstance(query, list):
                    prepared_queries = []
                    for q in query:
                        if isinstance(q, sql.Composed):
                            prepared_queries.append(q.as_string(self.__connection))
                        else:
                            prepared_queries.append(q)
                    sql_query = sql.SQL(f"BEGIN; {'; '.join(prepared_queries)}; COMMIT;")
                    cursor.execute(sql_query)
                else:
                    raise TypeError
            except Exception:
                self.__connection.rollback()
                raise

            self.__connection.commit()

    def __fetch_one(self, query: str, cursor: psycopg2.extensions.cursor) -> Union[psycopg2.extras.RealDictRow, tuple]:
        """
        Функция выполняет запрос и возвращает первое полученное в ответе от БД значение.
        Функция не знает о транзакциях и выполняет запрос через переданный в неё курсор.

        :param query: SQL запрос к БД
        :param cursor: курсор psycopg2

        :return: первое полученное из БД значение
        """
        try:
            if isinstance(query, sql.Composed):
                cursor.execute(query)
            else:
                cursor.execute(sql.SQL(query))
        except Exception:
            self.__connection.rollback()
            raise
        return cursor.fetchone()

    def __fetch_one_no_transaction(self,
                                   query: str,
                                   as_dict: bool = False) -> Union[psycopg2.extras.RealDictRow, tuple]:
        """
        Функция выполняет запрос и возвращает первое полученное в ответе от БД значение.
        Функция открывает транзакцию (создаёт курсор), выполняет запрос и закрывает транзакцию (закрывает курсор).

        :param query: SQL запрос к БД
        :param as_dict: флаг формата возврата (True - вернётся словарь
                                               False - вернётся кортеж)

        :return: первое полученное из БД значение
        """
        with self.__connection.cursor(cursor_factory=(psycopg2.extras.RealDictCursor if as_dict else None)) as cursor:
            return self.__fetch_one(query, cursor)

    def __fetch_all(self,
                    query: str,
                    cursor: psycopg2.extensions.cursor) -> List[Union[psycopg2.extras.RealDictRow, tuple]]:
        """
        Функция выполняет запрос и возвращает все полученные из БД значения.
        Функция не знает о транзакциях и выполняет запрос через переданный в неё курсор.

        :param query: SQL запрос к БД
        :param cursor: курсор psycopg2

        :return: все полученные из БД значения
        """
        try:
            if isinstance(query, sql.Composed):
                cursor.execute(query)
            else:
                cursor.execute(sql.SQL(query))
        except Exception:
            self.__connection.rollback()
            raise
        return cursor.fetchall()

    def __fetch_all_no_transaction(self,
                                   query: str,
                                   as_dict: bool = False) -> List[Union[psycopg2.extras.RealDictRow, tuple]]:
        """
        Функция выполняет запрос и возвращает все полученные из БД значения.
        Функция открывает транзакцию (создаёт курсор), выполняет запрос и закрывает транзакцию (закрывает курсор).

        :param query: SQL запрос к БД
        :param as_dict: флаг формата возврата (True - вернётся словарь
                                               False - вернётся кортеж)

        :return: все полученные из БД значения
        """
        with self.__connection.cursor(cursor_factory=(psycopg2.extras.RealDictCursor if as_dict else None)) as cursor:
            return self.__fetch_all(query, cursor)

    def __execute_in_transaction(self, query: Union[str, List[str]]):
        """
        Функция выполняет запрос, который не требует возврата (INSERT, UPDATE, DELETE).
        Функция так же обрабатывает группу запросов. Если запросов несколько, они объединяются в группу запросов,
        выполняются, но транзакция не закрывается. Если транзакция не была открыта заранее, функция упадёт.

        :param query: запрос или группа SQL запросов к БД
        """
        self.__check_transaction_opened()

        try:
            if isinstance(query, str):
                sql_query = sql.SQL(query)
                self.__cursor.execute(sql_query)
            elif isinstance(query, sql.Composed):
                self.__cursor.execute(query)
            elif isinstance(query, list):
                if len(query) > 0:
                    prepared_queries = []
                    for q in query:
                        if isinstance(q, sql.Composed):
                            prepared_queries.append(q.as_string(self.__connection))
                        else:
                            prepared_queries.append(q)
                    query_str = '; '.join(prepared_queries)
                    sql_query = sql.SQL(query_str)
                    self.__cursor.execute(sql_query)
            else:
                raise TypeError('Unexpected query type')
        except Exception:
            self.rollback_transaction()
            raise

    def __fetch_one_in_transaction(self, query: str) -> Union[psycopg2.extras.RealDictRow, tuple]:
        """
        Функция выполняет запрос и возвращает первое полученное в ответе от БД значение.
        Функция выполняется в транзакции, но не закрывает её. Если транзакция не была открыта заранее, функция упадёт.

        :param query: SQL запрос к БД

        :return: первое полученное из БД значение
        """
        self.__check_transaction_opened()
        return self.__fetch_one(query, self.__cursor)

    def __fetch_all_in_transaction(self, query: str) -> List[Union[psycopg2.extras.RealDictRow, tuple]]:
        """
        Функция выполняет запрос и возвращает все полученные из БД значения.
        Функция выполняется в транзакции, но не закрывает её. Если транзакция не была открыта заранее, функция упадёт.

        :param query: SQL запрос к БД

        :return: все полученные из БД значения
        """
        self.__check_transaction_opened()
        return self.__fetch_all(query, self.__cursor)
