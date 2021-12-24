import pymysql

from modules import AWSModule as aws


class RDSDatabase:
    def __init__(self, db_info=None):
        self.db_info = db_info or aws.rds_info()
        self.db_conn = None
        self.db_curs = None
        self.db_act = "R"

    def __enter__(self):
        # print("__enter__ call")
        self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        # print("__exit__ call")
        if self.db_act == "R":
            self.close()
        else:
            self.commit()
            self.release()

    def connect(self):
        self.db_conn = pymysql.connect(**self.db_info)
        if self.db_act == "R":
            self.db_curs = self.db_conn.cursor(pymysql.cursors.DictCursor)

    def commit(self):
        # print("commit call")
        self.db_conn.commit()

    def close(self):
        # print("close call")
        self.db_curs.close()
        self.db_conn.close()

    def release(self):
        # print("release call")
        self.db_conn.close()

    def select_one(self, sql, params=None):
        self.db_act = "R"
        self.db_curs.execute(sql, params)
        data = self.db_curs.fetchone()
        return data

    def select_all(self, sql, params=None):
        self.db_act = "R"
        self.db_curs.execute(sql, params)
        data = self.db_curs.fetchall()
        return data

    def execute_one(self, sql, params=None):
        self.db_act = "CUD"
        self.db_curs.execute(sql, params)
        cnt = self.db_curs.rowcount
        self.db_conn.commit()
        return cnt

    def execute_many(self, sql, params=None):
        self.db_act = "CUD"
        self.db_curs.executemany(sql, params)
        cnt = self.db_curs.rowcount
        self.db_conn.commit()
        return cnt


def read_data(req_query, req_params):
    """
    데이터베이스 한 row 조회
    :param req_query: (str) 조회 SQL Query
    :param req_params: (dictionary) 조회 조건
    :return: (dictionary) 조회 결과
    """
    db_request = RDSDatabase()
    with db_request:
        record_set = db_request.select_one(req_query, req_params)
        return record_set


def read_dataset(req_query, req_params):
    """
    데이터베이스 조회
    :param req_query: (str) 조회 SQL Query
    :param req_params: (dictionary) 조회 조건
    :return: (list(dictionary)) 조회 결과
    """
    db_request = RDSDatabase()
    with db_request:
        record_set = db_request.select_all(req_query, req_params)
        return record_set


def execute_data(req_query, req_params):
    """
    데이터베이스 단일 CUD 실행
    :param req_query: (str) 조회 SQL Query
    :param req_params: (dictionary) 조회 조건
    :return: (list(dictionary)) 조회 결과
    """
    db_request = RDSDatabase()
    with db_request:
        record_set = db_request.execute_one(req_query, req_params)
        return record_set


def execute_dataset(req_query, req_params):
    """
    데이터베이스 단일 CUD 실행
    :param req_query: (str) 조회 SQL Query
    :param req_params: (dictionary) 조회 조건
    :return: (list(dictionary)) 조회 결과
    """
    db_request = RDSDatabase()
    with db_request:
        record_set = db_request.execute_many(req_query, req_params)
        return record_set
