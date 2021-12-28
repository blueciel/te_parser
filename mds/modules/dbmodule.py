import boto3
import pymysql


def rds_info(parse_env="PROD"):
    """
    AWS RDS 접속 정보를 확인
    :return: (dict) RDS 접속 정보
    """
    rtn_val = {"host": "database-1.cluster-ckah5qj688fb.ap-northeast-2.rds.amazonaws.com",
               "port": 3306, "user": "itbadmin", "passwd": "gogobsgo2020!", "db": "itb", "charset": "utf8"}
    if parse_env == "PROD":
        sm_client = boto3.client("secretsmanager")
        res_secret = sm_client.get_secret_value(SecretId="itb-rds-database-1-credential")
        secret_str = eval(res_secret.get("SecretString"))

        rtn_val = {}
        rtn_val.setdefault("host", secret_str.get("host"))
        rtn_val.setdefault("port", secret_str.get("port"))
        rtn_val.setdefault("user", secret_str.get("username"))
        rtn_val.setdefault("passwd", secret_str.get("password"))
        rtn_val.setdefault("db", secret_str.get("dbname"))
        rtn_val.setdefault("charset", "utf8")

    return rtn_val


def make_like_str(_text_val):
    res = [""]
    res.extend([x.strip() for x in _text_val.split("&&")])
    res.append("")

    return "%".join(res)


class RDSDatabase:
    def __init__(self, db_info=None):
        self.db_conn = pymysql.connect(**(db_info or rds_info("dev")))
        self.db_curs = self.db_conn.cursor(pymysql.cursors.DictCursor)

    def close(self):
        self.db_curs.close()
        self.db_conn.close()

    def commit(self):
        self.db_conn.commit()

    def commit_close(self):
        self.db_curs.close()
        self.db_conn.commit()
        self.db_conn.close()

    def select_one(self, sql, params=None):
        self.db_curs.execute(sql, params)
        data = self.db_curs.fetchone()
        return data

    def select_all(self, sql, params=None):
        self.db_curs.execute(sql, params)
        data = self.db_curs.fetchall()
        return data

    def execute_one(self, sql, params=None):
        self.db_curs.execute(sql, params)
        cnt = self.db_curs.rowcount
        self.db_conn.commit()
        return cnt

    def execute_many(self, sql, params=None):
        self.db_curs.executemany(sql, params)
        cnt = self.db_curs.rowcount
        self.db_conn.commit()
        return cnt


def select_rules(client, feeder):
    """
    데이터베이스에서 parsing 룰 조회
    :param client: MDS client
    :param feeder: MDS feeder
    :return: parsing 룰 (pandas dataframe)
    """
    # 데이터베이스에서 parsing 룰 읽어 오기
    req_sql = "SELECT * FROM itb.mds_parse_rule WHERE client = %(client)s AND feeder = %(feeder)s"
    req_params = {"client": client, "feeder": feeder}
    rds = RDSDatabase()
    res_data = rds.select_all(req_sql, req_params)
    rds.close()
    return res_data


def select_mds_data(client, project, discipline, file_name, find_words):
    """
    데이터베이스에서 MDS 데이터 조회
    :param client:  MDS client
    :param project: MDS project
    :param discipline: MDS discipline
    :param file_name: MDS file 명(확장자 없음)
    :param find_words: 조회 단어
    :return: MDS Line String (pandas dataframe)
    """
    req_sql = """
    SELECT num, client, project, discipline, page_total, file_name_origin, page_no, file_url, 
           pos_top, pos_bottom, content 
      FROM itb.mds_data 
     WHERE client = %(client)s
       AND project = %(project)s 
       AND discipline = %(discipline)s 
       AND file_name_origin = %(file)s 
       AND BINARY content like %(word)s
    """
    req_params = {"client": client, "project": project, "discipline": discipline, "file": file_name}
    req_params.setdefault("word", make_like_str(find_words))
    # print(req_params)
    rds = RDSDatabase()
    res_data = rds.select_all(req_sql, req_params)
    rds.close()
    return res_data


def select_next_data(client, project, discipline, file_name, find_words, page, num, limit):
    """
    데이터베이스에서 MDS 데이터 조회
    :param client:  MDS client
    :param project: MDS project
    :param discipline: MDS discipline
    :param file_name: MDS file 명(확장자 없음)
    :param find_words: 조회 단어
    :param page: 조회 page 번호
    :param num: 줄 순번
    :param limit: 최대 조회 수
    :return: MDS Line String (pandas dataframe)
    """
    req_sql = """
    SELECT num, client, project, discipline, page_total, file_name_origin, page_no, file_url, 
           pos_top, pos_bottom, content 
      FROM (
        SELECT * 
          FROM itb.mds_data 
         WHERE client = %(client)s
           AND project = %(project)s 
           AND discipline = %(discipline)s 
           AND file_name_origin = %(file)s 
           AND page_no = %(page)s
           AND num >= %(num)s 
         LIMIT %(limit)s
      ) m
     WHERE BINARY content like %(word)s 
    """
    req_params = {"client": client, "project": project, "discipline": discipline, "file": file_name}
    req_params.setdefault("page", page)
    req_params.setdefault("num", num)
    req_params.setdefault("limit", int(limit + 1))
    req_params.setdefault("word", make_like_str(find_words))
    # print(req_params)
    rds = RDSDatabase()
    res_data = rds.select_all(req_sql, req_params)
    rds.close()
    return res_data


def save_result(data):
    req_query = """
    INSERT INTO itb.mds_parse_result_dev
    (client, project, discipline, doc_type, te_name, seq, category, key_name, te_value, total_page, page_no, 
    file_name, content_num, content_value)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    rds = RDSDatabase()
    cnt = rds.execute_many(req_query, data)
    rds.close()
    return cnt


def delete_result(_client, _project, _discipline, _file_name):
    req_query = """
    DELETE FROM itb.mds_parse_result_dev 
     WHERE client = %s
       AND project = %s 
       AND discipline = %s 
       AND file_name = %s 
    """
    rds = RDSDatabase()
    cnt = rds.execute_one(req_query, [_client, _project, _discipline, _file_name])
    rds.close()
    return cnt
