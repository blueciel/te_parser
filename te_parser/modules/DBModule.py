from modules import RDSModule as rdsMdl


def find_data_by_keyword(client, project, discipline, filename, word, tb_name="itb.mds_data"):
    req_sql = f"""
    SELECT num,
           client,
           project,
           discipline,
           page_total,
           file_name_origin,
           page_no,
           file_url,
           pos_top,
           pos_bottom,
           content
    FROM   {tb_name}
    WHERE  client = %(client)s
    AND    project = %(project)s
    AND    discipline = %(discipline)s
    AND    file_name_origin = %(filename)s
    AND    BINARY content LIKE %(word)s
    """
    req_params = {
        "client": client,
        "project": project,
        "discipline": discipline,
        "filename": filename,
        "word": f"%{word}%"
    }
    record_set = rdsMdl.read_dataset(req_sql, req_params)
    return record_set


def find_data_by_detail_word(client, project, discipline, filename, page_no, idx, word, n_limit,
                             tb_name="itb.mds_data"):
    req_sql = f"""
    SELECT num,
           client,
           project,
           discipline,
           page_total,
           file_name_origin,
           page_no,
           file_url,
           pos_top,
           pos_bottom,
           content
    FROM   (
              SELECT *
              FROM   {tb_name}
              WHERE  client = %(client)s
              AND    project = %(project)s
              AND    discipline = %(discipline)s
              AND    file_name_origin = %(file)s
              AND    page_no = %(page)s
              AND    num >= %(num)s 
              LIMIT  %(limit)s ) m
    WHERE  binary content LIKE %(word)s
    """
    req_params = {
        "client": client,
        "project": project,
        "discipline": discipline,
        "file": filename,
        "page": page_no,
        "num": idx,
        "limit": int(n_limit) + 1,
        "word": f"%{word}%"
    }
    record_set = rdsMdl.read_dataset(req_sql, req_params)
    return record_set


def find_data_by_detail_word_reverse(client, project, discipline, filename, page_no, idx, word, n_limit,
                                     tb_name="itb.mds_data"):
    req_sql = f"""
    SELECT num,
           client,
           project,
           discipline,
           page_total,
           file_name_origin,
           page_no,
           file_url,
           pos_top,
           pos_bottom,
           content
    FROM   (
              SELECT *
              FROM   {tb_name}
              WHERE  client = %(client)s
              AND    project = %(project)s
              AND    discipline = %(discipline)s
              AND    file_name_origin = %(file)s
              AND    page_no = %(page)s
              AND    num <= %(num)s 
              ORDER  BY num DESC
              LIMIT  %(limit)s ) m
    WHERE  binary content LIKE %(word)s
    """
    req_params = {
        "client": client,
        "project": project,
        "discipline": discipline,
        "file": filename,
        "page": page_no,
        "num": idx,
        "limit": int(n_limit) + 1,
        "word": f"%{word}%"
    }
    record_set = rdsMdl.read_dataset(req_sql, req_params)
    return record_set


def get_project_files(client, project, discipline):
    req_sql = f"""
    SELECT r.* 
    FROM (
        SELECT MAX(client) as client, 
            MAX(project) as project, 
            MAX(discipline) as discipline, 
            MAX(file_name_origin) as file_name, 
            file_url
        FROM itb.mds_data 
        WHERE 1 = 1
        AND client = %(client)s
        AND project = %(project)s
        AND discipline = %(discipline)s
        GROUP BY file_url) r
        LEFT JOIN (
            SELECT * 
            FROM itb.tb_te_status 
            WHERE status_cd IN (%(state)s)) s 
        on r.client = s.client 
        and r.project = s.project 
        and r.discipline = s.discipline 
        and r.file_name = s.filename 
    WHERE s.status_cd IS NULL
    """
    req_params = {
        "client": client,
        "project": project,
        "discipline": discipline,
        "state": "00"
    }
    record_set = rdsMdl.read_dataset(req_sql, req_params)
    return record_set


def get_project_rule(client, project, discipline):
    req_sql = f"""
    SELECT client,
        project,
        feeder,
        key_name,
        if_name,
        extract_method,
        a.rule_id,
        a.seq AS rowseq,
        b.seq AS ruleseq,
        find_word,
        find_next_word,
        next_line,
        extract_rule
    FROM `itb-frontend`.tbruleproject_master a,
        `itb-frontend`.tbruleproject_detail b
    WHERE a.rule_id = b.rule_id 
    AND   a.client = %(client)s
    AND   a.project = %(project)s
    ORDER BY a.seq, b.seq
    """
    req_params = {
        "client": client,
        "project": project,
        "discipline": discipline
    }
    record_set = rdsMdl.read_dataset(req_sql, req_params)
    return record_set


def insert_parse_result(params):
    req_sql = """
    INSERT INTO itb.tb_te_parse_result
    (client, project, discipline, feeder, doc_type, te_name, seq, if_name, key_name, te_value, 
    total_page, page_no, file_name, content_num, content_value)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    affected = rdsMdl.execute_data(req_sql, params)
    return affected


def insert_parse_result_set(params):
    req_sql = """
    INSERT INTO itb.tb_te_parse_result
    (client, project, discipline, feeder, doc_type, te_name, seq, if_name, key_name, te_value, 
    total_page, page_no, file_name, content_num, content_value)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    affected = rdsMdl.execute_dataset(req_sql, params)
    return affected


def insert_status_detail(params):
    req_sql = """
    INSERT
    INTO
    itb.tb_te_detail_status
    (client, project, discipline, filename, rule_id, seq, page_no, status_cd, status_desc, content)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # VALUES(%(client)s, %(project)s, %(discipline)s, %(filename)s,
    #     %(rule_id)s, %(seq)s, %(page_no)s, %(status_cd)s, %(status_desc)s, %(content)s)
    affected = rdsMdl.execute_data(req_sql, params)
    return affected


def insert_status_value(params):
    req_sql = """
    INSERT INTO itb.tb_te_status
    (client, project, discipline, filename, rule_cnt, extract_cnt, status_cd, status_desc, file_url)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    affected = rdsMdl.execute_data(req_sql, params)
    return affected


def delete_result(client, project, discipline, file_name):
    req_sql = """
    DELETE FROM itb.tb_te_parse_result 
     WHERE client = %s
       AND project = %s 
       AND discipline = %s 
       AND file_name = %s 
    """
    affected = rdsMdl.execute_data(req_sql, (client, project, discipline, file_name))
    return affected
