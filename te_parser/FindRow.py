import json
import sys

from modules import Utils, Logger, DBModule

logger = Logger.make_file_logger("TE.External")


def find_head_row(client, project, discipline, filename, word_head):
    rtn_val = []
    rs = DBModule.find_data_by_keyword(client, project, discipline, filename, word_head)
    for rec in rs:
        rtn_val.append(rec.get('content'))
    return rtn_val if rtn_val else "Content is not found"


def find_detail_row(client, project, discipline, filename, word_head, word_detail, n_row):
    rtn_val = []
    rs = DBModule.find_data_by_keyword(client, project, discipline, filename, word_head)
    for rec in rs:
        detail_row = DBModule.find_data_by_detail_word(
            client, project, discipline, filename,
            rec.get("page_no"), rec.get("num"), word_detail, n_row)
        for detail_rec in detail_row:
            rtn_val.append(detail_rec.get('content'))
    return rtn_val if rtn_val else "Content is not found"


if __name__ == '__main__':
    logger.debug("in parameters: %s", sys.argv)
    arg_val = " ".join(sys.argv[1:]).split("_")
    logger.debug("parsed parameters: %s", arg_val)

    if len(arg_val) == 5:
        resp = find_head_row(*arg_val)
    elif len(arg_val) == 7:
        resp = find_detail_row(*arg_val)
    else:
        resp = "Need to check the arguments"

    msg = {"content": "".join(resp)} if isinstance(resp, list) else {"error": resp}

    return_data = json.dumps(msg)
    logger.debug("result value: %s", return_data)

    print(return_data)
