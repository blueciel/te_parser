import datetime
import json
import sys

from modules import Utils, Logger, DBModule, ParsingModule, AWSModule

logger = Logger.make_logger("TE.Main")


def extract_regex(project, rule, te_comment):
    logger.debug(f"<-{rule.get('if_name')},{rule.get('key_name')}")
    logger.debug(
        f"{rule.get('find_word')}-{rule.get('find_next_word')}-{rule.get('next_line')}-{rule.get('extract_rule')}")

    state_code, state_msg, num_page = "00", "", None

    if rule.get('find_next_word') and rule.get('find_next_word') != "null":
        content_list = ParsingModule.find_detail_row(
            project.get('client'), project.get('project'), project.get('discipline'), project.get('file_name'),
            rule.get('find_word'), rule.get('find_next_word'), rule.get('next_line'))
    else:
        content_list = ParsingModule.find_head_row(
            project.get('client'), project.get('project'), project.get('discipline'),
            project.get('file_name'), rule.get('find_word'))

    ext_values = []
    if len(content_list):
        for content in content_list:
            ext_values = Utils.extract_text(content.get('content'), rule.get('extract_rule'))
            # logger.debug(f"extract->num: {content.get('num')}, " +
            #              f"page: {content.get('page_no')}/{content.get('page_total')}, {content.get('content')}")
            logger.debug(f"extract->te: {rule.get('if_name')}, value: {ext_values}")
            # client, project, discipline, feeder, doc_type, te_name, seq, if_name, key_name, te_value,
            # total_page, page_no, file_name, content_num, content_value
            if ext_values:
                for ext_val in ext_values:
                    res_in_params = ParsingModule.set_result_value(project, rule, te_comment, content, ext_val)
                    res_in_count = DBModule.insert_parse_result(list(res_in_params.values()))
                    logger.debug(f"extract text saved:{res_in_count}")
                if len(ext_values) > 1:
                    state_code, state_msg, num_page = "01", "Text Extraction is a lot from one line", content.get(
                        'page_no')
                else:
                    state_code, state_msg, num_page = "00", "OK", content.get('page_no')
            else:
                state_code, state_msg, num_page = "12", "Text Extraction is None", content.get('page_no')
    else:
        state_code, state_msg = "11", "Content Not Found"

    in_params = ParsingModule.set_status_detail(project, rule, state_code, state_msg, num_page)
    status_d_in_cnt = DBModule.insert_status_detail(list(in_params.values()))
    logger.debug(f"extraction result db logging:{status_d_in_cnt}")

    return state_code, state_msg, ext_values


def extract_crop(project, rule, te_comment):
    logger.debug(f"<-{rule.get('if_name')},{rule.get('key_name')}")
    logger.debug(
        f"{rule.get('find_word')}-{rule.get('find_next_word')}-{rule.get('next_line')}-{rule.get('extract_rule')}")

    state_code, state_msg, num_page = "00", "", None

    crop_info = ParsingModule.find_head_detail_row(
        project.get('client'), project.get('project'), project.get('discipline'), project.get('file_name'),
        rule.get('find_word'), rule.get('find_next_word'), rule.get('next_line'))

    result_set = []
    for crop in crop_info:
        image_name = "_".join(
            [project.get('client'), project.get('project'), project.get('discipline'),
             crop.get('page_total').zfill(3), crop.get('page_no').zfill(3), project.get('file_name')])

        # 이미지 가져오기
        image_file = AWSModule.get_s3_file(
            project.get('client'), project.get('project'), project.get('discipline'), image_name)
        logger.debug("Got an image from S3")

        # 이미지 자르기
        cropping_coordinate = (
            image_file.width * 0,
            image_file.height * crop.get('pos_top'),
            image_file.width * 1,
            image_file.height * crop.get('pos_bottom')
        )
        cropped_img = ParsingModule.cropping_image(image_file, cropping_coordinate)
        logger.debug("Image Cropped")

        # crop 이미지에서 bounding box 얻기
        bounding_boxes = ParsingModule.process_bounding_image(cropped_img)
        logger.debug("Get the boundaries of the box from the cropped image.")

        # crop 이미지에서 word 구하기
        text_extractions = AWSModule.extract_word_by_image(cropped_img, image_file.format)
        logger.debug("Extract text with ocr(textract)")
        # text_extractions = AWSModule.extract_word_by_local_image(cropped_img, image_file.format)
        word_extractions = ParsingModule.extract_words(text_extractions, cropped_img.width, cropped_img.height)
        logger.debug("Got the words from ocr results")

        # box, word 병합하기
        cell_dataset, title_coordinate = \
            ParsingModule.combine_word_into_box(bounding_boxes, word_extractions, rule.get('find_word'))
        logger.debug("Combined word into box")

        # 테이블 텍스트 구하기
        result_set.extend(ParsingModule.extract_crop_data(cell_dataset, title_coordinate, crop))
        logger.debug("Table Text Extracted")

    if crop_info:
        if result_set:
            res_in_params = []
            for n, result in enumerate(result_set):
                param_list = ParsingModule.set_crop_result_value(project, rule, te_comment, rule.get('rowseq') + n,
                                                                 result)
                res_in_params.append(list(param_list.values()))
            res_in_count = DBModule.insert_parse_result_set(res_in_params)
            logger.debug(f"extract table saved:{res_in_count}")
            state_code, state_msg, num_page = "00", "OK", result_set[0].get('page_no')
        else:
            state_code, state_msg, num_page = "12", "Text Extraction is None", crop_info[0].get('page_no')
    else:
        state_code, state_msg = "11", "Content Not Found"

    in_params = ParsingModule.set_status_detail(project, rule, state_code, state_msg, num_page)
    status_d_in_cnt = DBModule.insert_status_detail(list(in_params.values()))
    logger.debug(f"extraction result db logging:{status_d_in_cnt}")

    return state_code, state_msg, result_set


def extract_image(project, rule, te_comment):
    logger.debug(f"<-{rule.get('if_name')},{rule.get('key_name')}")
    logger.debug(
        f"{rule.get('find_word')}-{rule.get('find_next_word')}-{rule.get('next_line')}-{rule.get('extract_rule')}")

    state_code, state_msg, num_page = "00", "", None
    limit_num = rule.get('next_line', 1)
    if limit_num < 0:
        limit_num = -limit_num

    crop_info = ParsingModule.find_head_detail_row_reverse(
        project.get('client'), project.get('project'), project.get('discipline'), project.get('file_name'),
        rule.get('find_word'), rule.get('find_next_word'), limit_num)
    print(crop_info)

    result_set = []
    for crop in crop_info:
        image_name = "_".join(
            [project.get('client'), project.get('project'), project.get('discipline'),
             crop.get('page_total').zfill(3), crop.get('page_no').zfill(3), project.get('file_name')])
        print(image_name)
        # 이미지 가져오기
        image_file = AWSModule.get_s3_file(
            project.get('client'), project.get('project'), project.get('discipline'), image_name)
        logger.debug("Got an image from S3")

        # crop 이미지에서 bounding box 얻기
        bounding_boxes = ParsingModule.process_bounding_image(image_file)
        logger.debug("Get the boundaries of the box from the image.%s", len(bounding_boxes))

        # crop 이미지에서 word 구하기
        text_extractions = AWSModule.extract_word_by_image(image_file, image_file.format)
        logger.debug("Extract text with ocr(textract)")
        word_extractions = ParsingModule.extract_words(text_extractions, image_file.width, image_file.height)
        logger.debug("Got the words from ocr results")

        # box, word 병합하기
        cell_dataset, title_coordinate = \
            ParsingModule.combine_word_into_box(bounding_boxes, word_extractions, rule.get('find_word'))
        logger.debug("Combined word into box")
        logger.debug("title box: %s", title_coordinate)

        # 테이블 텍스트 구하기
        result_set.extend(ParsingModule.extract_image_data_reverse(cell_dataset, title_coordinate, crop))
        logger.debug("Table Text Extracted")

    if crop_info:
        if result_set:
            res_in_params = []
            for n, result in enumerate(result_set):
                param_list = ParsingModule.set_crop_result_value(project, rule, te_comment, rule.get('rowseq') + n,
                                                                 result)
                res_in_params.append(list(param_list.values()))
            res_in_count = DBModule.insert_parse_result_set(res_in_params)
            logger.debug(f"extract table saved:{res_in_count}")
            state_code, state_msg, num_page = "00", "OK", result_set[0].get('page_no')
        else:
            state_code, state_msg, num_page = "12", "Text Extraction is None", crop_info[0].get('page_no')
    else:
        state_code, state_msg = "11", "Content Not Found"

    in_params = ParsingModule.set_status_detail(project, rule, state_code, state_msg, num_page)
    status_d_in_cnt = DBModule.insert_status_detail(list(in_params.values()))
    logger.debug(f"extraction result db logging:{status_d_in_cnt}")

    return state_code, state_msg, result_set


def extract_process(project_set, rule_set, te_name):
    logger.debug("in extract process func")
    state_list = []
    for project in project_set:
        logger.debug(f"{project.get('project')}:{project.get('file_name')}")
        delete_count = DBModule.delete_result(
            project.get('client'), project.get('project'), project.get('discipline'), project.get('file_name'))
        logger.debug("parse data deleted: %d", delete_count)

        ext_count = 0
        for rule in rule_set:
            state_code, state_msg = "00", ""
            extract_set = []
            if rule.get('extract_method') == 'regex':
                state_code, state_msg, extract_set = extract_regex(project, rule, te_name)
            elif rule.get('extract_method') == 'cropped':
                state_code, state_msg, extract_set = extract_crop(project, rule, te_name)
            elif rule.get('extract_method') == 'image':
                state_code, state_msg, extract_set = extract_image(project, rule, te_name)
            ext_count += 1 if state_code.startswith("0") else 0

        if rule_set:
            if len(rule_set) == ext_count:
                status_code, status_msg = "00", "OK"
            elif len(rule_set) < ext_count:
                status_code, status_msg = "01", "extraction result is more than the rule."
            else:
                status_code, status_msg = "02", "extraction result is less than the rule"
        else:
            status_code, status_msg = "11", "Extraction Rules is not found"

        in_params = ParsingModule.set_status_value(project, rule_set, ext_count, status_code, status_msg)
        logger.debug("status(%s): %s ", status_code, status_msg)
        status_in_cnt = DBModule.insert_status_value(list(in_params.values()))
        logger.debug(f"extraction status db logging:{status_in_cnt}")
        state_list.append({
            "filename": project.get('file_name'),
            "state": status_code,
            "massage": status_msg,
            "tot_count": len(rule_set),
            "ext_count": ext_count
        })

    return state_list


def run_te(client, project, discipline, comment):
    logger.info("Table Extraction start ...")
    start_time = datetime.datetime.now()
    start_tm = start_time

    project_files = DBModule.get_project_files(client, project, discipline)
    # logger.debug(project_files)
    end_tm = datetime.datetime.now()
    logger.info(f"Project Read Complete - count: {len(project_files)}({end_tm - start_tm})")
    start_tm = end_tm

    extract_rule = DBModule.get_project_rule(client, project, discipline)
    # logger.debug(extract_rule)
    end_tm = datetime.datetime.now()
    logger.info(f"Rule Read Complete - count: {len(extract_rule)}({end_tm - start_tm})")
    start_tm = end_tm

    logger.info("Extract Text start ...")
    ret = extract_process(project_files, extract_rule, comment)
    end_tm = datetime.datetime.now()
    logger.info(f"Extract Text Complete - ({end_tm - start_tm})")

    logger.info(f"Table Extraction end.({datetime.datetime.now() - start_time})")
    return ret


if __name__ == '__main__':
    logger.debug("in parameters: %s", sys.argv)
    arg_val = " ".join(sys.argv[1:]).split("_")
    logger.debug("parsed parameters: %s", arg_val)
    if len(arg_val) == 4:
        resp = run_te(*arg_val)
        # resp = ["OK"]
    else:
        resp = "Need to check the arguments"

    # msg = {"content": "".join(resp)} if isinstance(resp, list) else {"error": resp}
    msg = {
        "status": "success" if isinstance(resp, list) else "fail",
        "data": resp if isinstance(resp, list) else "",
        "error": "" if isinstance(resp, list) else resp,
        "params": arg_val
    }

    return_data = json.dumps(msg)

    print(return_data)
