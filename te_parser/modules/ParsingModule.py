import cv2
import numpy as np
import pandas

from modules import Logger, DBModule, Utils

logger = Logger.make_logger("TE.parser")


def find_head_row(client, project, discipline, filename, word_head):
    rtn_val = DBModule.find_data_by_keyword(client, project, discipline, filename, word_head)
    return rtn_val


def find_detail_row(client, project, discipline, filename, word_head, word_detail, n_row):
    rtn_val = []
    rs = DBModule.find_data_by_keyword(client, project, discipline, filename, word_head)
    detail_word = "" if word_detail == "ALL LINE READ" else word_detail
    for rec in rs:
        detail_row = DBModule.find_data_by_detail_word(
            client, project, discipline, filename,
            rec.get("page_no"), rec.get("num"), detail_word, n_row)
        for detail_rec in detail_row:
            rtn_val.append(detail_rec)
    return rtn_val


def find_head_detail_row(client, project, discipline, filename, word_head, word_detail, n_row):
    rtn_val = []
    rs = DBModule.find_data_by_keyword(client, project, discipline, filename, word_head)
    detail_word = "" if word_detail == "ALL LINE READ" else word_detail
    for rec in rs:
        detail_row = DBModule.find_data_by_detail_word(
            client, project, discipline, filename,
            rec.get("page_no"), rec.get("num"), detail_word, n_row)
        for detail_rec in detail_row:
            obj = {
                "page_total": rec.get('page_total'), "page_no": rec.get('page_no'),
                "pos_top": rec.get('pos_top'), "pos_bottom": detail_rec.get('pos_bottom')}
            rtn_val.append(obj)
    return rtn_val


def find_head_detail_row_reverse(client, project, discipline, filename, word_head, word_detail, n_row):
    rtn_val = []
    rs = DBModule.find_data_by_keyword(client, project, discipline, filename, word_head)
    detail_word = "" if word_detail == "ALL LINE READ" else word_detail
    for rec in rs:
        detail_row = DBModule.find_data_by_detail_word_reverse(
            client, project, discipline, filename,
            rec.get("page_no"), rec.get("num"), detail_word, n_row)
        for detail_rec in detail_row:
            obj = {
                "page_total": rec.get('page_total'), "page_no": rec.get('page_no'),
                "pos_top": detail_rec.get('pos_top'), "pos_bottom": rec.get('pos_bottom')}
            rtn_val.append(obj)
    return rtn_val


def set_result_value(project, rule, comment, contents, ext_text):
    return {"client": project.get("client"), "project": project.get('project'),
            "discipline": project.get('discipline'), "feeder": rule.get('feeder'),
            "doc_type": project.get('discipline'), "te_name": comment, "seq": rule.get('rowseq'),
            "if_name": rule.get('if_name'), "key_name": rule.get('key_name'), "te_value": ext_text,
            "total_page": contents.get('page_total'), "page_no": contents.get('page_no'),
            "filename": project.get('file_name'), "num": contents.get('num'), "content": contents.get('content')}


def set_crop_result_value(project, rule, comment, num, extract_result):
    return {"client": project.get("client"), "project": project.get('project'),
            "discipline": project.get('discipline'), "feeder": rule.get('feeder'),
            "doc_type": project.get('discipline'), "te_name": comment, "seq": num,
            "if_name": f"{rule.get('if_name')}-{num}", "key_name": extract_result.get('key'),
            "te_value": extract_result.get("value"),
            "total_page": extract_result.get('page_total'), "page_no": extract_result.get('page_no'),
            "filename": project.get('file_name'), "num": None, "content": None}


def set_status_detail(project, rule, stat_cd, stat_msg, page_no=None, content=None):
    return {"client": project.get("client"), "project": project.get('project'),
            "discipline": project.get('discipline'), "filename": project.get('file_name'),
            "rule_id": rule.get('rule_id'), "seq": rule.get('ruleseq'), "page_no": page_no,
            "status_cd": stat_cd, "status_desc": stat_msg, "content": content}


def set_status_value(project, rule_set, ext_count, stat_cd, stat_msg):
    return {"client": project.get('client'), "project": project.get('project'),
            "discipline": project.get('discipline'), "filename": project.get('file_name'),
            "rule_cnt": len(rule_set), "extract_cnt": ext_count, "status_code": stat_cd,
            "status_msg": stat_msg, "file_url": project.get('file_url')}


def cropping_image(pil_image, coordinate):
    return pil_image.crop(coordinate)


def process_bounding_image(img):
    cv_img = np.array(img)
    # Length(width) of kernel as 100th of total width (img.shape[1])
    # kernel_len = np.array(img).shape[1] // 100
    kernel_len = cv_img.shape[1] // 100
    # Defining a vertical kernel to detect all vertical lines of image
    ver_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, kernel_len))
    # Defining a horizontal kernel to detect all horizontal lines of image
    hor_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (kernel_len, 1))
    # A kernel of 2x2
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))

    # thresholding the image to a binary image
    ret_bin, img_bin = cv2.threshold(cv_img, 128, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)

    # Use vertical kernel to detect and save the vertical lines in a jpg
    ver_img = cv2.erode(~img_bin, ver_kernel, iterations=3)
    vertical_lines = cv2.dilate(ver_img, ver_kernel, iterations=3)

    # Use horizontal kernel to detect and save the horizontal lines in a jpg
    hor_img = cv2.erode(~img_bin, hor_kernel, iterations=3)
    horizontal_lines = cv2.dilate(hor_img, hor_kernel, iterations=3)

    # Combine horizontal and vertical lines in a new third image, with both having same weight.
    img_vh = cv2.addWeighted(vertical_lines, 0.5, horizontal_lines, 0.5, 0.0)
    # Eroding and thesholding the image
    img_vh = cv2.erode(~img_vh, kernel, iterations=3)
    # ret_vh, img_vh = cv2.threshold(img_vh, 128, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)

    # 이진화
    thresh_np = np.zeros_like(img_vh)
    thresh_np[img_vh > 200] = 255

    # Detect contours for following box detection
    contours, hierarchy = cv2.findContours(thresh_np, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    bound_boxes = [cv2.boundingRect(c) for c in contours]  # x, y, w, h
    bound_boxes.sort(key=lambda bb: (bb[1], bb[0]))

    return bound_boxes


def extract_words(ocr_response, img_width, img_height):
    blocks = ocr_response.get('Blocks')
    word_blocks = [b for b in blocks if b.get('BlockType') == "WORD" and b.get('Confidence') > 60 and b.get('Text')]
    word_extractions = []
    for block in word_blocks:
        # if block.get('Text'):
        word_info = {"Text": block.get('Text')}
        geometry = block.get('Geometry')
        w, h, x, y = geometry.get('BoundingBox').values()
        word_info.setdefault('box', [img_width * x, img_height * y, img_width * (x + w), img_height * (y + h)])
        word_extractions.append(word_info)
    return word_extractions


def combine_word_into_box(bound_boxes, words, key_val):
    # bounding box 에 word list 병합
    text_boxes = []
    for box in bound_boxes:
        x, y, w, h = box
        text_box = {"bound": box}
        word_info = []
        for word in words:
            l, t, r, b = word.get('box')
            if x <= (l + r) / 2 <= x + w and y <= (t + b) / 2 <= y + h:
                word_info.append(word)
        # if word_info:
        text_box.setdefault("words", word_info)
        text_boxes.append(text_box)
    # bonding box 안 워드 리스트 병합
    table_cells = []
    title_box = ()
    for box in text_boxes:
        box_text = ""
        words_info = box.get("words")
        x, y, w, h = box.get('bound')
        if words_info:
            for n, word in enumerate(words_info[:-1]):
                box_text += word.get('Text')
                self_center = (word.get('box')[1] + word.get('box')[3]) / 2
                post_word = words_info[n + 1].get('box')
                post_top, post_bottom = post_word[1], post_word[3]
                if post_top <= self_center <= post_bottom:
                    box_text += " "
                else:
                    box_text += "\n"
            box_text += words_info[-1].get('Text')
        if key_val in box_text:
            title_box = box.get('bound')
        table_cells.append({"Text": box_text, "left": x, "top": y, "width": w, "height": h})

    return table_cells, title_box


def extract_crop_data(cell_dataset, title_coordinate, contents):
    pos_left, pos_top, pos_width, pos_height = title_coordinate
    cells_data = []
    for cell in cell_dataset:
        x, y, w, h = list(cell.values())[1:]
        if pos_left <= x and x + w <= pos_left + pos_width and y >= pos_top + pos_height:
            cells_data.append(cell)

    cells_frame = pandas.DataFrame(cells_data)
    ext_dataset = []
    for row_id, row_set in cells_frame.groupby("top"):
        row = row_set.get('Text').to_list()
        ext_dataset.append(row) if len(row) > 1 else ''

    headers = ext_dataset[0]
    result_set = []
    for ext_data in ext_dataset[1:]:
        keys = Utils.split_key(ext_data[0])
        for key in keys:
            if key:
                key_prefix = f"Nozzle_{key}"
                for n, ext in enumerate(ext_data):
                    obj = {
                        "key": f"{key_prefix}_{headers[n]}",
                        "value": ext,
                        "page_total": contents.get('page_total'),
                        "page_no": contents.get('page_no')
                    } if n else {}
                    result_set.append(obj) if obj else ""
    return result_set


def extract_image_data_reverse(cell_dataset, title_coordinate, contents):
    pos_left, pos_top, pos_width, pos_height = title_coordinate
    cells_data = []
    for cell in cell_dataset:
        x, y, w, h = list(cell.values())[1:]
        if pos_left <= x and x + w <= pos_left + pos_width:
            cells_data.append(cell)

    cells_frame = pandas.DataFrame(cells_data)
    ext_dataset = []
    for row_id, row_set in cells_frame.groupby("top"):
        row = row_set.get('Text').to_list()
        ext_dataset.append(row) if len(row) > 1 else ''

    headers = ext_dataset[-1]

    result_set = []
    for ext_data in ext_dataset[-2::-1]:
        keys = Utils.split_key(ext_data[0])
        for key in keys:
            if key:
                key_prefix = f"Nozzle_{key}"
                for n, ext in enumerate(ext_data):
                    obj = {
                        "key": f"{key_prefix}_{headers[n]}",
                        "value": ext,
                        "page_total": contents.get('page_total'),
                        "page_no": contents.get('page_no')
                    } if n else {}
                    result_set.append(obj) if obj else ""
    return result_set
