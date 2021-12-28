import re

import pandas

from textract_mds.moduless import awsmodule

def extract_text(_text_val, _re_pattern=r"\d*\.?\d+"):
    matched = re.findall(_re_pattern, _text_val)
    return matched


def cropping_adnoc_bechtel(data_set):
    pos_top = data_set[0].get("pos_top")
    img_name = "_".join([data_set[0].get("client"), data_set[0].get("project"), data_set[0].get("discipline"),
                         data_set[0].get("page_total").zfill(3), data_set[0].get("page_no").zfill(3),
                         data_set[0].get("file_name_origin")])
    pos_bottom = 0
    blank_cnt = 0
    for row in data_set:
        if row.get("content").find("SKIRT OPENINGS") > -1:
            pos_bottom = row.get("pos_top")
            break
        if re.findall(r"^\d+$", row.get("content")):
            blank_cnt += 1
        else:
            blank_cnt = 0
        if blank_cnt > 4:
            pos_bottom = row.get("pos_top")
            break

    analyzed = awsmodule.cropping_analyze(data_set[0].get("client"), data_set[0].get("project"),
                                          data_set[0].get("discipline"), img_name, (0, pos_top, 1, pos_bottom))

    ext_result = awsmodule.get_analyzed_result(analyzed)
    ext_result = [r for r in ext_result if "".join(r[1:])]
    ext_data = []
    for data in ext_result:
        obj = data[0].split(" ")
        obj.extend(data[1:])
        ext_data.append(obj)

    rtn_val = []
    df = pandas.DataFrame(ext_data[1:], columns=ext_data[0])
    print(df.columns)
    for n, row in df.iterrows():
        if row.get("Ref"):
            refs = row.get("Ref").split("/")
            for p, ref in enumerate(refs):
                arr = [ref if not p else refs[0][0:-len(ref)] + ref]
                arr.extend([row.get("Size NPS"), row.get("Rating#"), row.get("Facing")])
                arr.extend([row.get("Service"), row.get("Remarks")])
                rtn_val.append(arr)
    return rtn_val


def cropping_aramco_worleyparsons(data_set):
    pos_top = data_set[0].get("pos_top")
    pos_bottom = data_set[-1].get("pos_bottom")
    img_name = "_".join([data_set[0].get("client"), data_set[0].get("project"), data_set[0].get("discipline"),
                         data_set[0].get("page_total").zfill(3), data_set[0].get("page_no").zfill(3),
                         data_set[0].get("file_name_origin")])
    facing = ""
    # Rating 600# Facing RF Finish Note 19M Insulation None Fireproofing None
    for row in data_set:
        ext_txt = extract_text(row.get("content"), r"Facing (\w+) ")
        facing = ext_txt[0] if ext_txt else facing

    analyzed = awsmodule.cropping_analyze(data_set[0].get("client"), data_set[0].get("project"),
                                          data_set[0].get("discipline"), img_name, (0, pos_top, 0.52, pos_bottom))

    ext_result = awsmodule.get_analyzed_result(analyzed)

    rtn_val = []
    # Item No. Size Rating Service
    df = pandas.DataFrame(ext_result[1:], columns=ext_result[0])
    df.drop("No.", axis=1)
    df.insert(3, "Facing", facing)
    df["Remark"] = ""
    for n, row in df.iterrows():
        if row.get("Item"):
            row["Rating"] = row.get("Rating").replace("#", "")
            row["Size"] = row.get("Size").replace("\"", "")
            items = row.get("Item").split(",")
            row_list = row.values.tolist()
            for item in items:
                arr = [item.strip()]
                arr.extend(row_list[1:])
                rtn_val.append(arr)
    return rtn_val
