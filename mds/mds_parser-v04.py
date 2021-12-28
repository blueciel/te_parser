import datetime
import os

from modules import dbmodule
from modules import parsemodule


def get_path_info(_file_path: str):
    file_path, file_basename = os.path.split(_file_path)
    file_name, file_ext = os.path.splitext(file_basename)
    return file_path, file_basename, file_name, file_ext


def text_not_found(client, project, discipline, feeder, file_name, parse_rule):
    rtn_val = [client, project, discipline, discipline, feeder, "Vessel Design"]
    rtn_val.extend([parse_rule.get("seq"), parse_rule.get("category"), parse_rule.get("key_name")])
    rtn_val.extend(["N/A", None, None, file_name, None, None])
    return [rtn_val]


def find_selected_by_radio(data_set):
    rtn_val = "N/A"
    if data_set[-2].startswith("o ") and not data_set[-1].startswith("o "):
        print("case1")
        rtn_val = data_set[-1]
    elif not data_set[-2].startswith("o ") and data_set[-1].startswith("o "):
        rtn_val = data_set[-2]
        print("case2")
    else:
        print("case3")
        pass
    return rtn_val if len(data_set) < 3 or rtn_val == "N/A" else data_set[0] + rtn_val


def parse_line_text(client, project, discipline, feeder, file_name, parse_rule):
    rtn_val = []
    text_data = dbmodule.select_mds_data(client, project, discipline, file_name, parse_rule.get("find_word"))
    for row in text_data:
        print(row.get("content"))
        ext_text = parsemodule.extract_text(row.get("content"), parse_rule.get("extract_rule"))
        print(ext_text)
        if ext_text:
            obj = [row.get("client"), row.get("project"), row.get("discipline"), row.get("discipline"),
                   feeder, "Vessel Design"]
            obj.extend([parse_rule.get("seq"), parse_rule.get("category"), parse_rule.get("key_name")])
            if isinstance(ext_text[0], tuple):
                # ext = ext_text[0][0]
                # for t in ext_text[0][1:]:
                #     if t.startswith("o "):
                #         ext = ext.replace(t, "").strip()
                # print(ext)
                # obj.append(ext)
                obj.append(find_selected_by_radio(ext_text[0]))
            else:
                obj.append(ext_text[0] if ext_text else "N/A")
            obj.extend([row.get("page_total"), row.get("page_no"), row.get("file_name_origin")])
            obj.extend([row.get("num"), row.get("content")])
            rtn_val.append(obj)
    return rtn_val or text_not_found(client, project, discipline, feeder, file_name, parse_rule)


def parse_multi_text(client, project, discipline, feeder, file_name, parse_rule):
    rtn_val = []
    text_data = dbmodule.select_mds_data(client, project, discipline, file_name, parse_rule.get("find_word"))
    for row in text_data:
        next_data = dbmodule.select_next_data(client, project, discipline, file_name,
                                              parse_rule.get("find_next_word")
                                              if parse_rule.get("find_next_word") != "ALL LINE READ" else "",
                                              row.get("page_no"), row.get("num"), parse_rule.get("next_line"))

        for next_row in next_data:
            print(parse_rule.get("extract_rule"), next_row.get("content"))
            ext_text = parsemodule.extract_text(next_row.get("content"), parse_rule.get("extract_rule"))
            print(ext_text)
            if ext_text:
                obj = [next_row.get("client"), next_row.get("project"), next_row.get("discipline"),
                       next_row.get("discipline"), feeder, "Vessel Design"]
                obj.extend([parse_rule.get("seq"), parse_rule.get("category"), parse_rule.get("key_name"), ext_text[0]])
                # obj.append(ext_text[0] if ext_text else "N/A")
                obj.extend([next_row.get("page_total"), next_row.get("page_no"), next_row.get("file_name_origin")])
                obj.extend([next_row.get("num"), next_row.get("content")])
                rtn_val.append(obj)
        if len(rtn_val) > 0:
            break
    return rtn_val or text_not_found(client, project, discipline, feeder, file_name, parse_rule)


def parse_sub_text(client, project, discipline, feeder, file_name, parse_rule):
    rtn_val = []
    sub_find_word = "CONDITION 1"
    if project == "Borouge" and feeder == "Tecnimont":
        if parse_rule.get("key_name").find("ext") > -1:
            sub_find_word = "CONDITION 2"
    elif project == "RAPID" and feeder == "Tecnhip":
        if parse_rule.get("category") == "v-4" or parse_rule.get("category") == "v-9":
            sub_find_word = "Pressure"
        else:
            sub_find_word = "Temperature"

    text_data = dbmodule.select_mds_data(client, project, discipline, file_name, parse_rule.get("find_word"))
    for row in text_data:
        next_data = dbmodule.select_next_data(client, project, discipline, file_name,
                                              parse_rule.get("find_next_word")
                                              if parse_rule.get("find_next_word") != "ALL LINE READ" else "",
                                              row.get("page_no"), row.get("num"), parse_rule.get("next_line"))
        for n, next_row in enumerate(next_data):
            if next_row.get("content").find(sub_find_word) > -1:
                for sub_row in next_data[n - 1: n + 2]:
                    ext_text = parsemodule.extract_text(sub_row.get("content"), parse_rule.get("extract_rule"))
                    if ext_text:
                        obj = [client, project, discipline, discipline, feeder, "Vessel Design"]
                        obj.extend([parse_rule.get("seq"), parse_rule.get("category"), parse_rule.get("key_name"),
                                    ext_text[0]])
                        obj.extend([sub_row.get("page_total"), sub_row.get("page_no"), sub_row.get("file_name_origin")])
                        obj.extend([sub_row.get("num"), sub_row.get("content")])
                        rtn_val.append(obj)
    return rtn_val or text_not_found(client, project, discipline, feeder, file_name, parse_rule)


def parse_cropped_image(client, project, discipline, feeder, file_name, parse_rule):
    rtn_val = []
    parsed_set = []
    text_data = dbmodule.select_mds_data(client, project, discipline, file_name, parse_rule.get("find_word"))
    idx = 0
    for row in text_data:
        next_data = dbmodule.select_next_data(client, project, discipline, file_name, "",
                                              row.get("page_no"), row.get("num"), parse_rule.get("next_line"))

        if client == "ADNOC" and project == "Hail Gasha" and feeder == "BECHTEL":
            parsed_set = parsemodule.cropping_adnoc_bechtel(next_data)
        if client == "ARAMCO" and project == "UNAYZAH" and feeder == "WorleyParsons":
            parsed_set = parsemodule.cropping_aramco_worleyparsons(next_data)
        if client == "BOROUGE" and project == "Borouge" and feeder == "Tecnimont":
            parsed_set = parsemodule.cropping_borouge_tecnimont(next_data)
        if client == "PETRONAS" and project == "RAPID" and feeder == "Tecnhip":
            parsed_set = parsemodule.cropping_petronas_tecnhip(next_data)
        if client == "SAVIC" and project == "JUPC EOEG" and feeder == "WorleyParsons":
            parsed_set = parsemodule.cropping_savic_worleyparsons(next_data)

        for n, parse_text in enumerate(parsed_set):
            idx += 1
            obj = [row.get("client"), row.get("project"), row.get("discipline"), row.get("discipline"),
                   feeder, "Vessel Design"]
            obj.extend([parse_rule.get("seq") + (idx - 1), parse_rule.get("category") + str(idx)])
            obj.append(parse_rule.get("key_name"))
            obj.append("^".join(parse_text))
            obj.extend([row.get("page_total"), row.get("page_no"), row.get("file_name_origin")])
            obj.extend([row.get("num"), " ".join(parse_text)])
            rtn_val.append(obj)
    return rtn_val


def runner(pdf_file, feeder):
    # TE_name: "Concrete Strength Calc" -> "Vessel Design"
    start_time = datetime.datetime.now()
    print("MDS parsing ...")
    file_path, file_basename, file_name, file_ext = get_path_info(pdf_file)
    client, project, discipline = file_path.split("/")[-3:]

    result_set = []
    # parsing 룰 조회
    start_tm = datetime.datetime.now()
    rule_set = dbmodule.select_rules_by_project(project, feeder)
    end_tm = datetime.datetime.now()
    print(f"parsing rule read({end_tm - start_tm})")
    start_tm = end_tm

    for rule in rule_set:
        # print(f"{rule.get('key_name')} parsing....")
        if rule.get("extract_method") == "cropped":
            result_set.extend(parse_cropped_image(client, project, discipline, feeder, file_name, rule))
            # continue
        elif rule.get("extract_method") == "regex":
            if rule.get("find_next_word"):
                result_set.extend(parse_multi_text(client, project, discipline, feeder, file_name, rule))
            else:
                result_set.extend(parse_line_text(client, project, discipline, feeder, file_name, rule))
            # continue
        elif rule.get("extract_method") == "regex_sub_text":
            # print(rule)
            result_set.extend(parse_sub_text(client, project, discipline, feeder, file_name, rule))
        else:
            raise ValueError(f"Unknown parsing method: {rule.get('extract_method')}")
            # continue

        end_tm = datetime.datetime.now()
        # print(f"{rule.get('key_name')} parsed({end_tm - start_tm})")
        print(f"{rule.get('category')} parsed({end_tm - start_tm})")
        start_tm = end_tm

    del_count = dbmodule.delete_result(client, project, discipline, file_name)
    print(f"삭제완료: {del_count}건 삭제")

    result_set.sort(key=lambda x: x[6])
    for r in result_set:
        print(r)

    insert_count = dbmodule.save_result(result_set)
    print(f"작업완료: {insert_count}건 저장")

    print(f"MDS parsed ({datetime.datetime.now() - start_time})!")


if __name__ == "__main__":
    # target_file = "samsung-itb/ADNOC/Hail Gasha/MDS/ADNOC-Hail Ghasha-VESSEL MDS-Mark-Up.pdf"
    # target_feeder = "BECHTEL"

    # target_file = "samsung-itb/ARAMCO/UNAYZAH/MDS/ARAMCO-UNAYZAH-VESSEL MDS-Mark-Up.pdf"
    # target_feeder = "WorleyParsons"

    # target_file = "samsung-itb/BOROUGE/Borouge/MDS/BOROUGE-Borouge #4-VESSEL MDS-Mark-Up.pdf"
    # target_feeder = "Tecnimont"

    # target_file = "samsung-itb/PETRONAS/RAPID/MDS/PETRONAS-RAPID-VESSEL PDS-Mark-Up.pdf"
    # target_feeder = "Tecnhip"

    target_file = "samsung-itb/SAVIC/JUPC EOEG/MDS/SAVIC-JUPC EOEG-VESSEL MDS-Mark-Up.pdf"
    target_feeder = "WorleyParsons"

    runner(target_file, target_feeder)
