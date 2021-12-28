import io
import os

import boto3
import pandas
from PIL import Image
from botocore.client import Config

S3_ENDPOINT = "https://bucket.vpce-086027940a9933650-bbbysg49.s3.ap-northeast-2.vpce.amazonaws.com"
S3_BUCKET = "samsung-itb"
S3_SUBDIR = "out/Binarization"


def s3_file_list(bucket, endpoint, suffix, _prefix=""):
    s3 = boto3.resource('s3', endpoint_url=endpoint)
    bucket = s3.Bucket(name=bucket)
    s3_json_file_list = []

    for obj in bucket.objects.filter(Prefix=_prefix):
        if obj.key.lower().endswith(suffix):
            s3_json_file_list.append(obj.key)

    return s3_json_file_list


def get_path_by_filename(files, filename):
    for f in files:
        if f.find(filename) > -1:
            return f
    raise ValueError(f"image file not found")


def get_block_by_id(_id, _blocks):
    for b in _blocks:
        if b.get("Id") == _id:
            return b
    raise ValueError(f"no b for id: {_id}")


def cropping_analyze(client, project, discipline, filename, pos):
    img_files = s3_file_list(S3_BUCKET, S3_ENDPOINT, "_00.jpg",
                             os.path.join(client, project, discipline, S3_SUBDIR))
    img_file = get_path_by_filename(img_files, filename)
    print(img_file)
    # S3 이미지파일 일기
    s3_resource = boto3.resource('s3', endpoint_url=S3_ENDPOINT)
    s3_object = s3_resource.Object(S3_BUCKET, img_file)

    s3_response = s3_object.get()
    stream = io.BytesIO(s3_response.get('Body').read())

    image = Image.open(stream)
    # 이미지 파일 분할
    left, top, right, bottom = pos
    cropped_img = image.crop((image.width * left, image.height * top, image.width * right, image.height * bottom))

    byte_arr = io.BytesIO()
    cropped_img.save(byte_arr, format=image.format)
    cropped_binary = byte_arr.getvalue()

    config = Config(retries=dict(max_attempts=5))

    ocr_client = boto3.client('textract', config=config)
    rtn_val = ocr_client.analyze_document(Document={'Bytes': cropped_binary}, FeatureTypes=["TABLES", "FORMS"])

    return rtn_val


def get_analyzed_result(analyzed_data):
    # 전체 Block 얻기
    blocks = analyzed_data.get("Blocks")
    print(len(blocks))
    # tables = [block for block in blocks if block["BlockType"] == "TABLE"]
    cells = [block for block in blocks if block["BlockType"] == "CELL"]
    # Cell 정보를 추출(행, 열, 텍스트)
    cells_set = []
    for cell in cells:
        cell_set = {"RowIndex": cell.get("RowIndex"), "ColumnIndex": cell.get("ColumnIndex")}
        # Cell 하위 id 추출
        relationships = cell.get("Relationships", [])
        child_ids = relationships[0].get("Ids") if len(relationships) == 1 else []
        # 하위 Block 구해서 Text 추출
        child_text = []
        for child_id in child_ids:
            child_block = get_block_by_id(child_id, blocks)
            child_text.append(child_block.get("Text", None))
        cell_set["Text"] = " ".join(child_text)

        cells_set.append(cell_set)
    # Cell 정보에서 행 별로 텍스트 구하기(pandas group by 이용)
    cells_data = pandas.DataFrame(cells_set)
    rtn_val = []
    for row_id, row_set in cells_data.groupby("RowIndex"):
        lines = row_set["Text"].to_list()
        rtn_val.append(lines)

    return rtn_val


def adnoc_bechtel_nozzle(_data):
    rtn_val = []
    df = pandas.DataFrame(_data[1:], columns=_data[0])
    print(df.columns)
    for n, row in df.iterrows():
        if row.get("Ref"):
            refs = row.get("Ref").split("/")
            for p, ref in enumerate(refs):
                arr = [ref if not p else refs[0][0:-len(ref)] + ref]
                arr.extend([row.get("Size NPS"), row.get("Rating#"), row.get("Facing")])
                arr.extend([row.get("Service"), row.get("Remarks")])
                contents = [row.get("Ref")]
                contents.extend(arr[1:])
                arr.append(" ".join(contents))
                rtn_val.append(arr)
    return rtn_val


def aramco_worleyparsons(analyzed_data):
    rtn_val = []
    df = pandas.DataFrame(analyzed_data[1:], columns=analyzed_data[0])
    for n, row in df.iterrows():
        if row.get("Item"):
            row.pop("No.")
            row["Rating"] = row.get("Rating").replace("#", "")
            row["Size"] = row.get("Size").replace("\"", "")
            row["Remark"] = ""
            items = row.get("Item").split(",")
            row_list = row.values.tolist()
            for item in items:
                arr = [item.strip()]
                arr.extend(row_list[1:])
                rtn_val.append(arr)
    return rtn_val
