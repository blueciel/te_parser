import io
import json
import os

import boto3
from PIL import Image
from botocore.client import Config

from modules import Utils

is_debug = False

S3_ENDPOINT = "https://bucket.vpce-086027940a9933650-bbbysg49.s3.ap-northeast-2.vpce.amazonaws.com"
S3_BUCKET = "samsung-itb"
S3_SUBDIR = "out/Binarization"


def aws_ssm_parameters(region_nm="ap-northeast-2"):
    """
    AWS Simple Systems Manager(SSM)을 통해 정보를 얻기
    :param region_nm: (str) AWS region
    :return: (dictionary) AWS SSM parameters
    """
    rtn_val = {}
    if not is_debug:
        ssm_client = boto3.client("ssm", region_name=region_nm)
        ssm_names = ["/itb/aurora/endpoint", "/itb/aurora/port", "/itb/aurora/username", "/itb/aurora/password",
                     "/itb/aurora/db_comprehend", "/itb/s3/endpoint"]
        ssm_params = ssm_client.get_parameters(Names=ssm_names, WithDecryption=True)
        for param in ssm_params.get("Parameters"):
            rtn_val.setdefault(param.get("Name"), param.get("Value"))
    else:
        rtn_val.setdefault("/itb/aurora/endpoint", "database-1.cluster-ckah5qj688fb.ap-northeast-2.rds.amazonaws.com")
        rtn_val.setdefault("/itb/aurora/port", "3306")
        rtn_val.setdefault("/itb/aurora/username", "itbadmin")
        rtn_val.setdefault("/itb/aurora/password", "gogobsgo2020!")
        rtn_val.setdefault("/itb/aurora/db_comprehend", "itb")
        rtn_val.setdefault("/itb/s3/endpoint",
                           "https://bucket.vpce-086027940a9933650-bbbysg49.s3.ap-northeast-2.vpce.amazonaws.com")

    return rtn_val


def rds_info():
    """
    AWS RDS 접속 정보를 확인
    :return: (dict) RDS 접속 정보
    """
    value_set = aws_ssm_parameters()

    rtn_val = {}
    rtn_val.setdefault("host", value_set.get("/itb/aurora/endpoint"))
    rtn_val.setdefault("port", int(value_set.get("/itb/aurora/port")))
    rtn_val.setdefault("user", value_set.get("/itb/aurora/username"))
    rtn_val.setdefault("passwd", value_set.get("/itb/aurora/password"))
    rtn_val.setdefault("db", value_set.get("/itb/aurora/db_comprehend"))
    rtn_val.setdefault("charset", "utf8")

    return rtn_val


def s3_file_list(suffix, _prefix=""):
    s3 = boto3.resource('s3', endpoint_url=S3_ENDPOINT)
    bucket = s3.Bucket(name=S3_BUCKET)
    s3_json_file_list = []

    for obj in bucket.objects.filter(Prefix=_prefix):
        if obj.key.lower().endswith(suffix):
            s3_json_file_list.append(obj.key)

    return s3_json_file_list


def s3_file_resource(file_uri):
    s3_resource = boto3.resource('s3', endpoint_url=S3_ENDPOINT)
    s3_object = s3_resource.Object(S3_BUCKET, file_uri)

    s3_response = s3_object.get()
    stream = io.BytesIO(s3_response.get('Body').read())

    image = Image.open(stream)
    return image


def get_s3_file(client, project, discipline, img_name):
    s3_files = s3_file_list(".jpg",
                            os.path.join(client, project, discipline, S3_SUBDIR))
    s3_filepath = Utils.get_path_by_filename(s3_files, img_name)
    s3_image = s3_file_resource(s3_filepath)
    # s3_image = Image.open(
    #     "C:\\workspace\\samples\\R0BX_ADNOC_Hail Gasha_MDS_006_003_ADNOC-Hail Ghasha-VESSEL MDS-Mark-Up_00.jpg")
    return s3_image


def text_extract_by_image(image, image_format):
    byte_arr = io.BytesIO()
    image.save(byte_arr, format=image_format)
    image_binary = byte_arr.getvalue()

    ocr_client = boto3.client("textract")
    response = ocr_client.detect_document_text(Document={'Bytes': image_binary})

    return response


def extract_word_by_local_image(image, image_format):
    print(image_format)
    with open("C:\\workspace\\samples\\R0BX_ADNOC_Hail Gasha_MDS_006_003_ADNOC-Hail Ghasha-VESSEL MDS-Mark-Up_00.json",
              "r") as f:
        response = json.load(f)
    img_height, img_width = image.shape
    blocks = response.get('Blocks')
    word_blocks = [b for b in blocks if b.get('BlockType') == "WORD" and b.get('Confidence') > 60 and b.get('Text')]

    words = []
    for block in word_blocks:
        if block.get('Text'):
            word_info = {"Text": block.get('Text')}
            geometry = block.get('Geometry')
            w, h, x, y = geometry.get('BoundingBox').values()
            word_info.setdefault('box', [img_width * x, img_height * y, img_width * (x + w), img_height * (y + h)])
            words.append(word_info)

    return words


def extract_word_by_image(image, image_format):
    byte_arr = io.BytesIO()
    image.save(byte_arr, format=image_format)
    image_binary = byte_arr.getvalue()

    config = Config(retries=dict(max_attempts=5), region_name="ap-northeast-2")
    ocr_client = boto3.client("textract", config=config)
    response = ocr_client.detect_document_text(Document={'Bytes': image_binary})

    return response
