import argparse
import re
import sys


def read_argument_value():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("params", nargs="+")
    args = arg_parser.parse_args()

    rtn_val = {}
    if len(args.params) == 3:
        rtn_val.setdefault("client", args.params[0])
        rtn_val.setdefault("project", args.params[1])
        rtn_val.setdefault("discipline", args.params[2])
    elif len(args.params) == 5 or len(args.params) == 7:
        rtn_val = args.params
    elif len(args.params) == 0:
        sys.exit("arguments are required")
    else:
        sys.exit("Need to check the arguments")
    return rtn_val


def extract_text(text_val, re_pattern=r"\d*\.?\d+"):
    matched = re.findall(re_pattern, text_val)
    return matched


def get_path_by_filename(files, filename):
    for f in files:
        if f.find(filename) > -1:
            return f
    raise ValueError(f"image file not found")


def get_block_by_id(id_val, ext_blocks):
    for b in ext_blocks:
        if b.get("Id") == id_val:
            return b
    raise ValueError(f"no b for id: {id_val}")


def split_key(key_value):
    none_word = re.findall(r"([^\w]+)", key_value)
    keys = key_value.split(none_word[0]) if none_word else [key_value]
    keys = [keys[0][:-len(k)] + k for k in keys]
    return keys
