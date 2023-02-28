import re
import os
import yaml
import requests
from loguru import logger
from pathlib import Path
from sqlalchemy import exists
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
from retrying import retry


def convert_to_relative_path(path: Path):
    return re.sub(r"^.*?(?=output)", "", str(path))


def generate_path(category_path, basename):
    """
    生成爬虫本地文件目录
    :param category_path:
    :param basename:
    :return:
    """
    file_path = category_path.joinpath(basename)
    if not file_path.exists():
        file_path.mkdir()
    record_path = file_path.joinpath('record')
    if not record_path.exists():
        record_path.mkdir()
    attachment_path = file_path.joinpath('attachment')
    if not attachment_path.exists():
        attachment_path.mkdir()
    return record_path, attachment_path


def duplicate_filter(db, table, page_url):
    """
    筛选数据库重复link
    :param db:
    :param table:
    :param page_url:
    :return:
    """
    with db.engine.connect() as conn:
        with conn.begin():
            query = exists(table.c.page_url).select().\
                where(table.c.page_url == page_url)
            return conn.execute(query).scalar()


def gen_invalid_record(record, category, page_url):
    page_record = record.copy()
    page_record['category'] = category
    page_record['page_url'] = page_url
    page_record['created_time'] = datetime.now()
    return page_record


@retry(stop_max_attempt_number=3)
def downloader(url, save_path):
    """
    下载附件
    :param url:
    :param save_path:
    :return:
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36'
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    with open(save_path, 'wb') as f:
        f.write(resp.content)


def save_page(db, table, pages):
    """数据入库"""
    with db.engine.connect() as conn:
        with conn.begin():
            for page in pages:
                page_insert_stmt = insert(table).values(**page)
                conn.execute((page_insert_stmt.on_conflict_do_nothing(index_elements=['page_url'])))
                # on_duplicate_key_stmt = page_insert_stmt.on_duplicate_key_update(**page)
                # conn.execute(on_duplicate_key_stmt)


def load_yaml():
    """
    load src config for yaml file
    :return:
    """
    conf_name = "spider.yaml"
    try:
        conf_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
        with open(os.path.join(conf_path, conf_name), "r", encoding="utf-8") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        return config
    except Exception as e:
        logger.error(f"failed to load {conf_name}", exc_info=True)
        raise e
