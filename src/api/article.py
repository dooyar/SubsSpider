import sys
# from pathlib import Path
import re
import time
import traceback
import urllib3
import signal
import dateparser
from pyquery import PyQuery as Pq
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from utils.settings import DATETIME_REGEXES
from src.spider_base import SessionBase
from utils.tools import gen_invalid_record, duplicate_filter, convert_to_relative_path, save_page, \
    generate_path, downloader

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SHUTDOWN_FLAG = False
__BIZ = {
    "MzA5NDY5MzUzMQ==": {
        "province": '北京',
        "city": '北京',
        "site": '首都之窗'
    },
    "MzA4NTIyMjMyMw==": {
        "province": '北京',
        "city": '北京',
        "site": '北京发布'
    },
    "MzI4OTE5NzI3OA==": {
        "province": '北京',
        "city": '北京',
        "site": '北京公积金'
    },
    "MjM5ODI2MDgwNQ==": {
        "province": '北京',
        "city": '北京',
        "site": '北京人社'
    },
    "MzAxNTA1Njc4Mw==": {
        "province": '北京',
        "city": '北京',
        "site": '北京税务'
    },
    "MzI0NzE5Nzc0Mg==": {
        "province": '上海',
        "city": '上海',
        "site": '上海社保'
    },
    "MjM5NTA5NzYyMA==": {
        "province": '上海',
        "city": '上海',
        "site": '上海发布'
    },
    "MzI3NDAxNDI4OQ==": {
        "province": '广西',
        "city": '广西',
        "site": '广西人社'
    },
    "MzA5NTg2MTEwMA==": {
        "province": '广东',
        "city": '广州',
        "site": '广州人社'
    },
    "MzAwNDczMzYzNg==": {
        "province": '云南',
        "city": '昆明',
        "site": '昆明公积金'
    },
    "MzI5OTE0MjU0Ng==": {
        "province": '云南',
        "city": '昆明',
        "site": '昆明人社局'
    },
    "MzAwODEyOTA5NA==": {
        "province": '江苏',
        "city": '南京',
        "site": '南京公积金'
    },
    "MzkyMDI5ODI2NA==": {
        "province": '江苏',
        "city": '南京',
        "site": '南京人社'
    },
    "Mzg2NDA5MTE1Ng==": {
        "province": '江苏',
        "city": '南京',
        "site": '南京医保'
    },
    "MzIzMjQ1MzgwNA==": {
        "province": '广东',
        "city": '深圳',
        "site": '深圳人社'
    },
    "Mzg4NDU5MTI3NA==": {
        "province": '广东',
        "city": '深圳',
        "site": '深圳医保'
    },
    "MzU3NjY0MzIzMg==": {
        "province": '广东',
        "city": '深圳',
        "site": '深圳税务'
    },
    "MzAwMzYyODY2NA==": {
        "province": '天津',
        "city": '天津',
        "site": '天津公积金'
    },
    "MzA3NjAyMzM2Ng==": {
        "province": '天津',
        "city": '天津人社',
        "site": '深圳税务'
    },
    "MzA3MzIyMjAxOQ==": {
        "province": '天津',
        "city": '天津',
        "site": '天津医保'
    },
    "MzAwNDM3OTI0OQ==": {
        "province": '湖北',
        "city": '武汉',
        "site": '武汉公积金'
    },
    "MzI3Nzk0NjQ4MQ==": {
        "province": '湖北',
        "city": '武汉',
        "site": '武汉人社'
    },
    "MzUzOTI0NjE3NQ==": {
        "province": '广东',
        "city": '珠海',
        "site": '珠海税务'
    },
    "MzA3MjE1ODYzOA==": {
        "province": '广东',
        "city": '珠海',
        "site": '珠海社保'
    }
}
DELAY_ONLINE = {
    "MzI5NTU3NzgyMg==": {
        "province": '北京',
        "city": '北京',
        "site": '北京财政'
    },
    "MzA5NDY5MzUzMQ==": {
        "province": '广东',
        "city": '潮州',
        "site": '潮州公积金'
    },
    "MjM5OTYwMDAxOA==": {
        "province": '广东',
        "city": '潮州',
        "site": '潮州人社'
    },
    "Mzg2MDc1NjU2Mw==": {
        "province": '山西',
        "city": '大同',
        "site": '大同天下'
    }
}


class ArticleSpider(SessionBase):
    def __init__(self, biz: str = None, base_type: int = None, **kwargs):
        super().__init__(biz=biz, base_type=base_type, **kwargs)
        self.province = kwargs.get("province")
        self.city = kwargs.get("city")
        self.site = kwargs.get("site")
        self.categories = '公众号'
        self.page_record = {
            'province': self.province,
            'city': self.city,
            'site': self.site,
            'category': None,
            'page_url': None,
            'page_release_date': None,
            'page_source': None,
            'title': None,
            'content': None,
            'attachment_name': None,
            'record_path': None,
            'attachment_path': None,
            'created_time': None
        }

    def start_request(self):
        category_path = self.output_path.joinpath(self.site, self.categories)
        if not category_path.exists():
            category_path.mkdir(parents=True)

        articles = self.get_article()
        logger.info(f"Get articles: {len(articles)}")
        page_records = list()
        for article in articles:
            link = article.get('link')
            title = article.get('title')
            today = datetime.now().strftime("%Y-%m-%d 00:00:00")
            release_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(article.get("create_time")))
            if release_time < today:
                logger.info(f"Posted in the past: [{title}]\t*{release_time}*")
                continue
            if duplicate_filter(self.db, self.page_table, link):
                logger.info(f"Duplicate link: {link}, pass")
                continue
            logger.info(f"title: {article.get('title')}, link: {article.get('link')}")

            resp = self.link_session.get(url=link, verify=False, timeout=3)
            if resp.status_code != 200:
                logger.info(f"link failed: {link}")
                page_records.append(gen_invalid_record(self.page_record, self.categories, link))
                continue
            text = resp.text

            basename = re.search(r"sn=.*&", link).group().replace("=", "").replace("&", "")
            record_path, attachment_path = generate_path(category_path, basename)
            with open(record_path.joinpath(f'{title}.html'), 'w', encoding='utf-8-sig') as f:
                f.write(text)

            record = self.parse_page(resp.text)
            attachment_name = self.download_attachments(resp.text, attachment_path)
            for regex in DATETIME_REGEXES:
                result = re.search(regex, release_time)
                if result:
                    record['page_release_date'] = dateparser.parse(result.group(1))
                    break
            record["page_url"] = link
            record["category"] = self.categories
            record['record_path'] = convert_to_relative_path(record_path)
            record['attachment_name'] = attachment_name
            record['attachment_path'] = convert_to_relative_path(attachment_path)
            page_records.append(record)
        if page_records:
            save_page(self.db, self.page_table, page_records)

    def parse_page(self, text) -> dict:
        page_record = self.page_record.copy()
        page_record['created_time'] = datetime.now()
        doc = Pq(text)
        title = doc("div#img-content > h1#activity-name").text().strip()
        source = doc("div#img-content div#meta_content a#js_name").text().strip()
        content = doc("div#img-content div#js_content").text().strip().replace("\n", " ").replace("\r", " ")

        page_record["title"] = title
        page_record["page_source"] = source
        page_record["content"] = content
        return page_record

    @staticmethod
    def download_attachments(text, attachment_path):
        doc = Pq(text)
        attachments = doc("div#img-content div#js_content img")
        idx = 0
        attachment_name = list()
        for attachment in attachments.items():
            url = attachment.attr("data-src")
            extension = attachment.attr("data-type")
            if f".{extension}" in ['.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.pdf', '.jpg', '.jpeg', '.png',
                                   '.webp', '.bmp', '.zip', '.rar', '.7z']:
                idx += 1
                filename = f"img_{idx}." + extension
                download_path = attachment_path.joinpath(filename)
                try:
                    downloader(url, download_path)
                except Exception as e:
                    pass
                else:
                    attachment_name.append(filename)
        return ', '.join(attachment_name)


def task(__biz: str = None, **kwargs):
    """
    任务
    :param __biz:
    :param kwargs:
    :return:
    """
    global SHUTDOWN_FLAG

    province = kwargs.get("province")
    city = kwargs.get("city")
    site = kwargs.get("site")

    if SHUTDOWN_FLAG:
        logger.info(f"System signal to exit: [{province}/{city} - {site}]")
        return
    if not __biz:
        logger.info(f"[{province}/{city} - {site}], required '__biz' argument")
        return
    logger.info(f"[{province}/{city} - {site}] into execution")
    try:
        config = ArticleSpider.__config_init__()
        config.get("subscription").update({
            "biz": __biz
        })
        spider = ArticleSpider(**config.get("subscription"), **kwargs)
        spider.start_request()
        logger.info(f"Execution completed: [{province}/{city} - {site}]")
        return True
    except Exception as e:
        logger.error(f"An error occurred! \nTrigger: {site} \nError: {traceback.format_exc()}")
    return


def signal_handler(_signo, _stack_frame):
    """
    捕获信号
    :param _signo:
    :param _stack_frame:
    :return:
    """
    global SHUTDOWN_FLAG
    logger.info(f'Process _signo: {_signo}, system try to stop and exit')
    SHUTDOWN_FLAG = True


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    start_time = time.time()
    visualize_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        thread_amounts = int(sys.argv[1])
    except Exception as exc:
        thread_amounts = 10

    logger.info(f"{visualize_time}")
    logger.info(f"(Subscription) Spider script will start up, ThreadPoolExecutor: [{thread_amounts}]")
    thread_pool = ThreadPoolExecutor(max_workers=thread_amounts)

    for _biz, one in __BIZ.items():
        thread_pool.submit(task, __biz=_biz, **one)
        break

    thread_pool.shutdown()
    logger.info("# End at {}, consuming time: {:.2f}s, process exit.".format(
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"), time.time() - start_time))