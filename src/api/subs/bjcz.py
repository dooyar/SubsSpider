import sys
sys.path.append(".")
import re
import time
import urllib3
import dateparser
from pyquery import PyQuery as pq
from datetime import datetime
from loguru import logger
from utils.settings import DATETIME_REGEXES
from spider_base import SessionBase
from utils.tools import gen_invalid_record, duplicate_filter, convert_to_relative_path, save_page, \
    generate_path, downloader

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class BJCZArticleSpider(SessionBase):
    def __init__(self, biz: str = None, base_type: int = None, **kwargs):
        super().__init__(biz=biz, base_type=base_type, **kwargs)
        self.province = '北京'
        self.city = '北京'
        self.site = '北京财政'
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
        doc = pq(text)
        title = doc("div#img-content > h1#activity-name").text().strip()
        source = doc("div#img-content div#meta_content a#js_name").text().strip()
        content = doc("div#img-content div#js_content").text().strip().replace("\n", " ").replace("\r", " ")

        page_record["title"] = title
        page_record["page_source"] = source
        page_record["content"] = content
        return page_record

    @staticmethod
    def download_attachments(text, attachment_path):
        doc = pq(text)
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


if __name__ == '__main__':
    logger.info(f"北京财政公众号")
    config = BJCZArticleSpider.__config_init__()
    config.get("subscription").update({
        "biz": "MzI5NTU3NzgyMg=="
    })
    spider = BJCZArticleSpider(**config.get("subscription"))
    spider.start_request()
