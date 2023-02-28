#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
sys.path.append(".")
import re
import traceback
import fire
import dateparser
from pathlib import Path
from urllib.parse import urljoin
from datetime import datetime
from loguru import logger
from playwright.sync_api import sync_playwright
from spider_base import SpiderBase
from utils.settings import DATETIME_REGEXES
from utils.tools import convert_to_relative_path, duplicate_filter, gen_invalid_record, downloader, save_page


class ZFGBSpider(SpiderBase):
    def __init__(self, playwright, headless=None):
        super().__init__()
        """日志、浏览器、网站栏目配置"""
        self.playwright = playwright
        self.headless = headless
        self.browser = self.playwright.firefox.launch(
            headless=self.headless, firefox_user_prefs={'pdfjs.disabled': True, 'Content-Disposition': 'attachment'}
        )
        self.context = self.browser.new_context(locale='zh-CN', viewport={'width': 1920, 'height': 1080},
                                                accept_downloads=True)
        self.context.set_default_timeout(120 * 1000)
        self.province = '北京'
        self.city = '北京'
        self.site = '北京市人民政府公报'
        self.origin = 'http://www.beijing.gov.cn/zhengce/gongbao'
        self.categories = '政府公报'
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
        """开始爬取"""

        category = self.categories
        category_path = self.output_path.joinpath(self.site, category)
        if not category_path.exists():
            category_path.mkdir(parents=True)
        index_url = self.origin

        try:
            current_url = index_url
            page = self.context.new_page()
            page.goto(current_url)
            # page.wait_for_timeout(2000)
            page_handle = page.query_selector_all("div.qzb div.changepage > a")
            page_total = int(page_handle[-2].get_attribute("data-page").strip())
            logger.info(f"page_total: {page_total}")
            page_records = []
            while page_total:
                page_total -= 1
                page.wait_for_selector('ul#listcontent')
                links = page.query_selector_all('ul#listcontent > li > a')

                logger.info(f"links: {links}, total: {len(links)}")
                # page.pause()
                if not links:
                    logger.error(f'网页无法提取链接, {current_url}')
                    return
                for link in links:
                    href = link.get_attribute('href')
                    detail_page_url = href
                    if duplicate_filter(self.db, self.page_table, detail_page_url):
                        continue
                    if Path(href).suffix != '.html':
                        page_record = gen_invalid_record(self.page_record, category, detail_page_url)
                        page_records.append(page_record)
                        continue
                    else:
                        logger.info(detail_page_url)

                    for _ in range(3):
                        try:
                            with self.context.expect_page() as detail_page:
                                link.click()
                        except Exception as e:
                            logger.warning(traceback.format_exc())
                            continue
                        else:
                            break
                    detail_page.value.wait_for_timeout(1000)
                    if detail_page.value.query_selector('div.leftbox') is None:
                        detail_page.value.close()
                        page_record = gen_invalid_record(self.page_record, category, detail_page_url)
                        page_records.append(page_record)
                        continue

                    basename = Path(href).stem
                    file_path = category_path.joinpath(basename)
                    if not file_path.exists():
                        file_path.mkdir()
                    record_path = file_path.joinpath('record')
                    if not record_path.exists():
                        record_path.mkdir()
                    attachment_path = file_path.joinpath('attachment')
                    if not attachment_path.exists():
                        attachment_path.mkdir()

                    with open(record_path.joinpath(f'{basename}.html'), 'w', encoding='utf-8-sig') as f:
                        f.write(detail_page.value.content())
                    try:
                        detail_page.value.screenshot(path=record_path.joinpath(f'{basename}.png'), full_page=True)
                    except Exception as e:
                        logger.warning(traceback.format_exc())
                    page_record = self.parse_page(detail_page.value, category)
                    attachment_name = self.download_attachments(detail_page.value, attachment_path)
                    # page_record['record_path'] = str(record_path)
                    page_record['record_path'] = convert_to_relative_path(record_path)
                    if attachment_name:
                        page_record['attachment_name'] = attachment_name
                        # page_record['attachment_path'] = str(attachment_path)
                        page_record['attachment_path'] = convert_to_relative_path(attachment_path)
                    detail_page.value.close()
                    page_records.append(page_record)
                    # logger.info(f"{page_record}")

                if not page.query_selector('div.qzb div.changepage > a.next'):
                    break
                _next = page.query_selector('div.qzb div.changepage > a.next')
                _next.click()

            page.close()
            for opened_page in self.context.pages:
                opened_page.close()

            if page_records:
                save_page(self.db, self.page_table, page_records)
        except Exception as exc:
            logger.error(traceback.format_exc())
        self.browser.close()

    def parse_page(self, page, category):
        """在页面加载完成, 解析页面, 返回json object"""
        # page.wait_for_selector('//div[@class="fmaincon_R fmaincon_R01"]')
        # page.wait_for_timeout(100)
        page_record = self.page_record.copy()
        page_record['category'] = category
        page_record['page_url'] = page.url
        page_record['created_time'] = datetime.now()
        page_record = {
            'province': self.province,
            'city': self.city,
            'site': self.site,
            'category': category,
            'page_url': page.url,
            'page_release_date': None,
            'page_source': None,
            'title': None,
            'content': None,
            'attachment_name': None,
            'record_path': None,
            'attachment_path': None,
            'created_time': datetime.now()
        }
        doc_info = page.query_selector_all('ol.doc-info li')
        for li in doc_info:
            if li.text_content().strip().startswith("[发布日期]"):
                for regex in DATETIME_REGEXES:
                    result = re.search(regex, li.query_selector("span").text_content().strip().replace('\xa0', ' '))
                    if result:
                        page_record['page_release_date'] = dateparser.parse(result.group(1))
                        break
            if li.text_content().strip().startswith("[发文机构]"):
                page_record['page_source'] = li.query_selector("span").text_content().strip().replace('\xa0', ' ')
        page_record['title'] = page.query_selector("div.header h1 p").text_content().strip().replace('\xa0', ' ')

        contents = []

        elements = page.query_selector_all('div#mainText div.view p')
        for element in elements:
            if not element.is_visible():
                continue
            if not element.text_content().strip():
                continue
            text_content = element.text_content().strip().replace('\xa0', ' ')
            contents.append(text_content)
        if contents:
            page_record['content'] = '\n'.join(contents)

        return page_record

    @staticmethod
    def download_attachments(page, save_path):
        """等待面渲染完成,  查找并下载页面附件"""
        attachment_name = []
        attachments = page.query_selector_all('div.mainTextBox ul.fujian li')
        for attachment in attachments:
            link = attachment.query_selector('a').get_attribute('href')
            if link is None or attachment.text_content().strip() == '':
                continue
            attachment_url = urljoin(page.url, link)
            extension = Path(link).suffix.lower()
            if extension in ['.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.pdf', '.jpg', '.jpeg', '.png',
                             '.webp', '.bmp', '.zip', '.rar', '.7z']:
                if attachment.text_content().strip().lower().endswith(extension):
                    filename = attachment.text_content().strip()
                else:
                    filename = attachment.text_content().strip() + extension
                download_path = save_path.joinpath(filename)
                # noinspection PyBroadException
                try:
                    downloader(attachment_url, download_path)
                except Exception:
                    pass
                else:
                    attachment_name.append(filename)
        return ','.join(attachment_name)


if __name__ == '__main__':
    logger.info("政府公报")
    with sync_playwright() as p:
        spider = ZFGBSpider(p, headless=True)
        fire.Fire(spider.start_request)
