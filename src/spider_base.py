import requests
import urllib3
from pathlib import Path
from loguru import logger
from utils.settings import FILE_SAVE_PATH
from utils.database import Database
from utils.tools import load_yaml

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class SpiderBase:
    def __init__(self):
        """日志、数据库、文件路径配置"""
        self.db = Database()
        self.page_table = self.db.metadata.tables['public.data']
        # self.page_table = self.db.metadata.tables['public.data-new']
        if FILE_SAVE_PATH:
            self.output_path = Path(FILE_SAVE_PATH) / 'output'
        else:
            self.output_path = Path(__file__).absolute().parent / 'output'

        self.log_path = Path(__file__).absolute().parent / 'log'
        if not self.log_path.exists():
            self.log_path.mkdir()
        logger.add(self.log_path / f'{self.__class__.__name__}.log')

    def start_request(self):
        raise NotImplementedError


class SessionBase(SpiderBase):
    """
    base type: [1] 公众号, [2] https://www.jzl.com (第三方)
    """
    def __init__(self, biz: str = None, base_type: int = None, **kwargs):
        super(SessionBase, self).__init__()
        self.session = requests.session()
        self.link_session = requests.session()
        self.biz = biz
        self.base_type = base_type
        if base_type == 1 and kwargs.get("cookie") and kwargs.get("token"):
            self.__appmsg_init__(**kwargs)
        elif base_type == 2 and kwargs.get("key") and kwargs.get("secret"):
            self.__thirdpart_init(**kwargs)
        else:
            raise RuntimeError(f"require necessary attr")
        # self.__config_init__()

    @classmethod
    def __config_init__(cls):
        cls.config = load_yaml()
        return cls.config

    def __appmsg_init__(self, cookie: str = None, token: object = None, **kwargs):
        """
        公众号账号获取公众号列表
        :return:
        """
        self.url = "https://mp.weixin.qq.com/cgi-bin/appmsg"
        self.headers = {
            "Cookie": cookie,
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        self.params = {
            "action": "list_ex",
            "begin": 0,
            "count": 5,
            "fakeid": self.biz,
            "type": 9,
            "query": "",
            "token": token,
            "lang": "zh_CN",
            "f": "json",
            "ajax": "1"
        }

    def __thirdpart_init(self, key: str = None, secret: str = None, **kwargs):
        """
        第三方账号获取公众号列表
        :return:
        """
        self.url = "https://www.jzl.com/fbmain/monitor/v3/post_condition"
        self.headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        self.params = {
            'biz': self.biz,
            'key': key,
            'verifycode': secret
        }

    def start_request(self):
        raise NotImplementedError

    def get_article(self):
        """
        获取文章列表
        :return:
        """
        logger.info(f"Source: {self.url}")
        resp = self.session.get(self.url, headers=self.headers, params=self.params, verify=False, timeout=5)
        resp.raise_for_status()
        content = resp.json()
        if self.base_type == 1 and content.get('base_resp').get('ret') == 0:
            return self._articles(content)
        elif self.base_type == 2 and content.get('code') == 0:
            return self._articles(content)
        logger.error(f"[{resp.url}] get articles failed: {resp.text}")
        raise RuntimeError(f"[{self.site}] - failed, go pass")

    def _articles(self, content):
        """
        format result
        :param content:
        :return:
        """
        if not content:
            raise RuntimeError(f"articles is Null [{content}]")
        articles = list()
        if self.base_type == 1:
            for article in content.get("app_msg_list", []):
                articles.append({
                    "link": article.get("link"),
                    "title": article.get("title"),
                    "create_time": article.get("create_time")
                })
        elif self.base_type == 2:
            for article in content.get("data", []):
                articles.append({
                    "link": article.get("url"),
                    "title": article.get("title"),
                    "create_time": article.get("post_time")
                })
        return articles


