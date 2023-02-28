#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from sqlalchemy import create_engine, MetaData
from utils.settings import DB_ENGINE, DB_HOST, DB_PORT, DB_USERNAME, DB_PASSWORD, DB_NAME


class Database(object):
    def __init__(self):
        _database_url = f'{DB_ENGINE}://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        self.engine = create_engine(_database_url, future=True)
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine, schema='public')
