#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from abc import ABC
import tornado.web
import instock.core.singleton_stock_web_module_data as sswmd

__author__ = 'myh '
__date__ = '2023/3/10 '


# 基础handler，主要负责检查mysql的数据库链接。
class BaseHandler(tornado.web.RequestHandler, ABC):
    @property
    def db(self):
        db_conn = self.application.db
        if db_conn is None:
            raise Exception("数据库连接未初始化")
        try:
            # check every time。
            db_conn.query("SELECT 1 ")
        except Exception as e:
            print(f"数据库连接检查失败，尝试重连: {e}")
            db_conn.reconnect()
        return db_conn


class LeftMenu:
    def __init__(self, url):
        self.leftMenuList = sswmd.stock_web_module_data().get_data_list()
        self.current_url = url


# 获得左菜单。
def GetLeftMenu(url):
    return LeftMenu(url)
