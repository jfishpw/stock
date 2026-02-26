#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import logging
import os.path
import sys
from abc import ABC

import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.options
from tornado import gen
import hashlib

# 在项目运行时，临时将项目路径添加到环境变量
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
log_path = os.path.join(cpath_current, 'log')
if not os.path.exists(log_path):
    os.makedirs(log_path)
logging.basicConfig(format='%(asctime)s %(message)s', filename=os.path.join(log_path, 'stock_web.log'))
logging.getLogger().setLevel(logging.ERROR)
import instock.lib.torndb as torndb
import instock.lib.database as mdb
import instock.lib.version as version
import instock.web.dataTableHandler as dataTableHandler
import instock.web.dataIndicatorsHandler as dataIndicatorsHandler
import instock.web.base as webBase
import instock.web.auth_handler as auth_handler

__author__ = 'myh '
__date__ = '2023/3/10 '


def check_auth(handler):
    """检查是否已认证"""
    from instock.lib.web_password import is_password_set, verify_password
    
    # 如果没有设置密码，跳转到设置密码页面
    if not is_password_set():
        return False
    
    # 检查cookie中的认证信息
    auth_cookie = handler.get_cookie('web_auth')
    if auth_cookie:
        expected = hashlib.sha256('authenticated'.encode()).hexdigest()[:16]
        if auth_cookie == expected:
            return True
    
    return False


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            # 登录相关
            (r"/instock/api/login", auth_handler.LoginHandler),
            (r"/instock/login", auth_handler.LoginHandler),
            # 设置路由
            (r"/", HomeHandler),
            (r"/instock/", HomeHandler),
            # 使用datatable 展示报表数据模块。
            (r"/instock/api_data", dataTableHandler.GetStockDataHandler),
            (r"/instock/data", dataTableHandler.GetStockHtmlHandler),
            # 获得股票指标数据。
            (r"/instock/data/indicators", dataIndicatorsHandler.GetDataIndicatorsHandler),
            # 加入关注
            (r"/instock/control/attention", dataIndicatorsHandler.SaveCollectHandler),
        ]
        settings = dict(  # 配置
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            xsrf_cookies=False,  # True,
            # cookie加密
            cookie_secret="027bb1b670eddf0392cdda8709268a17b58b7",
            debug=True,
        )
        super(Application, self).__init__(handlers, **settings)
        # Have one global connection to the blog DB across all handlers
        try:
            self.db = torndb.Connection(**mdb.MYSQL_CONN_TORNDB)
            print(f"数据库连接成功: {mdb.MYSQL_CONN_TORNDB['host']}/{mdb.MYSQL_CONN_TORNDB['database']}")
        except Exception as e:
            logging.error(f"数据库连接失败: {e}")
            print(f"数据库连接失败: {e}")
            raise


# 首页handler。
class HomeHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        # 检查是否需要认证
        if not check_auth(self):
            # 未认证，重定向到登录页面
            self.redirect("/instock/login")
            return
        
        self.render("index.html",
                    stockVersion=version.__version__,
                    leftMenu=webBase.GetLeftMenu(self.request.uri))


def main():
    # tornado.options.parse_command_line()
    tornado.options.options.logging = None

    http_server = tornado.httpserver.HTTPServer(Application())
    port = 9988
    http_server.listen(port)

    print(f"服务已启动，web地址 : http://localhost:{port}/")
    logging.error(f"服务已启动，web地址 : http://localhost:{port}/")

    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
