#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
登录验证Handler
"""

import tornado.web
import tornado.escape
from tornado import gen
import hashlib

class LoginHandler(tornado.web.RequestHandler):
    """处理登录请求"""
    
    def set_default_headers(self):
        """设置响应头"""
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
    
    def options(self):
        """处理OPTIONS请求"""
        self.set_status(204)
        self.finish()
    
    @gen.coroutine
    def get(self):
        """返回是否需要设置密码"""
        from instock.lib.web_password import is_password_set
        self.set_header('Content-Type', 'application/json')
        self.write({'need_login': not is_password_set()})
    
    def set_auth_cookie(self):
        """设置认证cookie"""
        auth_value = hashlib.sha256('authenticated'.encode()).hexdigest()[:16]
        self.set_cookie('web_auth', auth_value, expires_days=7)
    
    @gen.coroutine
    def post(self):
        """处理登录请求"""
        import json
        from instock.lib.web_password import set_password, verify_password, is_password_set
        
        try:
            data = json.loads(self.request.body)
            action = data.get('action', 'login')
            
            if action == 'set_password':
                # 首次设置密码
                password = data.get('password', '')
                if len(password) < 4:
                    self.write({'success': False, 'message': '密码长度至少4位'})
                    return
                
                if set_password(password):
                    # 设置密码成功后自动登录
                    self.set_auth_cookie()
                    self.write({'success': True, 'message': '密码设置成功'})
                else:
                    self.write({'success': False, 'message': '密码设置失败'})
            
            elif action == 'login':
                # 登录验证
                if not is_password_set():
                    self.write({'success': False, 'message': '请先设置密码'})
                    return
                
                password = data.get('password', '')
                if verify_password(password):
                    # 登录成功后设置cookie
                    self.set_auth_cookie()
                    self.write({'success': True, 'message': '登录成功'})
                else:
                    self.write({'success': False, 'message': '密码错误'})
            
            else:
                self.write({'success': False, 'message': '未知操作'})
                
        except Exception as e:
            self.write({'success': False, 'message': str(e)})