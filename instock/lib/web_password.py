#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
密码管理模块
"""

import os
import hashlib
import json

PASSWORD_FILE = '/data/InStock/instock/config/web_password.json'

def get_password_file():
    """获取密码文件路径"""
    config_dir = os.path.dirname(PASSWORD_FILE)
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
    return PASSWORD_FILE

def get_stored_password():
    """获取存储的密码"""
    pwd_file = get_password_file()
    if os.path.exists(pwd_file):
        try:
            with open(pwd_file, 'r') as f:
                data = json.load(f)
                return data.get('password')
        except:
            return None
    return None

def set_password(password):
    """设置密码"""
    if not password:
        return False
    
    # 使用SHA256哈希存储密码
    hashed = hashlib.sha256(password.encode()).hexdigest()
    
    pwd_file = get_password_file()
    try:
        with open(pwd_file, 'w') as f:
            json.dump({'password': hashed}, f)
        return True
    except:
        return False

def verify_password(password):
    """验证密码"""
    stored = get_stored_password()
    if not stored:
        return False
    
    hashed = hashlib.sha256(password.encode()).hexdigest()
    return hashed == stored

def is_password_set():
    """检查是否已设置密码"""
    return get_stored_password() is not None
