#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
import time
import random
import logging
from instock.core.singleton_proxy import proxys

__author__ = 'myh '
__date__ = '2025/12/31 '

logger = logging.getLogger(__name__)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
]

class eastmoney_fetcher:
    """
    东方财富网数据获取器
    封装了Cookie管理、会话管理和请求发送功能
    增强反反爬机制，支持动态请求头、请求间隔控制等
    """

    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if eastmoney_fetcher._initialized:
            return
        eastmoney_fetcher._initialized = True
        
        self.base_dir = os.path.dirname(os.path.dirname(__file__))
        self.proxies = proxys().get_proxies()
        self.session = self._create_session()
        self._last_request_time = 0
        self._min_request_interval = 0.5
        self._request_count = 0
        self._max_requests_per_session = 500
        self._cookie_refresh_time = 0
        self._cookie_max_age = 3600

    def _get_cookie(self):
        """
        获取东方财富网的Cookie
        优先级：环境变量 > 文件 > 默认Cookie
        """
        cookie = os.environ.get('EAST_MONEY_COOKIE')
        if cookie:
            logger.info(f"从环境变量获取Cookie成功，长度: {len(cookie)}")
            return cookie

        cookie_file = Path(os.path.join(self.base_dir, 'config', 'eastmoney_cookie.txt'))
        if cookie_file.exists():
            try:
                with open(cookie_file, 'r', encoding='utf-8') as f:
                    cookie = f.read().strip()
                if cookie:
                    logger.info(f"从文件获取Cookie成功，长度: {len(cookie)}")
                    return cookie
                else:
                    logger.warning("Cookie文件为空")
            except Exception as e:
                logger.warning(f"读取Cookie文件失败: {e}")
        else:
            logger.info("Cookie文件不存在，跳过Cookie配置")

        return None

    def _parse_cookie_string(self, cookie_string):
        """
        解析Cookie字符串为字典
        """
        cookies = {}
        if not cookie_string:
            return cookies
        
        for item in cookie_string.split(';'):
            item = item.strip()
            if '=' in item:
                name, value = item.split('=', 1)
                cookies[name.strip()] = value.strip()
        return cookies

    def _create_session(self):
        """创建并配置会话，增强反反爬能力"""
        session = requests.Session()

        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "OPTIONS"]
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=100,
            pool_maxsize=100
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        self._update_session_headers(session)
        
        cookie_string = self._get_cookie()
        if cookie_string:
            cookies = self._parse_cookie_string(cookie_string)
            cookie_count = len(cookies)
            for name, value in cookies.items():
                session.cookies.set(name, value)
            logger.info(f"已设置 {cookie_count} 个Cookie到会话中")
        else:
            logger.info("未配置Cookie，将使用无Cookie模式访问")
        
        return session

    def _update_session_headers(self, session, url=None):
        """
        更新会话请求头，模拟真实浏览器
        """
        user_agent = random.choice(USER_AGENTS)
        
        headers = {
            'User-Agent': user_agent,
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
        }
        
        if url:
            if 'eastmoney.com' in url:
                headers['Referer'] = 'https://quote.eastmoney.com/'
                headers['Origin'] = 'https://quote.eastmoney.com'
            elif 'data.eastmoney.com' in url:
                headers['Referer'] = 'https://data.eastmoney.com/'
                headers['Origin'] = 'https://data.eastmoney.com'
        else:
            headers['Referer'] = 'https://quote.eastmoney.com/'
        
        session.headers.update(headers)

    def _wait_for_rate_limit(self):
        """
        请求频率控制，避免请求过快
        """
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        
        if time_since_last < self._min_request_interval:
            sleep_time = self._min_request_interval - time_since_last
            sleep_time += random.uniform(0, 0.5)
            time.sleep(sleep_time)
        
        self._last_request_time = time.time()

    def _check_session_health(self):
        """
        检查会话健康状态，必要时重建
        """
        self._request_count += 1
        
        if self._request_count >= self._max_requests_per_session:
            logger.info("达到最大请求数，重建会话...")
            self._rebuild_session()
        
        current_time = time.time()
        if current_time - self._cookie_refresh_time > self._cookie_max_age:
            logger.info("Cookie过期，刷新会话...")
            self._rebuild_session()

    def _rebuild_session(self):
        """
        重建会话
        """
        try:
            self.session.close()
        except:
            pass
        
        self.session = self._create_session()
        self._request_count = 0
        self._cookie_refresh_time = time.time()

    def _convert_http_to_https(self, url):
        """
        将HTTP URL转换为HTTPS
        """
        if url.startswith('http://'):
            url = 'https://' + url[7:]
        return url

    def make_request(self, url, params=None, retry=3, timeout=15):
        """
        发送请求，增强错误处理和重试机制
        :param url: 请求URL
        :param params: 请求参数
        :param retry: 重试次数
        :param timeout: 超时时间
        :return: 响应对象
        """
        url = self._convert_http_to_https(url)
        
        for attempt in range(retry):
            try:
                self._wait_for_rate_limit()
                self._check_session_health()
                
                self._update_session_headers(self.session, url)
                
                response = self.session.get(
                    url,
                    proxies=self.proxies,
                    params=params,
                    timeout=timeout,
                    verify=True
                )
                
                if response.status_code == 403:
                    logger.warning(f"请求被拒绝(403)，尝试重建会话...")
                    self._rebuild_session()
                    if attempt < retry - 1:
                        time.sleep(random.uniform(2, 5))
                        continue
                
                response.raise_for_status()
                
                content_type = response.headers.get('Content-Type', '')
                if 'json' in content_type or response.text.startswith('{') or response.text.startswith('['):
                    return response
                elif not response.text.strip():
                    logger.warning(f"收到空响应，URL: {url}")
                    if attempt < retry - 1:
                        time.sleep(random.uniform(1, 3))
                        continue
                
                return response
                
            except requests.exceptions.SSLError as e:
                logger.warning(f"SSL错误: {e}, 尝试 {attempt + 1}/{retry}")
                if attempt < retry - 1:
                    time.sleep(random.uniform(2, 5))
                    continue
                raise
                
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"连接错误: {e}, 尝试 {attempt + 1}/{retry}")
                if attempt < retry - 1:
                    time.sleep(random.uniform(2, 5))
                    self._rebuild_session()
                    continue
                raise
                
            except requests.exceptions.Timeout as e:
                logger.warning(f"请求超时: {e}, 尝试 {attempt + 1}/{retry}")
                if attempt < retry - 1:
                    time.sleep(random.uniform(1, 3))
                    continue
                raise
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"请求错误: {e}, 尝试 {attempt + 1}/{retry}")
                if attempt < retry - 1:
                    time.sleep(random.uniform(1, 3))
                    continue
                raise
        
        raise requests.exceptions.RequestException(f"请求失败，已重试 {retry} 次")

    def make_post_request(self, url, data=None, json=None, params=None, retry=3, timeout=30):
        """
        发送POST请求
        :param url: 请求URL
        :param data: 请求数据（表单形式）
        :param json: 请求数据（JSON形式）
        :param params: URL参数
        :param retry: 重试次数
        :param timeout: 超时时间
        :return: 响应对象
        """
        url = self._convert_http_to_https(url)
        
        for attempt in range(retry):
            try:
                self._wait_for_rate_limit()
                self._check_session_health()
                
                self._update_session_headers(self.session, url)
                self.session.headers.update({
                    'Content-Type': 'application/json;charset=UTF-8'
                })
                
                response = self.session.post(
                    url,
                    proxies=self.proxies,
                    params=params,
                    data=data,
                    json=json,
                    timeout=timeout,
                    verify=True
                )
                
                if response.status_code == 403:
                    logger.warning(f"POST请求被拒绝(403)，尝试重建会话...")
                    self._rebuild_session()
                    if attempt < retry - 1:
                        time.sleep(random.uniform(2, 5))
                        continue
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"POST请求错误: {e}, 尝试 {attempt + 1}/{retry}")
                if attempt < retry - 1:
                    time.sleep(random.uniform(1, 3))
                    continue
                raise
        
        raise requests.exceptions.RequestException(f"POST请求失败，已重试 {retry} 次")

    def update_cookie(self, new_cookie):
        """
        更新Cookie
        :param new_cookie: 新的Cookie值
        """
        cookies = self._parse_cookie_string(new_cookie)
        for name, value in cookies.items():
            self.session.cookies.set(name, value)
        self._cookie_refresh_time = time.time()
        logger.info("Cookie已更新")

    def close(self):
        """
        关闭会话
        """
        try:
            self.session.close()
        except:
            pass
