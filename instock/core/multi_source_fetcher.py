#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import random
import logging
import json
from functools import lru_cache

__author__ = 'myh '
__date__ = '2025/12/31 '

logger = logging.getLogger(__name__)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
]

EASTMONEY_DOMAINS = [
    'eastmoney.com',
    'push2.eastmoney.com',
    'push2his.eastmoney.com',
    'datacenter-web.eastmoney.com',
    'data.eastmoney.com',
    'quote.eastmoney.com',
    'emweb.eastmoney.com',
]

SINA_DOMAINS = [
    'sina.com.cn',
    'sinajs.cn',
    'finance.sina.com.cn',
    'vip.stock.finance.sina.com.cn',
    'quotes.sina.cn',
]

class DataSource:
    EASTMONEY = 'eastmoney'
    SINA = 'sina'
    AUTO = 'auto'

class MultiSourceFetcher:
    """
    多数据源获取器
    支持东方财富网和新浪财经数据源
    自动切换和故障转移
    """
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if MultiSourceFetcher._initialized:
            return
        MultiSourceFetcher._initialized = True
        
        self.base_dir = os.path.dirname(os.path.dirname(__file__))
        self._last_request_time = 0
        self._min_request_interval = 0.3
        self._current_source = DataSource.AUTO
        self._source_status = {
            DataSource.EASTMONEY: {'available': True, 'last_check': 0, 'fail_count': 0},
            DataSource.SINA: {'available': True, 'last_check': 0, 'fail_count': 0}
        }
        self._check_interval = 300
        self._max_fail_count = 3
        
        self.sina_session = self._create_sina_session()
        self.eastmoney_session = self._create_eastmoney_session()
    
    def _create_sina_session(self):
        """创建新浪财经会话"""
        session = requests.Session()
        retry_strategy = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=50, pool_maxsize=50)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': 'https://vip.stock.finance.sina.com.cn/',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        })
        return session
    
    def _create_eastmoney_session(self):
        """创建东方财富网会话"""
        session = requests.Session()
        retry_strategy = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=50, pool_maxsize=50)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': 'https://quote.eastmoney.com/',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
        })
        return session
    
    def _get_url_domain(self, url):
        """获取URL的域名"""
        url_lower = url.lower()
        for domain in EASTMONEY_DOMAINS + SINA_DOMAINS:
            if domain in url_lower:
                return domain
        return None
    
    def _is_eastmoney_url(self, url):
        """检查是否是东方财富的URL"""
        url_lower = url.lower()
        return any(domain in url_lower for domain in EASTMONEY_DOMAINS)
    
    def _is_sina_url(self, url):
        """检查是否是新浪的URL"""
        url_lower = url.lower()
        return any(domain in url_lower for domain in SINA_DOMAINS)
    
    def _wait_for_rate_limit(self):
        """请求频率控制"""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        if time_since_last < self._min_request_interval:
            sleep_time = self._min_request_interval - time_since_last
            sleep_time += random.uniform(0, 0.2)
            time.sleep(sleep_time)
        self._last_request_time = time.time()
    
    def _update_source_status(self, source, success):
        """更新数据源状态"""
        status = self._source_status[source]
        if success:
            status['fail_count'] = 0
            status['available'] = True
        else:
            status['fail_count'] += 1
            if status['fail_count'] >= self._max_fail_count:
                status['available'] = False
                logger.warning(f"数据源 {source} 已标记为不可用")
        status['last_check'] = time.time()
    
    def _is_source_available(self, source):
        """检查数据源是否可用"""
        status = self._source_status[source]
        if not status['available']:
            if time.time() - status['last_check'] > self._check_interval:
                status['available'] = True
                status['fail_count'] = 0
                logger.info(f"重新尝试数据源 {source}")
        return status['available']
    
    def get_available_source(self):
        """获取当前可用的数据源"""
        if self._current_source != DataSource.AUTO:
            if self._is_source_available(self._current_source):
                return self._current_source
        
        if self._is_source_available(DataSource.SINA):
            return DataSource.SINA
        if self._is_source_available(DataSource.EASTMONEY):
            return DataSource.EASTMONEY
        
        return DataSource.SINA
    
    def make_request(self, url, params=None, source=None, retry=3, timeout=15):
        """
        发送请求，支持多数据源
        注意：不同数据源的API URL不同，不会自动转换URL
        """
        if source is None:
            source = self.get_available_source()
        
        if self._is_eastmoney_url(url) and source == DataSource.SINA:
            if self._is_source_available(DataSource.EASTMONEY):
                source = DataSource.EASTMONEY
            else:
                logger.warning(f"东方财富API不可用，无法通过新浪数据源访问: {url[:50]}...")
                raise requests.exceptions.RequestException(f"东方财富数据源不可用")
        
        if self._is_sina_url(url) and source == DataSource.EASTMONEY:
            if self._is_source_available(DataSource.SINA):
                source = DataSource.SINA
            else:
                logger.warning(f"新浪API不可用，无法通过东方财富数据源访问: {url[:50]}...")
                raise requests.exceptions.RequestException(f"新浪数据源不可用")
        
        self._wait_for_rate_limit()
        
        session = self.sina_session if source == DataSource.SINA else self.eastmoney_session
        
        for attempt in range(retry):
            try:
                session.headers['User-Agent'] = random.choice(USER_AGENTS)
                
                if source == DataSource.SINA and self._is_sina_url(url):
                    pass
                elif url.startswith('http://'):
                    url = 'https://' + url[7:]
                
                logger.info(f"正在请求 [{source}]: {url[:80]}...")
                response = session.get(url, params=params, timeout=timeout, verify=True)
                
                if response.status_code == 403:
                    logger.warning(f"{source} 请求被拒绝(403)")
                    self._update_source_status(source, False)
                    if attempt < retry - 1:
                        time.sleep(random.uniform(2, 4))
                        continue
                
                response.raise_for_status()
                self._update_source_status(source, True)
                return response
                
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"{source} 连接错误: {str(e)[:100]}")
                self._update_source_status(source, False)
                if attempt < retry - 1:
                    time.sleep(random.uniform(1, 3))
                    continue
                    
            except requests.exceptions.Timeout as e:
                logger.warning(f"{source} 超时: {e}")
                if attempt < retry - 1:
                    time.sleep(random.uniform(1, 2))
                    continue
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"{source} 请求错误: {str(e)[:100]}")
                if attempt < retry - 1:
                    time.sleep(random.uniform(1, 2))
                    continue
        
        raise requests.exceptions.RequestException(f"数据源 {source} 请求失败")

multi_fetcher = MultiSourceFetcher()

class sina_data_fetcher:
    """新浪财经数据获取器"""
    
    @staticmethod
    def get_stock_list(page=1, page_size=500):
        """获取A股股票列表"""
        url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
        params = {
            "page": page,
            "num": page_size,
            "sort": "symbol",
            "asc": 1,
            "node": "hs_a",
            "symbol": "",
            "_s_r_a": "page"
        }
        r = multi_fetcher.make_request(url, params=params, source=DataSource.SINA)
        return r.json()
    
    @staticmethod
    def get_stock_realtime(codes):
        """获取股票实时行情"""
        if isinstance(codes, list):
            codes = ','.join(codes)
        
        url = f"http://hq.sinajs.cn/list={codes}"
        try:
            r = multi_fetcher.make_request(url, source=DataSource.SINA, timeout=10)
            return r.text
        except:
            return ""
    
    @staticmethod
    def get_kline_data(symbol, scale=240, datalen=365):
        """获取K线数据
        symbol: 股票代码，如 sh600000
        scale: 周期 240=日线, 60=60分钟, 30=30分钟, 15=15分钟, 5=5分钟
        datalen: 数据长度
        """
        url = "https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
        params = {
            "symbol": symbol,
            "scale": scale,
            "ma": "no",
            "datalen": datalen
        }
        r = multi_fetcher.make_request(url, params=params, source=DataSource.SINA)
        return r.json()
    
    @staticmethod
    def get_stock_info(code):
        """获取个股信息"""
        if code.startswith('6'):
            full_code = f'sh{code}'
        else:
            full_code = f'sz{code}'
        
        url = f"http://hq.sinajs.cn/list={full_code}"
        r = multi_fetcher.make_request(url, source=DataSource.SINA, timeout=10)
        return r.text

def get_fetcher():
    """获取多数据源fetcher实例"""
    return multi_fetcher
