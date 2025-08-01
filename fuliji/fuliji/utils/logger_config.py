#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""
日志配置工具模块
提供统一的日志配置功能，避免在每个爬虫和pipeline中重复配置
"""
import logging
import os
import time
from scrapy.utils.project import get_project_settings


class SpiderLoggerMixin:
    """
    爬虫日志混入类
    为爬虫提供统一的日志配置功能
    """
    
    def setup_spider_logger(self):
        """
        设置爬虫专属的文件日志记录器
        """
        settings = get_project_settings()
        # 构造该爬虫专属的日志文件路径
        log_dir = settings.get('LOG_DIR', 'logs')
        spider_log_file = os.path.join(log_dir, f'{self.name}_{time.strftime("%Y%m%d")}.log')
        
        # 确保日志目录存在
        os.makedirs(os.path.dirname(spider_log_file), exist_ok=True)
        
        # 创建一个独立的文件日志记录器
        file_logger = logging.getLogger(f'{self.name}_file_logger')
        file_logger.propagate = False  # 避免日志重复
        
        # 设置日志级别
        log_level = settings.get('LOG_LEVEL', 'DEBUG')
        if isinstance(log_level, str):
            log_level = getattr(logging, log_level)
        file_logger.setLevel(log_level)
        
        # 清除之前的处理器（避免重复添加）
        file_logger.handlers.clear()
        
        # 添加文件处理器
        handler = logging.FileHandler(spider_log_file, encoding='utf-8')
        formatter = logging.Formatter(settings.get('LOG_FORMAT', '%(asctime)s [%(name)s] %(levelname)s: %(message)s'))
        handler.setFormatter(formatter)
        file_logger.addHandler(handler)
        
        # 保存文件日志记录器供后续使用
        self.file_logger = file_logger
        
        return file_logger
    
    def log(self, message, level=logging.DEBUG, **kwargs):
        """
        重写log方法，同时将日志发送到Scrapy和文件
        
        Args:
            message: 日志消息
            level: 日志级别
            **kwargs: 其他参数
        """
        # 使用Scrapy的原生logger记录日志
        super().log(message, level, **kwargs)
        
        # 同时使用文件日志记录器
        if hasattr(self, 'file_logger'):
            self.file_logger.log(level, message)


class PipelineLoggerMixin:
    """
    管道日志混入类
    为pipeline提供统一的日志配置功能
    """
    
    def setup_pipeline_logger(self, name):
        """
        设置管道专属的文件日志记录器
        
        Args:
            name: 管道名称，用于生成日志文件名
        """
        settings = get_project_settings()
        
        # 构造管道专属的日志文件路径
        log_dir = settings.get('LOG_DIR', 'logs')
        pipeline_log_file = os.path.join(log_dir, f'pipeline_{name}_{time.strftime("%Y%m%d")}.log')
        
        # 确保日志目录存在
        os.makedirs(os.path.dirname(pipeline_log_file), exist_ok=True)
        
        # 创建一个独立的文件日志记录器
        file_logger = logging.getLogger(f'pipeline_{name}_file_logger')
        file_logger.propagate = False  # 避免日志重复
        
        # 设置日志级别
        log_level = settings.get('LOG_LEVEL', 'DEBUG')
        if isinstance(log_level, str):
            log_level = getattr(logging, log_level)
        file_logger.setLevel(log_level)
        
        # 清除之前的处理器（避免重复添加）
        file_logger.handlers.clear()
        
        # 添加文件处理器
        handler = logging.FileHandler(pipeline_log_file, encoding='utf-8')
        formatter = logging.Formatter(settings.get('LOG_FORMAT', '%(asctime)s [%(name)s] %(levelname)s: %(message)s'))
        handler.setFormatter(formatter)
        file_logger.addHandler(handler)
        
        # 保存文件日志记录器供后续使用
        self.file_logger = file_logger
        
        return file_logger
    
    def log(self, message, level=logging.INFO):
        """
        记录日志到文件和标准logging
        
        Args:
            message: 日志消息
            level: 日志级别
        """
        # 使用标准logging记录日志
        logging.log(level, message)
        
        # 同时使用文件日志记录器
        if hasattr(self, 'file_logger'):
            self.file_logger.log(level, message)


def setup_spider_file_logger(spider_name):
    """
    为指定的爬虫创建文件日志记录器
    
    Args:
        spider_name: 爬虫名称
        
    Returns:
        logging.Logger: 配置好的文件日志记录器
    """
    settings = get_project_settings()
    
    # 构造日志文件路径
    log_dir = settings.get('LOG_DIR', 'logs')
    spider_log_file = os.path.join(log_dir, f'{spider_name}_{time.strftime("%Y%m%d")}.log')
    
    # 确保日志目录存在
    os.makedirs(os.path.dirname(spider_log_file), exist_ok=True)
    
    # 创建文件日志记录器
    file_logger = logging.getLogger(f'{spider_name}_file_logger')
    file_logger.propagate = False
    
    # 设置日志级别
    log_level = settings.get('LOG_LEVEL', 'DEBUG')
    if isinstance(log_level, str):
        log_level = getattr(logging, log_level)
    file_logger.setLevel(log_level)
    
    # 清除之前的处理器
    file_logger.handlers.clear()
    
    # 添加文件处理器
    handler = logging.FileHandler(spider_log_file, encoding='utf-8')
    formatter = logging.Formatter(settings.get('LOG_FORMAT', '%(asctime)s [%(name)s] %(levelname)s: %(message)s'))
    handler.setFormatter(formatter)
    file_logger.addHandler(handler)
    
    return file_logger


def setup_pipeline_file_logger(pipeline_name):
    """
    为指定的pipeline创建文件日志记录器
    
    Args:
        pipeline_name: pipeline名称
        
    Returns:
        logging.Logger: 配置好的文件日志记录器
    """
    settings = get_project_settings()
    
    # 构造日志文件路径
    log_dir = settings.get('LOG_DIR', 'logs')
    pipeline_log_file = os.path.join(log_dir, f'pipeline_{pipeline_name}_{time.strftime("%Y%m%d")}.log')
    
    # 确保日志目录存在
    os.makedirs(os.path.dirname(pipeline_log_file), exist_ok=True)
    
    # 创建文件日志记录器
    file_logger = logging.getLogger(f'pipeline_{pipeline_name}_file_logger')
    file_logger.propagate = False
    
    # 设置日志级别
    log_level = settings.get('LOG_LEVEL', 'DEBUG')
    if isinstance(log_level, str):
        log_level = getattr(logging, log_level)
    file_logger.setLevel(log_level)
    
    # 清除之前的处理器
    file_logger.handlers.clear()
    
    # 添加文件处理器
    handler = logging.FileHandler(pipeline_log_file, encoding='utf-8')
    formatter = logging.Formatter(settings.get('LOG_FORMAT', '%(asctime)s [%(name)s] %(levelname)s: %(message)s'))
    handler.setFormatter(formatter)
    file_logger.addHandler(handler)
    
    return file_logger


def get_spider_logger_info(spider_name):
    """
    获取爬虫日志相关信息
    
    Args:
        spider_name: 爬虫名称
        
    Returns:
        dict: 包含日志文件路径等信息的字典
    """
    settings = get_project_settings()
    log_dir = settings.get('LOG_DIR', 'logs')
    spider_log_file = os.path.join(log_dir, f'{spider_name}_{time.strftime("%Y%m%d")}.log')
    
    return {
        'log_file': spider_log_file,
        'log_dir': log_dir,
        'log_level': settings.get('LOG_LEVEL', 'DEBUG'),
        'log_format': settings.get('LOG_FORMAT', '%(asctime)s [%(name)s] %(levelname)s: %(message)s')
    }


def get_pipeline_logger_info(pipeline_name):
    """
    获取pipeline日志相关信息
    
    Args:
        pipeline_name: pipeline名称
        
    Returns:
        dict: 包含日志文件路径等信息的字典
    """
    settings = get_project_settings()
    log_dir = settings.get('LOG_DIR', 'logs')
    pipeline_log_file = os.path.join(log_dir, f'pipeline_{pipeline_name}_{time.strftime("%Y%m%d")}.log')
    
    return {
        'log_file': pipeline_log_file,
        'log_dir': log_dir,
        'log_level': settings.get('LOG_LEVEL', 'DEBUG'),
        'log_format': settings.get('LOG_FORMAT', '%(asctime)s [%(name)s] %(levelname)s: %(message)s')
    }
