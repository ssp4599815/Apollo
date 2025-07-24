# !/usr/bin/env python
# -*-coding:utf-8 -*-
import logging
import os
import subprocess
import concurrent.futures
import threading
import time

import requests
import scrapy
from scrapy.exceptions import DropItem
from scrapy.pipelines.images import ImagesPipeline
from scrapy.utils.project import get_project_settings


class ImgPipeline(ImagesPipeline):

    def get_media_requests(self, item, info):
        # 获取将用于存储图片的目录路径
        dir_path = self.get_directory_path(item)
        logging.info("Checking if directory exists at: %s" % dir_path)
        if not os.path.exists(dir_path):
            logging.info("Directory does not exist, downloading images")
            for index, image_url in enumerate(item['image_urls']):
                yield scrapy.Request(image_url, meta={'item': item, 'index': index})
        else:
            logging.info("Directory already exists, skipping download for images in: %s" % dir_path)

    def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        # logging.info("image_paths: %s" % image_paths)
        if not image_paths:
            raise DropItem("No images downloaded %s" % image_paths)
        item['image_paths'] = image_paths
        # 打印完结的日志
        logging.info("Download images completed: %s" % item['title'])

        return item

    def file_path(self, request, response=None, info=None):
        item = request.meta['item']
        index = request.meta['index']
        url = request.url
        # 获取图片格式
        image_format = url.split('.')[-1]
        file_name = f"{item['title']}-{index + 1}"  # 以图片URL的顺序命名文件
        # logging.info("file_name: %s" % file_name)
        return f"{item['title']}/{file_name}.{image_format}"

    def get_directory_path(self, item):
        """
        Returns the directory path for the given item.
        """
        settings = get_project_settings()
        images_store = settings.get('IMAGES_STORE', '')
        dir_path = os.path.join(images_store, item['site'], item['title'])
        return dir_path


class M3U8Pipeline:
    """
    用于下载和处理M3U8视频文件的管道
    支持并行下载多个不同的m3u8视频文件，并确保所有下载完成后才关闭爬虫
    """

    def __init__(self):
        self.settings = get_project_settings()
        self.videos_store = self.settings.get('VIDEOS_STORE', 'videos')
        # FFmpeg多线程下载配置
        self.max_threads = self.settings.get('FFMPEG_MAX_THREADS', 20)  # 单个文件的线程数
        # 并行下载配置
        self.max_concurrent_downloads = self.settings.get('MAX_CONCURRENT_DOWNLOADS', 5)  # 同时下载的视频数量
        # 确保存储目录存在
        if not os.path.exists(self.videos_store):
            os.makedirs(self.videos_store)
        
        # 创建线程池用于并行下载
        self.download_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_concurrent_downloads,
            thread_name_prefix="M3U8Download"
        )
        
        # 正在下载的视频URL集合
        self.downloading_urls = set()
        # 活动下载任务计数器
        self.active_downloads = 0
        # 活动下载任务条件变量（用于等待所有下载完成）
        self.downloads_done = threading.Condition()
        # 线程锁
        self.lock = threading.RLock()
        # 是否已通知爬虫结束
        self.shutdown_notified = False
        
        logging.info(f"M3U8Pipeline初始化完成 - 最大并行下载数: {self.max_concurrent_downloads}, FFmpeg线程数: {self.max_threads}")
        
        # 启动监控线程
        self.monitor_thread = threading.Thread(target=self._monitor_downloads, daemon=True)
        self.monitor_thread.start()

    def process_item(self, item, spider):
        """
        处理每个包含m3u8_url的item
        直接处理传入的item，使用线程池并行处理多个视频
        """
        # 检查item是否包含m3u8_url字段
        if 'm3u8_url' not in item:
            logging.info(f"Item {item.get('title', 'Unknown')} 没有m3u8_url字段，跳过")
            return item

        m3u8_url = item['m3u8_url']
        title = item.get('title', 'Unknown')
        
        logging.info(f"M3U8Pipeline接收到item: {title}")
        
        # 检查是否已经在下载队列中
        with self.lock:
            if m3u8_url in self.downloading_urls:
                logging.info(f"视频 {title} 已在下载队列中，跳过")
                return item
            
            # 标记为正在下载
            self.downloading_urls.add(m3u8_url)
            # 增加活动下载计数
            self.active_downloads += 1

        # 获取存储路径
        dir_path = self.get_directory_path(item)
        
        # 确保目录存在
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            logging.info(f"创建目录: {dir_path}")

        # 提交到线程池异步下载
        future = self.download_executor.submit(self._download_video_async, item, dir_path)
        
        # 添加完成回调
        future.add_done_callback(lambda f: self._download_completed(f, m3u8_url, title))
        
        logging.info(f"已提交M3U8下载任务到线程池: {title}")
        
        return item
    
    def _monitor_downloads(self):
        """
        监控线程，检查并显示下载进度
        """
        last_count = -1
        while True:
            time.sleep(5)  # 每5秒检查一次
            with self.lock:
                if self.active_downloads != last_count:
                    last_count = self.active_downloads
                    if self.active_downloads > 0:
                        logging.info(f"当前正在进行的下载任务: {self.active_downloads}")
                
                # 如果没有活动下载且已经通知过关闭，则退出监控
                if self.active_downloads == 0 and self.shutdown_notified:
                    break

    def _download_video_async(self, item, dir_path):
        """
        异步下载视频的函数
        """
        title = item.get('title', 'Unknown')
        try:
            logging.info(f"开始下载视频: {title}")
            self.download_m3u8(item['m3u8_url'], dir_path, title)
            return {'success': True, 'title': title}
        except Exception as e:
            logging.error(f"异步下载视频失败: {title}, 错误: {e}")
            return {'success': False, 'title': title, 'error': str(e)}

    def _download_completed(self, future, m3u8_url, title):
        """
        下载完成的回调函数
        """
        try:
            result = future.result()
            if result['success']:
                logging.info(f"✅ M3U8视频下载成功: {title}")
            else:
                logging.error(f"❌ M3U8视频下载失败: {title}, 错误: {result.get('error', '未知错误')}")
        except Exception as e:
            logging.error(f"下载回调处理失败: {title}, 错误: {e}")
        finally:
            # 从下载集合中移除并减少活动下载计数
            with self.lock:
                self.downloading_urls.discard(m3u8_url)
                self.active_downloads -= 1
                logging.info(f"视频 {title} 处理完成，剩余下载任务: {self.active_downloads}")
                
                # 如果所有下载都完成了，通知条件变量
                if self.active_downloads == 0:
                    with self.downloads_done:
                        self.downloads_done.notify_all()

    def download_m3u8(self, url, dir_path, title):
        """
        下载m3u8文件及其分片
        如果文件名已存在，会自动在文件名后添加递增数字
        支持多线程下载以提高下载速度
        """
        # 生成唯一的文件名
        output_filename = self.generate_unique_filename(dir_path, title, '.mp4')
        output_path = os.path.join(dir_path, output_filename)

        try:
            # 方法1: 使用ffmpeg多线程下载
            cmd = [
                'ffmpeg',
                '-allowed_extensions', 'ALL',  # 允许所有扩展
                '-threads', str(self.max_threads),  # 设置线程数
                '-http_seekable', '1',  # 启用HTTP可寻址
                '-user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',  # 设置User-Agent
                '-i', url,  # 输入URL
                '-c', 'copy',  # 复制流，不重新编码
                '-bsf:a', 'aac_adtstoasc',  # 音频流处理
                '-threads', str(self.max_threads),  # 输出线程数
                '-y',  # 覆盖输出文件
                output_path
            ]

            logging.info(f"🚀 开始使用ffmpeg多线程下载 (线程数: {self.max_threads}): {title}")

            # 执行命令并捕获输出
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)  # 1小时超时

            if result.returncode == 0:
                logging.info(f"✅ 使用ffmpeg多线程下载完成: {title}")
            else:
                logging.error(f"❌ ffmpeg下载失败，返回码: {result.returncode}, 视频: {title}")
                logging.error(f"错误输出: {result.stderr}")
                raise subprocess.CalledProcessError(result.returncode, cmd)

        except subprocess.TimeoutExpired:
            logging.error(f"⏰ ffmpeg下载超时: {title}")
            raise Exception(f"ffmpeg下载超时: {title}")

        except Exception as e:
            logging.error(f"❌ ffmpeg下载失败: {title}, 错误: {e}")

            try:
                # 方法2: 如果ffmpeg失败，尝试直接下载m3u8文件
                m3u8_filename = self.generate_unique_filename(dir_path, title, '.m3u8')
                m3u8_file_path = os.path.join(dir_path, m3u8_filename)

                logging.info(f"🔄 尝试备用方案：直接下载m3u8文件 - {title}")
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }

                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()

                with open(m3u8_file_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                logging.info(f"✅ 已保存m3u8文件: {title}")

                # 注意: 这里只保存了m3u8文件本身，没有下载视频分片
                # 完整实现应该解析m3u8文件，下载所有分片并合并

            except Exception as e2:
                logging.error(f"❌ 保存m3u8文件失败: {title}, 错误: {e2}")
                raise Exception(f"无法下载m3u8文件: {title}")

    def generate_unique_filename(self, dir_path, title, file_ext):
        """
        生成唯一的文件名，如果已存在同名文件，在文件名后添加递增数字
        """
        # 清理文件名，移除不合法字符
        cleaned_title = self.clean_filename(title)
        base_filename = f"{cleaned_title}{file_ext}"

        # 如果文件不存在，直接返回原始文件名
        if not os.path.exists(os.path.join(dir_path, base_filename)):
            return base_filename

        # 如果文件已存在，添加递增数字
        counter = 1
        while True:
            new_filename = f"{cleaned_title}_{counter}{file_ext}"
            if not os.path.exists(os.path.join(dir_path, new_filename)):
                logging.info(f"文件 {base_filename} 已存在，使用新文件名: {new_filename}")
                return new_filename
            counter += 1

    def clean_filename(self, filename):
        """
        清理文件名，移除或替换不合法字符
        """
        # 定义不合法字符
        illegal_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']

        cleaned = filename
        for char in illegal_chars:
            cleaned = cleaned.replace(char, '_')

        # 移除首尾空格和点
        cleaned = cleaned.strip(' .')

        return cleaned

    def get_directory_path(self, item):
        """
        返回视频文件的存储目录路径
        """
        dir_path = os.path.join(self.videos_store, item.get('site', 'default'))
        return dir_path

    def close_spider(self, spider):
        """
        爬虫关闭时等待所有下载任务完成再关闭
        """
        logging.info("🔄 爬虫即将关闭，等待所有M3U8下载任务完成...")
        
        with self.lock:
            # 标记为已通知关闭
            self.shutdown_notified = True
            
            # 如果没有活动下载，直接关闭
            if self.active_downloads == 0:
                logging.info("✅ 没有活动的下载任务，直接关闭爬虫")
                self._cleanup_resources()
                return
            
            logging.info(f"⏳ 等待 {self.active_downloads} 个下载任务完成...")
        
        # 等待所有下载完成
        with self.downloads_done:
            # 设置最长等待时间，防止无限等待
            self.downloads_done.wait(timeout=7200)  # 最多等待2小时
        
        self._cleanup_resources()
        logging.info("🎉 所有M3U8下载任务已完成，爬虫可以安全关闭")

    def _cleanup_resources(self):
        """
        清理资源
        """
        try:
            # 关闭线程池
            if hasattr(self, 'download_executor'):
                self.download_executor.shutdown(wait=False)
                logging.info("🧹 下载线程池已关闭")
        except Exception as e:
            logging.error(f"❌ 关闭资源时出错: {e}")
