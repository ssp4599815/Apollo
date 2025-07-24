# !/usr/bin/env python
# -*-coding:utf-8 -*-
import concurrent.futures
import logging
import os
import shutil
import subprocess
import threading
import time

import scrapy
from scrapy.exceptions import DropItem
from scrapy.pipelines.images import ImagesPipeline
from scrapy.utils.project import get_project_settings

from .database import DownloadDatabase


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
    改进的M3U8视频下载管道，支持：
    1. 断点续传
    2. 临时文件管理  
    3. SQLite数据库记录
    4. 自动清理临时文件
    """

    def __init__(self):
        self.settings = get_project_settings()
        self.videos_store = self.settings.get('VIDEOS_STORE', 'videos')

        # 临时文件目录
        self.temp_store = os.path.join(os.path.dirname(self.videos_store), 'temp_downloads')

        # 数据库文件路径
        self.db_path = os.path.join(os.path.dirname(self.videos_store), 'downloads.db')

        # FFmpeg多线程下载配置
        self.max_threads = self.settings.get('FFMPEG_MAX_THREADS', 20)
        # 并行下载配置
        self.max_concurrent_downloads = self.settings.get('MAX_CONCURRENT_DOWNLOADS', 5)

        # 确保存储目录存在
        for directory in [self.videos_store, self.temp_store]:
            if not os.path.exists(directory):
                os.makedirs(directory)
                logging.info(f"创建目录: {directory}")

        # 初始化数据库
        self.db = DownloadDatabase(self.db_path)

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

        logging.info(
            f"M3U8Pipeline初始化完成 - 最大并行下载数: {self.max_concurrent_downloads}, FFmpeg线程数: {self.max_threads}")
        logging.info(f"视频存储目录: {self.videos_store}")
        logging.info(f"临时文件目录: {self.temp_store}")
        logging.info(f"数据库文件: {self.db_path}")

        # 启动监控线程
        self.monitor_thread = threading.Thread(target=self._monitor_downloads, daemon=True)
        self.monitor_thread.start()

    def _get_url_hash(self, url):
        """生成URL的唯一哈希标识"""
        return self.db.get_url_hash(url)

    def _is_download_completed(self, url, title):
        """检查视频是否已经下载完成"""
        completed, file_path = self.db.is_download_completed(url)

        if completed:
            logging.info(f"视频 {title} 已下载完成，文件路径: {file_path}")
            return True

        return False

    def _get_temp_file_path(self, url, title):
        """获取临时文件路径"""
        # 先尝试从数据库获取现有的临时文件路径
        existing_temp_path = self.db.get_temp_file_path(url)
        if existing_temp_path and os.path.exists(existing_temp_path):
            return existing_temp_path

        # 生成新的临时文件路径
        url_hash = self._get_url_hash(url)
        cleaned_title = self.clean_filename(title)
        temp_filename = f"{cleaned_title}_{url_hash}.tmp.mp4"
        return os.path.join(self.temp_store, temp_filename)

    def _get_final_file_path(self, item, title):
        """获取最终文件路径"""
        dir_path = self.get_directory_path(item)
        final_filename = self.generate_unique_filename(dir_path, title, '.mp4')
        return os.path.join(dir_path, final_filename)

    def process_item(self, item, spider):
        """
        处理每个包含m3u8_url的item
        检查是否已下载完成，支持断点续传
        """
        # 检查item是否包含m3u8_url字段
        if 'm3u8_url' not in item:
            logging.info(f"Item {item.get('title', 'Unknown')} 没有m3u8_url字段，跳过")
            return item

        m3u8_url = item['m3u8_url']
        title = item.get('title', 'Unknown')
        site = item.get('site', 'unknown')

        logging.info(f"M3U8Pipeline接收到item: {title}")

        # 检查是否已经下载完成
        if self._is_download_completed(m3u8_url, title):
            logging.info(f"视频 {title} 已下载完成，跳过下载")
            return item

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

        # 获取临时文件路径并添加数据库记录
        temp_file_path = self._get_temp_file_path(m3u8_url, title)
        self.db.add_download_record(m3u8_url, title, site, temp_file_path)

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
        异步下载视频的函数，支持断点续传
        """
        title = item.get('title', 'Unknown')
        m3u8_url = item['m3u8_url']

        try:
            logging.info(f"开始下载视频: {title}")

            # 获取临时文件路径和最终文件路径
            temp_file_path = self._get_temp_file_path(m3u8_url, title)
            final_file_path = self._get_final_file_path(item, title)

            # 检查是否存在临时文件（断点续传）
            resume_download = os.path.exists(temp_file_path) and os.path.getsize(temp_file_path) > 0

            if resume_download:
                logging.info(f"发现临时文件，尝试断点续传: {title}")

            # 下载视频到临时文件
            success = self.download_m3u8_with_resume(m3u8_url, temp_file_path, title, resume_download)

            if success:
                # 获取文件大小
                file_size = os.path.getsize(temp_file_path)

                # 下载成功，移动到正式目录
                shutil.move(temp_file_path, final_file_path)
                logging.info(f"✅ 视频下载完成，已移动到: {final_file_path}")

                # 更新数据库记录
                self.db.update_download_status(m3u8_url, 'completed', final_file_path, file_size)

                return {'success': True, 'title': title, 'file_path': final_file_path}
            else:
                # 下载失败，更新状态
                self.db.update_download_status(m3u8_url, 'failed')
                logging.error(f"❌ 视频下载失败，临时文件保留: {temp_file_path}")
                return {'success': False, 'title': title}

        except Exception as e:
            # 下载异常，更新状态
            self.db.update_download_status(m3u8_url, 'error')
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

    def download_m3u8_with_resume(self, url, temp_file_path, title, resume=False):
        """
        下载m3u8文件，支持断点续传
        """
        try:
            # 构建ffmpeg命令
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
            ]

            # 如果是断点续传且文件存在，则不覆盖
            if not resume:
                cmd.append('-y')  # 覆盖输出文件

            cmd.append(temp_file_path)

            if resume:
                logging.info(f"🔄 断点续传下载: {title}")
            else:
                logging.info(f"🚀 开始新下载 (线程数: {self.max_threads}): {title}")

            # 执行命令并捕获输出
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)  # 1小时超时

            if result.returncode == 0:
                # 检查文件是否真的下载完成（文件大小大于0）
                if os.path.exists(temp_file_path) and os.path.getsize(temp_file_path) > 0:
                    logging.info(f"✅ 视频下载完成: {title}")
                    return True
                else:
                    logging.error(f"❌ 下载完成但文件无效: {title}")
                    return False
            else:
                logging.error(f"❌ ffmpeg下载失败，返回码: {result.returncode}, 视频: {title}")
                logging.error(f"错误输出: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            logging.error(f"⏰ ffmpeg下载超时: {title}")
            return False

        except Exception as e:
            logging.error(f"❌ 下载过程中出现异常: {title}, 错误: {e}")
            return False

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

    def cleanup_temp_files(self):
        """
        清理孤立的临时文件（可选功能）
        """
        try:
            if not os.path.exists(self.temp_store):
                return

            temp_files = [f for f in os.listdir(self.temp_store) if f.endswith('.tmp.mp4')]

            # 检查临时文件的修改时间，删除超过7天的文件
            current_time = time.time()
            week_ago = current_time - (7 * 24 * 60 * 60)  # 7天前

            cleaned_count = 0
            for temp_file in temp_files:
                temp_file_path = os.path.join(self.temp_store, temp_file)
                try:
                    file_mtime = os.path.getmtime(temp_file_path)
                    if file_mtime < week_ago:
                        os.remove(temp_file_path)
                        cleaned_count += 1
                        logging.info(f"删除过期临时文件: {temp_file}")
                except Exception as e:
                    logging.error(f"删除临时文件失败 {temp_file}: {e}")

            if cleaned_count > 0:
                logging.info(f"清理了 {cleaned_count} 个过期临时文件")

        except Exception as e:
            logging.error(f"清理临时文件时出错: {e}")

    def get_download_status(self):
        """
        获取下载状态统计
        """
        stats = self.db.get_download_statistics()
        temp_files_count = 0

        if os.path.exists(self.temp_store):
            temp_files_count = len([f for f in os.listdir(self.temp_store) if f.endswith('.tmp.mp4')])

        stats['temp_files'] = temp_files_count
        stats['active_downloads'] = self.active_downloads

        return stats

    def close_spider(self, spider):
        """
        爬虫关闭时等待所有下载任务完成再关闭
        """
        logging.info("🔄 爬虫即将关闭，等待所有M3U8下载任务完成...")

        # 显示当前下载状态
        status = self.get_download_status()
        logging.info(f"📊 下载状态统计: {status}")

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

        # 清理孤立的临时文件
        self.cleanup_temp_files()

        self._cleanup_resources()

        # 最终状态统计
        final_status = self.get_download_status()
        logging.info(f"📊 最终下载状态: {final_status}")
        logging.info("🎉 所有M3U8下载任务已完成，爬虫可以安全关闭")

    def _cleanup_resources(self):
        """
        清理资源
        """
        try:
            # 关闭数据库连接
            if hasattr(self, 'db'):
                self.db.close()
                logging.info("🗄️ 数据库连接已关闭")

            # 关闭线程池
            if hasattr(self, 'download_executor'):
                self.download_executor.shutdown(wait=False)
                logging.info("🧹 下载线程池已关闭")

        except Exception as e:
            logging.error(f"❌ 关闭资源时出错: {e}")
