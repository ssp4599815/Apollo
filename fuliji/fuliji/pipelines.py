#!/usr/bin/env python
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

from .utils.database import DownloadDatabase
from .utils.logger_config import PipelineLoggerMixin


class ImgPipeline(PipelineLoggerMixin, ImagesPipeline):

    def __init__(self, store_uri, download_func=None, settings=None):
        super(ImgPipeline, self).__init__(store_uri, download_func, settings)
        # 设置pipeline专属日志
        self.setup_pipeline_logger('img')

    def get_media_requests(self, item, info):
        # 获取将用于存储图片的目录路径
        dir_path = self.get_directory_path(item)
        self.log(f"Checking if directory exists at: {dir_path}")
        if not os.path.exists(dir_path):
            self.log(f"Directory does not exist, downloading images")
            for index, image_url in enumerate(item['image_urls']):
                yield scrapy.Request(image_url, meta={'item': item, 'index': index})
        else:
            self.log(f"Directory already exists, skipping download for images in: {dir_path}")

    def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            self.log(f"No images downloaded {image_paths}", logging.WARNING)
            raise DropItem(f"No images downloaded {image_paths}")
        item['image_paths'] = image_paths
        # 打印完结的日志
        self.log(f"Download images completed: {item['title']}")

        return item

    def file_path(self, request, response=None, info=None):
        item = request.meta['item']
        index = request.meta['index']
        url = request.url
        # 获取图片格式
        image_format = url.split('.')[-1]
        file_name = f"{item['title']}-{index + 1}"  # 以图片URL的顺序命名
        # 确保字符合法用于文件名
        file_name = self.sanitize_filename(file_name)
        self.log(f"Storing image at: {file_name}.{image_format}")
        return f"{item['site']}/{file_name}.{image_format}"

    def get_directory_path(self, item):
        # 基于item的'title'构建图片存储目录路径
        settings = get_project_settings()
        images_store = settings.get('IMAGES_STORE')
        sanitized_title = self.sanitize_filename(item['title'])
        return os.path.join(images_store, item['site'], sanitized_title)

    def sanitize_filename(self, filename):
        """清理文件名，移除或替换不符合要求的字符."""
        import re
        # 移除/替换文件名中不合法的字符
        sanitized_name = re.sub(r'[\\/*?:"<>|\s]', '_', filename)
        # 限制文件名长度
        if len(sanitized_name) > 100:
            sanitized_name = sanitized_name[:100]
        return sanitized_name


class M3U8Pipeline(PipelineLoggerMixin):
    """
    改进的M3U8视频下载管道，支持：
    1. 断点续传
    2. 临时文件管理  
    3. SQLite数据库记录
    4. 自动清理临时文件
    5. 排除指定标题的视频下载
    """

    def __init__(self):
        # 设置pipeline专属日志
        self.setup_pipeline_logger('m3u8')

        self.settings = get_project_settings()
        self.videos_store = self.settings.get('VIDEOS_STORE', 'videos')

        # 临时文件目录
        self.temp_store = os.path.join(os.path.dirname(self.videos_store), 'temp_downloads')

        # 数据库文件路径
        self.db_path = self.settings.get('DATABASE_PATH', './data/downloads.db')

        # FFmpeg多线程下载配置
        self.max_threads = self.settings.get('FFMPEG_MAX_THREADS', 20)
        # 并行下载配置
        self.max_concurrent_downloads = self.settings.get('MAX_CONCURRENT_DOWNLOADS', 5)

        # 确保存储目录存在
        for directory in [self.videos_store, self.temp_store]:
            if not os.path.exists(directory):
                os.makedirs(directory)
                self.log(f"创建目录: {directory}")

        # 初始化数据库
        self.db = DownloadDatabase(self.db_path)

        # 创建线程池用于并行下载
        self.download_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_concurrent_downloads,
            thread_name_prefix="M3U8Download"
        )

        # 正在下载的视频URL集合
        self.downloading_urls = set()
        # 已处理的视频URL集合
        self.processed_urls = set()
        # 正在下载的视频标题集合
        self.downloading_titles = set()
        # 已处理的视频标题集合
        self.processed_titles = set()
        # 活动下载任务计数器
        self.active_downloads = 0
        # 活动下载任务条件变量（用于等待所有下载完成）
        self.downloads_done = threading.Condition()
        # 线程锁
        self.lock = threading.RLock()
        # 是否已通知爬虫结束
        self.shutdown_notified = False

        # 初始化排除标题列表
        self.excluded_titles = self._load_excluded_titles()

        self.log(
            f"M3U8Pipeline初始化完成 - 最大并行下载数: {self.max_concurrent_downloads}, FFmpeg线程数: {self.max_threads}")
        self.log(f"视频存储目录: {self.videos_store}")
        self.log(f"临时文件目录: {self.temp_store}")
        self.log(f"数据库文件: {self.db_path}")
        self.log(f"已加载排除标题数量: {len(self.excluded_titles)}")

        # 启动监控线程
        self.monitor_thread = threading.Thread(target=self._monitor_downloads, daemon=True)
        self.monitor_thread.start()

    def _load_excluded_titles(self):
        """
        加载从 utils/chigua 文件中排除的标题列表
        """
        excluded_titles = set()
        chigua_path = os.path.join(os.path.dirname(__file__), 'utils', '51chigua.txt')

        if os.path.exists(chigua_path):
            try:
                with open(chigua_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):  # 跳过空行和注释
                            excluded_titles.add(line)
                self.log(f"成功从 {chigua_path} 加载 {len(excluded_titles)} 个排除标题")
            except Exception as e:
                self.log(f"加载排除标题文件失败: {e}")
        else:
            self.log(f"排除标题文件不存在: {chigua_path}")

        return excluded_titles

    def _is_title_excluded(self, title):
        """
        检查标题是否在排除列表中
        """
        return title in self.excluded_titles

    def _get_url_hash(self, url):
        """生成URL的唯一哈希标识"""
        return self.db.get_url_hash(url)

    def _is_download_completed(self, url, title):
        """检查视频是否已经下载完成"""
        completed, file_path = self.db.is_download_completed(url)

        if completed:
            self.log(f"视频 {title} 已下载完成，文件路径: {file_path}")
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
        支持单个URL和URL列表，进行多层去重检查
        """
        # 检查item是否包含m3u8_url字段
        if 'm3u8_url' not in item:
            self.log(f"Item {item.get('title', 'Unknown')} 没有m3u8_url字段，跳过")
            return item

        m3u8_url_data = item['m3u8_url']
        title = item.get('title', 'Unknown')
        site = item.get('site', 'unknown')

        # 统一处理：将单个URL转换为列表
        if isinstance(m3u8_url_data, str):
            m3u8_urls = [m3u8_url_data]
        elif isinstance(m3u8_url_data, list):
            m3u8_urls = m3u8_url_data
        else:
            self.log(f"❌ m3u8_url字段类型不支持: {type(m3u8_url_data)}")
            return item

        self.log(f"🎯 M3U8Pipeline接收到item: {title}, URL数量: {len(m3u8_urls)}")

        # 第一层：检查是否在排除列表中
        if self._is_title_excluded(title):
            self.log(f"❌ 视频 '{title}' 在排除列表中，跳过下载")
            return item

        # 对URL列表进行去重处理
        unique_urls = self._deduplicate_urls(m3u8_urls, title)

        if not unique_urls:
            self.log(f"❌ 所有URL都已处理过或重复，跳过: {title}")
            return item

        self.log(f"✅ 去重后的URL数量: {len(unique_urls)}")

        # 处理每个唯一的URL
        processed_count = 0
        for m3u8_url in unique_urls:
            if self._process_single_url(m3u8_url, title, site, item):
                processed_count += 1

        if processed_count > 0:
            self.log(f"✅ 成功处理了 {processed_count} 个URL，标题: {title}")
        else:
            self.log(f"❌ 没有成功处理任何URL，标题: {title}")

        return item

    def _deduplicate_urls(self, m3u8_urls, title):
        """
        对m3u8_url列表进行去重
        """
        unique_urls = []
        seen_urls = set()
        seen_url_keys = set()

        for url in m3u8_urls:
            if not url or not isinstance(url, str):
                continue

            # URL标准化
            normalized_url = url.strip()

            # 第一步：完全相同的URL去重
            if normalized_url in seen_urls:
                self.log(f"🔄 发现完全重复的URL，跳过: {normalized_url[:100]}...")
                continue

            # 第二步：提取URL关键特征进行去重
            url_key = self._extract_url_key(normalized_url)
            if url_key in seen_url_keys:
                self.log(f"🔄 发现相似URL，跳过: {normalized_url[:100]}...")
                continue

            # 第三步：检查是否已在会话中处理过
            with self.lock:
                if normalized_url in self.processed_urls:
                    self.log(f"🔄 URL已在本次会话中处理过，跳过: {normalized_url[:100]}...")
                    continue

                # 检查相似URL
                is_similar, similar_url = self._is_similar_url(normalized_url)
                if is_similar:
                    self.log(f"🔄 发现相似URL已处理，跳过: {normalized_url[:100]}...")
                    continue

            # 第四步：检查数据库
            if self.db.is_downloaded(normalized_url):
                self.log(f"🔄 URL已在数据库中存在，跳过: {normalized_url[:100]}...")
                with self.lock:
                    self.processed_urls.add(normalized_url)
                continue

            # 第五步：检查是否在当前下载队列中
            with self.lock:
                if normalized_url in self.downloading_urls:
                    self.log(f"🔄 URL已在下载队列中，跳过: {normalized_url[:100]}...")
                    continue

            # 通过所有检查，添加到唯一列表
            unique_urls.append(normalized_url)
            seen_urls.add(normalized_url)
            seen_url_keys.add(url_key)

        return unique_urls

    def _process_single_url(self, m3u8_url, title, site, item):
        """
        处理单个m3u8 URL
        """
        try:
            # 最后一次检查：确保URL仍然有效
            with self.lock:
                # 再次检查是否在下载队列中（避免并发问题）
                if m3u8_url in self.downloading_urls:
                    self.log(f"❌ URL已在下载队列中，跳过: {m3u8_url[:100]}...")
                    return False

                # 标记为正在下载
                self.downloading_urls.add(m3u8_url)
                self.downloading_titles.add(title)
                self.processed_urls.add(m3u8_url)
                self.processed_titles.add(title)

            # 生成唯一的文件名（防止多个URL下载到同一文件）
            safe_filename = self.sanitize_filename(title)

            # 如果有多个URL，需要添加序号
            url_hash = self._get_url_hash(m3u8_url)
            final_filename = f"{safe_filename}_{url_hash}"

            output_file = os.path.join(self.download_path, f"{final_filename}.mp4")

            # 检查文件是否已存在
            if os.path.exists(output_file):
                with self.lock:
                    self.downloading_urls.discard(m3u8_url)
                    self.downloading_titles.discard(title)
                self.log(f"❌ 文件已存在，跳过: {output_file}")
                self.db.mark_as_downloaded(m3u8_url, output_file, "already_exists")
                return False

            # 提交下载任务到线程池
            future = self.download_executor.submit(self.download_m3u8,
                                                   {'m3u8_url': m3u8_url, 'title': title, 'site': site},
                                                   output_file)
            self.download_tasks.append(future)

            self.log(f"✅ 已添加到下载队列: {title} -> {m3u8_url[:100]}...")
            return True

        except Exception as e:
            self.log(f"❌ 处理URL失败: {e}")
            # 清理标记
            with self.lock:
                self.downloading_urls.discard(m3u8_url)
                self.downloading_titles.discard(title)
            return False

    def _get_url_hash(self, url):
        """
        生成URL的短哈希值，用于文件名
        """
        import hashlib
        return hashlib.md5(url.encode()).hexdigest()[:8]

    def _extract_url_key(self, url):
        """
        从URL中提取关键标识符用于重复检测
        增强版本，提取更多特征
        """
        import re

        # 方法1：提取长的字母数字组合
        matches = re.findall(r'[a-zA-Z0-9]{8,}', url)
        if matches:
            # 返回最长的匹配项作为关键标识
            longest_match = max(matches, key=len)
            if len(longest_match) >= 12:  # 足够长的标识符
                return longest_match

        # 方法2：提取路径中的关键部分
        # 例如: /video/abc123/playlist.m3u8 -> abc123
        path_matches = re.findall(r'/([a-zA-Z0-9]{6,})/', url)
        if path_matches:
            return path_matches[-1]  # 取最后一个匹配

        # 方法3：提取文件名（不含扩展名）
        filename_match = re.search(r'/([^/]+)\.m3u8', url)
        if filename_match:
            filename = filename_match.group(1)
            if len(filename) >= 6:
                return filename

        # 方法4：如果都没找到，返回URL的哈希值
        import hashlib
        return hashlib.md5(url.encode()).hexdigest()[:16]

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
                        self.log(f"当前正在进行的下载任务: {self.active_downloads}")

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
            self.log(f"开始下载视频: {title}")

            # 获取临时文件路径和最终文件路径
            temp_file_path = self._get_temp_file_path(m3u8_url, title)
            final_file_path = self._get_final_file_path(item, title)

            # 检查是否存在临时文件（断点续传）
            resume_download = os.path.exists(temp_file_path) and os.path.getsize(temp_file_path) > 0

            if resume_download:
                self.log(f"发现临时文件，尝试断点续传: {title}")

            # 下载视频到临时文件
            success = self.download_m3u8_with_resume(m3u8_url, temp_file_path, title, resume_download)

            if success:
                # 获取文件大小
                file_size = os.path.getsize(temp_file_path)

                # 下载成功，移动到正式目录
                shutil.move(temp_file_path, final_file_path)
                self.log(f"✅ 视频下载完成，已移动到: {final_file_path}")

                # 更新数据库记录
                self.db.update_download_status(m3u8_url, 'completed', final_file_path, file_size)

                # 将成功下载的视频标题追加到排除列表文件中
                self._append_to_excluded_list(title)

                return {'success': True, 'title': title, 'file_path': final_file_path}
            else:
                # 下载失败，更新状态
                self.db.update_download_status(m3u8_url, 'failed')
                self.log(f"❌ 视频下载失败，临时文件保留: {temp_file_path}")
                return {'success': False, 'title': title}

        except Exception as e:
            # 下载异常，更新状态
            self.db.update_download_status(m3u8_url, 'error')
            self.log(f"异步下载视频失败: {title}, 错误: {e}")
            return {'success': False, 'title': title, 'error': str(e)}

    def _download_completed(self, future, m3u8_url, title):
        """
        下载完成的回调函数
        """
        try:
            result = future.result()
            if result['success']:
                self.log(f"✅ M3U8视频下载成功: {title}")
            else:
                self.log(f"❌ M3U8视频下载失败: {title}, 错误: {result.get('error', '未知错误')}")
        except Exception as e:
            self.log(f"下载回调处理失败: {title}, 错误: {e}")
        finally:
            # 从下载集合中移除并减少活动下载计数
            with self.lock:
                self.downloading_urls.discard(m3u8_url)
                self.active_downloads -= 1
                self.log(f"视频 {title} 处理完成，剩余下载任务: {self.active_downloads}")

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
                # '-http_seekable', '1',  # 启用HTTP可寻址 (旧版本ffmpeg不支持此参数)
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
                self.log(f"🔄 断点续传下载: {title}")
            else:
                self.log(f"🚀 开始新下载 (线程数: {self.max_threads}): {title}")

            # 执行命令并捕获输出（兼容Python 3.6及以下版本）
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                                    timeout=3600)  # 1小时超时

            if result.returncode == 0:
                # 检查文件是否真的下载完成（文件大小大于0）
                if os.path.exists(temp_file_path) and os.path.getsize(temp_file_path) > 0:
                    self.log(f"✅ 视频下载完成: {title}")
                    return True
                else:
                    self.log(f"❌ 下载完成但文件无效: {title}")
                    return False
            else:
                self.log(f"❌ ffmpeg下载失败，返回码: {result.returncode}, 视频: {title}")
                self.log(f"错误输出: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            self.log(f"⏰ ffmpeg下载超时: {title}")
            return False

        except Exception as e:
            self.log(f"❌ 下载过程中出现异常: {title}, 错误: {e}")
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
                self.log(f"文件 {base_filename} 已存在，使用新文件名: {new_filename}")
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
                        self.log(f"删除过期临时文件: {temp_file}")
                except Exception as e:
                    self.log(f"删除临时文件失败 {temp_file}: {e}")

            if cleaned_count > 0:
                self.log(f"清理了 {cleaned_count} 个过期临时文件")

        except Exception as e:
            self.log(f"清理临时文件时出错: {e}")

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
        self.log("🔄 爬虫即将关闭，等待所有M3U8下载任务完成...")

        # 显示当前下载状态
        status = self.get_download_status()
        self.log(f"📊 下载状态统计: {status}")

        with self.lock:
            # 标记为已通知关闭
            self.shutdown_notified = True

            # 如果没有活动下载，直接关闭
            if self.active_downloads == 0:
                self.log("✅ 没有活动的下载任务，直接关闭爬虫")
                self._cleanup_resources()
                return

            self.log(f"⏳ 等待 {self.active_downloads} 个下载任务完成...")

        # 等待所有下载完成
        with self.downloads_done:
            # 设置最长等待时间，防止无限等待
            self.downloads_done.wait(timeout=7200)  # 最多等待2小时

        # 清理孤立的临时文件
        self.cleanup_temp_files()

        self._cleanup_resources()

        # 最终状态统计
        final_status = self.get_download_status()
        self.log(f"📊 最终下载状态: {final_status}")
        self.log("🎉 所有M3U8下载任务已完成，爬虫可以安全关闭")

    def _cleanup_resources(self):
        """
        清理资源
        """
        try:
            # 关闭数据库连接
            if hasattr(self, 'db'):
                self.db.close()
                self.log("🗄️ 数据库连接已关闭")

            # 关闭线程池
            if hasattr(self, 'download_executor'):
                self.download_executor.shutdown(wait=False)
                self.log("🧹 下载线程池已关闭")

        except Exception as e:
            self.log(f"❌ 关闭资源时出错: {e}")

    def _append_to_excluded_list(self, title):
        """
        将成功下载的视频标题追加到排除列表文件中
        """
        try:
            chigua_path = os.path.join(os.path.dirname(__file__), 'utils', '51chigua.txt')

            # 确保目录存在
            os.makedirs(os.path.dirname(chigua_path), exist_ok=True)

            # 追加标题到文件（不包含.mp4后缀）
            with open(chigua_path, 'a', encoding='utf-8') as f:
                f.write(f"\n{title}")

            # 同时添加到内存中的排除集合，避免重复下载
            self.excluded_titles.add(title)

            self.log(f"✅ 已将标题 '{title}' 追加到排除列表文件: {chigua_path}")

        except Exception as e:
            self.log(f"❌ 追加标题到排除列表失败: {title}, 错误: {e}")

    def get_url_dedup_stats(self):
        """
        获取URL去重统计信息
        """
        with self.lock:
            return {
                'downloading_urls': len(self.downloading_urls),
                'processed_urls': len(self.processed_urls),
                'downloading_titles': len(self.downloading_titles),
                'processed_titles': len(self.processed_titles)
            }
