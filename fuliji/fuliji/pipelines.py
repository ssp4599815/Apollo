#!/usr/bin/env python
# -*-coding:utf-8 -*-
import concurrent.futures
import logging
import os
import re
import shutil
import subprocess
import threading
import time

import scrapy
from scrapy.exceptions import DropItem
from scrapy.pipelines.images import ImagesPipeline
from scrapy.utils.project import get_project_settings

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
        # 移除/替换文件名中不合法的字符
        sanitized_name = re.sub(r'[\\/*?:"<>|\s]', '_', filename)
        # 限制文件名长度
        if len(sanitized_name) > 100:
            sanitized_name = sanitized_name[:100]
        return sanitized_name


class M3U8Pipeline(PipelineLoggerMixin):
    """
    精简版M3U8视频下载管道，支持：
    1. 多线程下载
    2. 临时文件管理  
    3. 下载前检查是否已下载
    4. 下载完成后记录到 m3u8_urls.txt
    5. 实时下载进度监控
    """

    def __init__(self):
        # 设置pipeline专属日志
        self.setup_pipeline_logger('m3u8')

        self.settings = get_project_settings()
        self.videos_store = self.settings.get('VIDEOS_STORE', 'videos')

        # 临时文件目录
        self.temp_store = os.path.join(os.path.dirname(self.videos_store), 'temp_downloads')

        # FFmpeg多线程下载配置
        self.max_threads = self.settings.get('FFMPEG_MAX_THREADS', 20)
        # 并行下载配置
        self.max_concurrent_downloads = self.settings.get('MAX_CONCURRENT_DOWNLOADS', 5)

        # 确保存储目录存在
        for directory in [self.videos_store, self.temp_store]:
            if not os.path.exists(directory):
                os.makedirs(directory)
                self.log(f"创建目录: {directory}")

        # 创建线程池用于并行下载
        self.download_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_concurrent_downloads,
            thread_name_prefix="M3U8Download"
        )

        # 正在下载的视频URL集合
        self.downloading_urls = set()
        # 已处理的视频URL集合（用于URL级别去重）
        self.processed_urls = set()
        # 活动下载任务计数器
        self.active_downloads = 0
        # 线程锁
        self.lock = threading.RLock()

        # 下载进度统计
        self.download_stats = {
            'total_received': 0,        # 总接收数量（接收到的item数量）
            'download_success': 0,      # 下载成功数量
            'download_failed': 0,       # 下载失败数量
            'skipped_duplicate': 0,     # 跳过的重复项
            'queued_downloads': 0,      # 排队中的下载任务
            'start_time': time.time(),  # 开始时间
        }
        
        # 当前正在下载的视频信息 {url: {'title': title, 'start_time': time}}
        self.current_downloads = {}

        # 初始化排除标题列表
        self.excluded_titles = self._load_excluded_titles()
        # 初始化已下载URL列表
        self.excluded_urls = self._load_excluded_urls()

        self.log(f"M3U8Pipeline初始化完成 - 最大并行下载数: {self.max_concurrent_downloads}, FFmpeg线程数: {self.max_threads}")
        self.log(f"视频存储目录: {self.videos_store}")
        self.log(f"临时文件目录: {self.temp_store}")
        self.log(f"已加载排除标题数量: {len(self.excluded_titles)}")
        self.log(f"已加载排除URL数量: {len(self.excluded_urls)}")

        # 启动进度监控线程
        self.progress_monitor_thread = threading.Thread(target=self._progress_monitor, daemon=True)
        self.progress_monitor_thread.start()
        self.log("📊 下载进度监控已启动")

    def process_item(self, item, spider):
        """
        处理每个包含m3u8_url的item，支持URL和标题双重去重
        """
        # 检查item是否包含m3u8_url字段
        if 'm3u8_url' not in item:
            self.log(f"Item {item.get('title', 'Unknown')} 没有m3u8_url字段，跳过")
            return item

        m3u8_url = item['m3u8_url']
        title = item.get('title', 'Unknown')

        with self.lock:
            self.download_stats['total_received'] += 1

        self.log(f"🎯 M3U8Pipeline接收到item [{self.download_stats['total_received']}]: {title}")

        # 第一重去重：检查URL是否已经处理过
        if m3u8_url in self.processed_urls:
            self.log(f"🔄 URL '{m3u8_url}' 已处理过，跳过重复下载")
            with self.lock:
                self.download_stats['skipped_duplicate'] += 1
            return item

        # 第二重去重：检查URL是否在已下载URL列表中
        if self._is_url_excluded(m3u8_url):
            self.log(f"❌ URL '{m3u8_url}' 已下载过，跳过下载")
            # 添加到已处理集合
            self.processed_urls.add(m3u8_url)
            with self.lock:
                self.download_stats['skipped_duplicate'] += 1
            return item

        # 第三重去重：检查标题是否在排除列表中（已下载过）
        if self._is_title_excluded(title):
            self.log(f"❌ 视频 '{title}' 已下载过，跳过下载")
            # 添加到已处理集合
            self.processed_urls.add(m3u8_url)
            with self.lock:
                self.download_stats['skipped_duplicate'] += 1
            return item

        # 检查是否正在下载
        with self.lock:
            if m3u8_url in self.downloading_urls:
                self.log(f"⏳ 视频 '{title}' 正在下载中，跳过重复下载")
                self.download_stats['skipped_duplicate'] += 1
                return item
            
            # 添加到下载集合和已处理集合
            self.downloading_urls.add(m3u8_url)
            self.processed_urls.add(m3u8_url)
            
            # 记录当前下载信息
            self.current_downloads[m3u8_url] = {
                'title': title,
                'start_time': time.time()
            }

            # 更新排队中的下载任务数量
            self.download_stats['queued_downloads'] += 1

        # 提交下载任务到线程池
        self.log(f"🚀 提交下载任务 [排队: {self.download_stats['queued_downloads']}, 活动: {len(self.current_downloads)}/{self.max_concurrent_downloads}]: {title}")
        future = self.download_executor.submit(self._download_video, item)
        future.add_done_callback(lambda f: self._download_completed(f, m3u8_url, title))

        return item

    def _download_video(self, item):
        """
        下载视频的主要方法
        """
        title = item.get('title', 'Unknown')
        m3u8_url = item['m3u8_url']

        try:
            with self.lock:
                self.active_downloads += 1
                self.download_stats['queued_downloads'] -= 1

            self.log(f"🚀 开始下载视频: {title}")

            # 获取输出目录和文件路径
            dir_path = self.get_directory_path(item)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)

            # 生成临时文件和最终文件路径
            temp_file = self._get_temp_file_path(title)
            final_file = self._get_final_file_path(item, title)

            # 使用ffmpeg下载
            success = self._download_m3u8_with_ffmpeg(m3u8_url, temp_file, title)

            if success and os.path.exists(temp_file):
                # 移动临时文件到最终位置
                shutil.move(temp_file, final_file)
                
                # 记录标题到已下载列表
                self._append_to_excluded_list(title)
                # 记录URL到已下载列表
                self._append_to_excluded_urls(m3u8_url)
                
                self.log(f"✅ 视频下载完成: {title}")
                return {'success': True, 'title': title, 'file_path': final_file}
            else:
                self.log(f"❌ 视频下载失败: {title}")
                return {'success': False, 'title': title}

        except Exception as e:
            self.log(f"❌ 下载过程中出现异常: {title}, 错误: {e}")
            return {'success': False, 'title': title, 'error': str(e)}
        finally:
            with self.lock:
                self.active_downloads -= 1

    def _download_m3u8_with_ffmpeg(self, url, temp_file_path, title):
        """
        使用ffmpeg下载m3u8文件
        """
        try:
            # 构建ffmpeg命令
            cmd = [
                'ffmpeg',
                '-allowed_extensions', 'ALL',  # 允许所有扩展
                '-threads', str(self.max_threads),  # 设置线程数
                '-user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',  # 设置User-Agent
                '-i', url,  # 输入URL
                '-c', 'copy',  # 复制流，不重新编码
                '-bsf:a', 'aac_adtstoasc',  # 音频流处理
                '-y',  # 覆盖输出文件
                temp_file_path
            ]

            self.log(f"🚀 开始ffmpeg下载 (线程数: {self.max_threads}): {title}")

            # 执行命令
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    universal_newlines=True, timeout=3600)  # 1小时超时

            if result.returncode == 0:
                # 检查文件是否真的下载完成
                if os.path.exists(temp_file_path) and os.path.getsize(temp_file_path) > 0:
                    self.log(f"✅ ffmpeg下载完成: {title}")
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

    def _download_completed(self, future, m3u8_url, title):
        """
        下载完成的回调函数
        """
        try:
            result = future.result()
            
            # 计算下载耗时
            download_time = 0
            if m3u8_url in self.current_downloads:
                download_time = time.time() - self.current_downloads[m3u8_url]['start_time']
            
            with self.lock:
                # 减少排队中的任务数量
                self.download_stats['queued_downloads'] -= 1
                
                if result['success']:
                    self.download_stats['download_success'] += 1
                    self.log(f"✅ M3U8视频下载成功: {title} (耗时: {download_time:.1f}秒)")
                    # 记录URL到已下载URL列表
                    self._append_to_excluded_urls(m3u8_url)
                else:
                    self.download_stats['download_failed'] += 1
                    self.log(f"❌ M3U8视频下载失败: {title} (耗时: {download_time:.1f}秒)")
        except Exception as e:
            with self.lock:
                self.download_stats['queued_downloads'] -= 1
                self.download_stats['download_failed'] += 1
            self.log(f"下载回调处理失败: {title}, 错误: {e}")
        finally:
            # 从下载集合中移除
            with self.lock:
                self.downloading_urls.discard(m3u8_url)
                self.current_downloads.pop(m3u8_url, None)

    def _load_excluded_titles(self):
        """
        加载从 utils/51chigua.txt 文件中排除的标题列表
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

    def _load_excluded_urls(self):
        """
        加载已下载过的URL列表
        """
        excluded_urls = set()
        urls_path = os.path.join(os.path.dirname(__file__), 'utils', 'm3u8_urls.txt')

        if os.path.exists(urls_path):
            try:
                with open(urls_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):  # 跳过空行和注释
                            excluded_urls.add(line)
                self.log(f"成功从 {urls_path} 加载 {len(excluded_urls)} 个排除URL")
            except Exception as e:
                self.log(f"加载排除URL文件失败: {e}")
        else:
            self.log(f"排除URL文件不存在: {urls_path}，将创建新文件")
            # 确保目录存在
            os.makedirs(os.path.dirname(urls_path), exist_ok=True)
            # 创建空文件
            with open(urls_path, 'w', encoding='utf-8') as f:
                f.write("# 已下载的M3U8视频URL列表\n")

        return excluded_urls

    def _is_title_excluded(self, title):
        """
        检查标题是否在排除列表中
        """
        return title in self.excluded_titles

    def _is_url_excluded(self, url):
        """
        检查URL是否在排除列表中
        """
        return url in self.excluded_urls

    def _append_to_excluded_list(self, title):
        """
        将成功下载的视频标题追加到排除列表文件中
        """
        try:
            chigua_path = os.path.join(os.path.dirname(__file__), 'utils', '51chigua.txt')

            # 确保目录存在
            os.makedirs(os.path.dirname(chigua_path), exist_ok=True)

            # 追加标题到文件
            with open(chigua_path, 'a', encoding='utf-8') as f:
                f.write(f"\n{title}")

            # 同时添加到内存中的排除集合
            self.excluded_titles.add(title)

            self.log(f"✅ 已将标题 '{title}' 追加到排除列表文件")

        except Exception as e:
            self.log(f"❌ 追加标题到排除列表失败: {title}, 错误: {e}")

    def _append_to_excluded_urls(self, url):
        """
        将成功下载的视频URL追加到排除列表文件中
        """
        try:
            urls_path = os.path.join(os.path.dirname(__file__), 'utils', 'm3u8_urls.txt')

            # 确保目录存在
            os.makedirs(os.path.dirname(urls_path), exist_ok=True)

            # 追加URL到文件
            with open(urls_path, 'a', encoding='utf-8') as f:
                f.write(f"{url}\n")

            # 同时添加到内存中的排除集合
            self.excluded_urls.add(url)

            self.log(f"✅ 已将URL追加到排除列表文件")

        except Exception as e:
            self.log(f"❌ 追加URL到排除列表失败: {url}, 错误: {e}")

    def _get_temp_file_path(self, title):
        """
        获取临时文件路径
        """
        cleaned_title = self._clean_filename(title)
        temp_filename = f"{cleaned_title}_{int(time.time())}.tmp.mp4"
        return os.path.join(self.temp_store, temp_filename)

    def _get_final_file_path(self, item, title):
        """
        获取最终文件路径
        """
        dir_path = self.get_directory_path(item)
        cleaned_title = self._clean_filename(title)
        final_filename = f"{cleaned_title}.mp4"
        return os.path.join(dir_path, final_filename)

    def _clean_filename(self, filename):
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

        # 限制文件名长度
        if len(cleaned) > 100:
            cleaned = cleaned[:100]

        return cleaned

    def get_directory_path(self, item):
        """
        返回视频文件的存储目录路径
        """
        dir_path = os.path.join(self.videos_store, item.get('site', 'default'))
        return dir_path

    def close_spider(self, spider):
        """
        爬虫关闭时等待所有下载任务完成
        """
        self.log("🔄 爬虫即将关闭，等待所有M3U8下载任务完成...")

        # 显示关闭时的进度报告
        self._show_progress_report()

        with self.lock:
            if self.active_downloads == 0:
                self.log("✅ 没有活动的下载任务，直接关闭爬虫")
                self._show_final_statistics()
                self._cleanup_resources()
                return

            self.log(f"⏳ 等待 {self.active_downloads} 个下载任务完成...")

        # 等待所有下载完成（最多等待2小时）
        start_time = time.time()
        while self.active_downloads > 0 and (time.time() - start_time) < 7200:
            time.sleep(5)
            # 每30秒显示一次等待状态
            if int(time.time() - start_time) % 30 == 0:
                self.log(f"⏳ 仍在等待 {self.active_downloads} 个下载任务完成... (已等待 {int(time.time() - start_time)}秒)")

        # 显示最终统计
        self._show_final_statistics()
        self._cleanup_resources()
        self.log("🎉 所有M3U8下载任务已完成，爬虫可以安全关闭")

    def _show_final_statistics(self):
        """
        显示最终的下载统计报告
        """
        stats = self._get_download_statistics()
        runtime = time.time() - stats['start_time']
        hours, remainder = divmod(runtime, 3600)
        minutes, seconds = divmod(remainder, 60)
        runtime_str = f"{int(hours)}h{int(minutes)}m{int(seconds)}s"
        
        total_attempts = stats['download_success'] + stats['download_failed']
        success_rate = (stats['download_success'] / total_attempts * 100) if total_attempts > 0 else 0
        
        # 计算平均下载速度
        avg_speed = stats['download_success'] / (runtime / 60) if runtime > 0 else 0  # 每分钟成功下载数
        
        self.log("🎊" * 40)
        self.log("🏁 M3U8下载管道最终统计报告")
        self.log("🎊" * 40)
        self.log(f"⏱️  总运行时间: {runtime_str}")
        self.log(f"📊 处理统计:")
        self.log(f"   📋 总接收数量: {stats['total_received']}")
        self.log(f"   ✅ 下载成功: {stats['download_success']}")
        self.log(f"   ❌ 下载失败: {stats['download_failed']}")
        self.log(f"   🔄 跳过重复: {stats['skipped_duplicate']}")
        self.log(f"   ⏳ 剩余排队: {stats['queued_downloads']}")
        self.log(f"📈 成功率: {success_rate:.1f}%")
        self.log(f"⚡ 平均速度: {avg_speed:.1f} 视频/分钟")
        self.log(f"📁 存储位置: {self.videos_store}")
        self.log("🎊" * 40)

    def _cleanup_resources(self):
        """
        清理资源
        """
        try:
            # 关闭线程池
            if hasattr(self, 'download_executor'):
                self.download_executor.shutdown(wait=False)
                self.log("🧹 下载线程池已关闭")

        except Exception as e:
            self.log(f"❌ 关闭资源时出错: {e}")

    def _progress_monitor(self):
        """
        进度监控线程，定时显示下载状态
        """
        last_report_time = time.time()
        report_interval = 30  # 每30秒报告一次进度
        
        while True:
            try:
                time.sleep(5)  # 每5秒检查一次
                current_time = time.time()
                
                # 每30秒或有活动下载时显示详细进度
                if (current_time - last_report_time) >= report_interval or self.active_downloads > 0:
                    self._show_progress_report()
                    last_report_time = current_time
                    
                # 如果没有活动下载，延长检查间隔
                if self.active_downloads == 0:
                    time.sleep(25)  # 总共30秒间隔
                    
            except Exception as e:
                self.log(f"进度监控异常: {e}")

    def _show_progress_report(self):
        """
        显示详细的进度报告
        """
        # 使用_get_download_statistics函数获取统计信息
        stats = self._get_download_statistics()
        
        with self.lock:
            current_downloads_info = self.current_downloads.copy()
        
        # 计算运行时间
        runtime = time.time() - stats['start_time']
        hours, remainder = divmod(runtime, 3600)
        minutes, seconds = divmod(remainder, 60)
        runtime_str = f"{int(hours)}h{int(minutes)}m{int(seconds)}s"
        
        # 计算成功率
        total_attempts = stats['download_success'] + stats['download_failed']
        success_rate = (stats['download_success'] / total_attempts * 100) if total_attempts > 0 else 0
        
        # 显示总体统计
        self.log("=" * 80)
        self.log(f"📊 下载进度报告 - 运行时间: {runtime_str}")
        self.log(f"📋 总接收: {stats['total_received']} | ✅ 成功: {stats['download_success']} | ❌ 失败: {stats['download_failed']} | 🔄 跳过: {stats['skipped_duplicate']} | ⏳ 排队: {stats['queued_downloads']}")
        self.log(f"📈 成功率: {success_rate:.1f}% | 🏃 活动下载: {stats['active_downloads']}/{self.max_concurrent_downloads}")
        
        # 显示当前正在下载的视频
        if current_downloads_info:
            self.log("🎬 当前下载中:")
            for i, (url, info) in enumerate(current_downloads_info.items(), 1):
                download_time = time.time() - info['start_time']
                self.log(f"   {i}. {info['title']} (已进行 {download_time:.0f}秒)")
        else:
            self.log("😴 当前无下载任务")
        
        self.log("=" * 80)

    def _get_download_statistics(self):
        """
        获取下载统计信息
        """
        with self.lock:
            stats = self.download_stats.copy()
            stats['active_downloads'] = self.active_downloads
            stats['current_downloads'] = len(self.current_downloads)
            
        return stats
