#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""
视频下载进度监控 - Streamlit Web界面
实时监控M3U8视频下载状态和进度
"""

import os
import sys
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

# 添加父目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

# 导入数据库管理器
try:
    from fuliji.database import DownloadDatabase
except ImportError:
    try:
        sys.path.insert(0, os.path.join(parent_dir, 'fuliji'))
        from database import DownloadDatabase
    except ImportError as e:
        st.error(f"❌ 无法导入数据库模块: {e}")
        st.stop()


class DownloadMonitor:
    def __init__(self):
        # 获取数据库路径
        home_path = os.path.expanduser("~")
        self.videos_store = os.path.join(home_path, "Downloads", "Videos")
        self.temp_store = os.path.join(os.path.dirname(self.videos_store), 'temp_downloads')
        self.db_path = os.path.join(os.path.dirname(self.videos_store), 'downloads.db')

        # 初始化数据库
        try:
            self.db = DownloadDatabase(self.db_path)
        except Exception as e:
            st.error(f"❌ 数据库初始化失败: {e}")
            st.stop()

    def get_download_stats(self):
        """获取下载统计信息"""
        stats = self.db.get_download_statistics()

        # 统计临时文件
        temp_files_count = 0
        temp_files_size = 0
        if os.path.exists(self.temp_store):
            temp_files = [f for f in os.listdir(self.temp_store) if f.endswith('.tmp.mp4')]
            temp_files_count = len(temp_files)
            for temp_file in temp_files:
                try:
                    temp_file_path = os.path.join(self.temp_store, temp_file)
                    temp_files_size += os.path.getsize(temp_file_path)
                except:
                    pass

        stats['temp_files_count'] = temp_files_count
        stats['temp_files_size_mb'] = temp_files_size / (1024 * 1024)

        return stats

    def get_recent_downloads(self, limit=100):
        """获取最近下载记录"""
        try:
            records = self.db.get_all_downloads(limit=limit)
            downloads_data = []

            for record in records:
                download_info = {
                    'id': record['id'],
                    'title': record['title'],
                    'site': record['site'] or 'Unknown',
                    'status': record['status'],
                    'created_at': record['created_at'],
                    'updated_at': record['updated_at'],
                    'file_size_mb': (record['file_size'] / (1024 * 1024)) if record['file_size'] else 0,
                    'file_exists': os.path.exists(record['file_path']) if record['file_path'] else False,
                    'file_path': record['file_path'] or 'N/A'
                }

                # 解析时间
                try:
                    if record['download_time']:
                        download_info['download_time'] = datetime.fromtimestamp(record['download_time'])
                    else:
                        download_info['download_time'] = None
                except:
                    download_info['download_time'] = None

                downloads_data.append(download_info)

            return downloads_data
        except Exception as e:
            st.error(f"获取下载记录失败: {e}")
            return []

    def get_temp_files_info(self):
        """获取临时文件信息"""
        if not os.path.exists(self.temp_store):
            return []

        temp_files_info = []
        temp_files = [f for f in os.listdir(self.temp_store) if f.endswith('.tmp.mp4')]

        for temp_file in temp_files:
            temp_file_path = os.path.join(self.temp_store, temp_file)
            try:
                file_size = os.path.getsize(temp_file_path)
                file_mtime = os.path.getmtime(temp_file_path)

                temp_files_info.append({
                    'filename': temp_file,
                    'size_mb': file_size / (1024 * 1024),
                    'modified_time': datetime.fromtimestamp(file_mtime),
                    'path': temp_file_path
                })
            except Exception as e:
                temp_files_info.append({
                    'filename': temp_file,
                    'size_mb': 0,
                    'modified_time': None,
                    'path': temp_file_path,
                    'error': str(e)
                })

        return temp_files_info


def main():
    st.set_page_config(
        page_title="M3U8下载监控",
        page_icon="📺",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.title("📺 M3U8视频下载监控")
    st.markdown("实时监控视频下载进度和状态")

    # 初始化监控器
    monitor = DownloadMonitor()

    # 侧边栏控制
    st.sidebar.header("⚙️ 控制面板")

    # 自动刷新控制
    auto_refresh = st.sidebar.checkbox("🔄 自动刷新", value=True)
    refresh_interval = st.sidebar.selectbox(
        "刷新间隔",
        [5, 10, 30, 60],
        index=1,
        format_func=lambda x: f"{x}秒"
    )

    # 手动刷新按钮
    if st.sidebar.button("🔄 立即刷新"):
        st.rerun()

    # 获取统计数据
    stats = monitor.get_download_stats()

    # 主要统计信息
    st.header("📊 下载统计")

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            label="📈 总记录数",
            value=stats.get('total', 0)
        )

    with col2:
        completed = stats.get('status_stats', {}).get('completed', 0)
        st.metric(
            label="✅ 已完成",
            value=completed
        )

    with col3:
        downloading = stats.get('status_stats', {}).get('downloading', 0)
        st.metric(
            label="🔄 下载中",
            value=downloading
        )

    with col4:
        failed = stats.get('status_stats', {}).get('failed', 0)
        error = stats.get('status_stats', {}).get('error', 0)
        st.metric(
            label="❌ 失败",
            value=failed + error
        )

    with col5:
        st.metric(
            label="💾 总大小",
            value=f"{stats.get('total_size_mb', 0):.1f} MB"
        )

    # 状态分布饼图
    if stats.get('status_stats'):
        st.header("📈 状态分布")

        status_data = stats['status_stats']
        status_emoji = {
            'completed': '✅ 已完成',
            'downloading': '🔄 下载中',
            'failed': '❌ 失败',
            'error': '💥 错误',
            'missing_file': '⚠️ 文件丢失',
            'pending': '⏳ 等待中'
        }

        # 创建饼图数据
        chart_data = pd.DataFrame([
            {'状态': status_emoji.get(status, status), '数量': count}
            for status, count in status_data.items()
        ])

        fig = px.pie(
            chart_data,
            values='数量',
            names='状态',
            title="下载状态分布"
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)

    # 最近下载记录
    st.header("📋 最近下载记录")

    # 过滤选项
    col1, col2, col3 = st.columns(3)
    with col1:
        status_filter = st.selectbox(
            "状态过滤",
            ['全部', 'completed', 'downloading', 'failed', 'error', 'missing_file', 'pending']
        )

    with col2:
        limit = st.selectbox("显示数量", [20, 50, 100, 200], index=1)

    with col3:
        show_file_exists = st.checkbox("显示文件状态", value=True)

    # 获取下载记录
    downloads = monitor.get_recent_downloads(limit=limit)

    if downloads:
        # 过滤数据
        if status_filter != '全部':
            downloads = [d for d in downloads if d['status'] == status_filter]

        # 创建DataFrame
        df_data = []
        for download in downloads:
            row = {
                'ID': download['id'],
                '标题': download['title'][:50] + ('...' if len(download['title']) > 50 else ''),
                '网站': download['site'],
                '状态': download['status'],
                '大小(MB)': f"{download['file_size_mb']:.1f}",
                '创建时间': download['created_at'],
                '更新时间': download['updated_at']
            }

            if show_file_exists:
                row['文件存在'] = '✅' if download['file_exists'] else '❌'

            df_data.append(row)

        df = pd.DataFrame(df_data)

        # 使用颜色编码状态
        def highlight_status(val):
            if val == 'completed':
                return 'background-color: #d4edda'
            elif val == 'downloading':
                return 'background-color: #d1ecf1'
            elif val in ['failed', 'error']:
                return 'background-color: #f8d7da'
            elif val == 'missing_file':
                return 'background-color: #fff3cd'
            return ''

        st.dataframe(
            df.style.map(highlight_status, subset=['状态']),
            use_container_width=True
        )
    else:
        st.info("📭 暂无下载记录")

    # 临时文件信息
    st.header("🔄 临时文件")

    temp_files = monitor.get_temp_files_info()

    if temp_files:
        col1, col2 = st.columns(2)

        with col1:
            st.metric(
                label="临时文件数量",
                value=len(temp_files)
            )

        with col2:
            total_temp_size = sum(f['size_mb'] for f in temp_files)
            st.metric(
                label="临时文件总大小",
                value=f"{total_temp_size:.1f} MB"
            )

        # 临时文件列表
        temp_df_data = []
        for temp_file in temp_files:
            temp_df_data.append({
                '文件名': temp_file['filename'],
                '大小(MB)': f"{temp_file['size_mb']:.1f}",
                '修改时间': temp_file['modified_time'].strftime('%Y-%m-%d %H:%M:%S') if temp_file[
                    'modified_time'] else 'N/A',
                '路径': temp_file['path']
            })

        temp_df = pd.DataFrame(temp_df_data)
        st.dataframe(temp_df, use_container_width=True)

        # 清理按钮
        if st.button("🗑️ 清理过期临时文件 (7天前)"):
            # 这里可以添加清理逻辑
            st.info("清理功能需要与后端集成")
    else:
        st.info("📭 暂无临时文件")

    # 系统信息
    st.header("🖥️ 系统信息")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.info(f"**数据库路径**\n{monitor.db_path}")

    with col2:
        st.info(f"**视频存储目录**\n{monitor.videos_store}")

    with col3:
        st.info(f"**临时文件目录**\n{monitor.temp_store}")

    # 自动刷新
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()
