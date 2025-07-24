#!/usr/bin/env python
# -*-coding:utf-8 -*-
"""
SQLite数据库管理器 - 用于管理M3U8视频下载记录
"""

import hashlib
import logging
import os
import sqlite3
import time
from contextlib import contextmanager


class DownloadDatabase:
    """下载记录数据库管理器"""

    def __init__(self, db_path):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """初始化数据库表"""
        try:
            # 确保数据库目录存在
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir)

            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 创建下载记录表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS downloads (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url_hash TEXT UNIQUE NOT NULL,
                        url TEXT NOT NULL,
                        title TEXT NOT NULL,
                        site TEXT,
                        file_path TEXT,
                        temp_file_path TEXT,
                        status TEXT DEFAULT 'pending',
                        download_time REAL,
                        file_size REAL,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                # 创建索引
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_url_hash ON downloads(url_hash)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON downloads(status)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_site ON downloads(site)')

                conn.commit()
                logging.info(f"数据库初始化完成: {self.db_path}")

        except Exception as e:
            logging.error(f"数据库初始化失败: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            conn.row_factory = sqlite3.Row  # 使结果可以通过列名访问
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()

    @staticmethod
    def get_url_hash(url):
        """生成URL的唯一哈希标识"""
        return hashlib.md5(url.encode('utf-8')).hexdigest()

    def is_download_completed(self, url):
        """检查下载是否已完成"""
        url_hash = self.get_url_hash(url)

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT file_path, status FROM downloads 
                    WHERE url_hash = ? AND status = 'completed'
                ''', (url_hash,))

                result = cursor.fetchone()
                if result:
                    file_path = result['file_path']
                    # 检查文件是否真的存在
                    if file_path and os.path.exists(file_path):
                        return True, file_path
                    else:
                        # 文件不存在，更新状态为失败
                        self.update_download_status(url, 'missing_file')
                        return False, None

                return False, None

        except Exception as e:
            logging.error(f"检查下载状态失败: {e}")
            return False, None

    def get_temp_file_path(self, url):
        """获取临时文件路径"""
        url_hash = self.get_url_hash(url)

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT temp_file_path FROM downloads 
                    WHERE url_hash = ?
                ''', (url_hash,))

                result = cursor.fetchone()
                return result['temp_file_path'] if result else None

        except Exception as e:
            logging.error(f"获取临时文件路径失败: {e}")
            return None

    def add_download_record(self, url, title, site, temp_file_path):
        """添加下载记录"""
        url_hash = self.get_url_hash(url)

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO downloads 
                    (url_hash, url, title, site, temp_file_path, status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, 'downloading', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ''', (url_hash, url, title, site, temp_file_path))

                conn.commit()
                logging.info(f"添加下载记录: {title}")
                return cursor.lastrowid

        except Exception as e:
            logging.error(f"添加下载记录失败: {e}")
            return None

    def update_download_status(self, url, status, file_path=None, file_size=None):
        """更新下载状态"""
        url_hash = self.get_url_hash(url)

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                update_fields = ['status = ?', 'updated_at = CURRENT_TIMESTAMP']
                params = [status]

                if file_path:
                    update_fields.append('file_path = ?')
                    params.append(file_path)

                if file_size is not None:
                    update_fields.append('file_size = ?')
                    params.append(file_size)

                if status == 'completed':
                    update_fields.append('download_time = ?')
                    params.append(time.time())

                params.append(url_hash)

                sql = f'''
                    UPDATE downloads 
                    SET {', '.join(update_fields)}
                    WHERE url_hash = ?
                '''

                cursor.execute(sql, params)
                conn.commit()

                if cursor.rowcount > 0:
                    logging.info(f"更新下载状态: {status}")
                    return True
                else:
                    logging.warning(f"未找到要更新的记录: {url_hash}")
                    return False

        except Exception as e:
            logging.error(f"更新下载状态失败: {e}")
            return False

    def get_download_statistics(self):
        """获取下载统计信息"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 总记录数
                cursor.execute('SELECT COUNT(*) as total FROM downloads')
                total = cursor.fetchone()['total']

                # 按状态统计
                cursor.execute('''
                    SELECT status, COUNT(*) as count 
                    FROM downloads 
                    GROUP BY status
                ''')
                status_stats = {row['status']: row['count'] for row in cursor.fetchall()}

                # 完成的文件中实际存在的文件数
                cursor.execute('''
                    SELECT COUNT(*) as valid_files
                    FROM downloads 
                    WHERE status = 'completed' AND file_path IS NOT NULL
                ''')
                valid_files = cursor.fetchone()['valid_files']

                # 总文件大小
                cursor.execute('''
                    SELECT SUM(file_size) as total_size
                    FROM downloads 
                    WHERE status = 'completed' AND file_size IS NOT NULL
                ''')
                total_size = cursor.fetchone()['total_size'] or 0

                return {
                    'total': total,
                    'status_stats': status_stats,
                    'valid_files': valid_files,
                    'total_size_mb': total_size / (1024 * 1024) if total_size else 0
                }

        except Exception as e:
            logging.error(f"获取统计信息失败: {e}")
            return {}

    def get_all_downloads(self, limit=None, offset=0):
        """获取所有下载记录"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                sql = '''
                    SELECT * FROM downloads 
                    ORDER BY created_at DESC
                '''

                if limit:
                    sql += f' LIMIT {limit} OFFSET {offset}'

                cursor.execute(sql)
                return cursor.fetchall()

        except Exception as e:
            logging.error(f"获取下载记录失败: {e}")
            return []

    def get_downloads_by_status(self, status):
        """根据状态获取下载记录"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM downloads 
                    WHERE status = ?
                    ORDER BY created_at DESC
                ''', (status,))

                return cursor.fetchall()

        except Exception as e:
            logging.error(f"根据状态获取下载记录失败: {e}")
            return []

    def cleanup_missing_files(self):
        """清理文件已丢失的记录"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 获取所有已完成的记录
                cursor.execute('''
                    SELECT id, title, file_path FROM downloads 
                    WHERE status = 'completed' AND file_path IS NOT NULL
                ''')

                missing_files = []
                for row in cursor.fetchall():
                    if not os.path.exists(row['file_path']):
                        missing_files.append(row)

                # 更新丢失文件的状态
                if missing_files:
                    for record in missing_files:
                        cursor.execute('''
                            UPDATE downloads 
                            SET status = 'missing_file', updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        ''', (record['id'],))
                        logging.info(f"标记文件丢失: {record['title']}")

                    conn.commit()
                    return len(missing_files)

                return 0

        except Exception as e:
            logging.error(f"清理丢失文件记录失败: {e}")
            return 0

    def reset_all_records(self):
        """重置所有下载记录"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM downloads')
                conn.commit()

                deleted_count = cursor.rowcount
                logging.info(f"已删除 {deleted_count} 条下载记录")
                return deleted_count

        except Exception as e:
            logging.error(f"重置下载记录失败: {e}")
            return 0

    def close(self):
        """关闭数据库连接（实际上这里不需要做什么，因为我们使用上下文管理器）"""
        pass
