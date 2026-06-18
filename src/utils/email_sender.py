#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
邮件发送工具，支持异步发送邮件通知。
"""
import asyncio
import datetime
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from config.env_loader import EMAIL_CONFIG
from src.utils.log import get_logger

logger = get_logger(__name__)


class EmailSender:
    """邮件发送器"""

    def __init__(self, sender: Optional[str] = None, password: Optional[str] = None,
                 receiver: Optional[str] = None):
        """
        初始化邮件发送器。

        Args:
            sender: 发送者邮箱，如果为 None 则使用 .env 配置
            password: 发送者邮箱密码/授权码，如果为 None 则使用 .env 配置
            receiver: 接收者邮箱，如果为 None 则使用 .env 配置
        """
        config = EMAIL_CONFIG
        self.sender = sender or config.get('sender', '')
        self.password = password or config.get('password', '')
        self.receiver = receiver or config.get('receiver', '')

        if not self.sender or not self.password or not self.receiver:
            raise ValueError("邮件配置不完整，请检查 .env 文件中的 EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVER 配置")

        self.logger = logger

    async def send_email_async(self, subject: str, content: str,
                               content_type: str = 'plain') -> bool:
        """
        异步发送邮件。

        Args:
            subject: 邮件主题
            content: 邮件内容
            content_type: 内容类型，'plain' 或 'html'

        Returns:
            bool: 发送是否成功
        """
        try:
            # 使用线程池执行同步的 SMTP 操作
            success = await asyncio.to_thread(
                self._send_email_sync, subject, content, content_type
            )
            return success
        except Exception as e:
            self.logger.error(f"发送邮件失败: {e}")
            return False

    def _send_email_sync(self, subject: str, content: str, content_type: str) -> bool:
        """
        同步发送邮件（内部方法，不要在异步代码中直接调用）。

        Args:
            subject: 邮件主题
            content: 邮件内容
            content_type: 内容类型

        Returns:
            bool: 发送是否成功
        """
        try:
            # 创建邮件
            msg = MIMEMultipart()
            msg['From'] = self.sender
            msg['To'] = self.receiver
            msg['Subject'] = subject

            # 添加邮件正文
            msg.attach(MIMEText(content, content_type, 'utf-8'))

            # 连接 SMTP 服务器并发送
            # QQ 邮箱 SMTP 服务器配置
            smtp_server = 'smtp.qq.com'
            smtp_port = 587  # 使用 TLS

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()  # 启用 TLS 加密
                server.login(self.sender, self.password)
                server.send_message(msg)

            self.logger.info(f"邮件发送成功: {subject}")
            return True

        except smtplib.SMTPException as e:
            self.logger.error(f"SMTP 错误: {e}")
            return False
        except Exception as e:
            self.logger.error(f"发送邮件时发生未知错误: {e}")
            return False

    async def send_trading_signal(self, symbol: str, signal_type: str,
                                  price: float, details: str) -> bool:
        """
        发送交易信号邮件。

        Args:
            symbol: 交易对符号
            signal_type: 信号类型，如 '抄底机会', '突破信号' 等
            price: 当前价格
            details: 详细信息

        Returns:
            bool: 发送是否成功
        """
        subject = f"🚨 交易信号 | {symbol} | {signal_type}"

        content = f"""
        <h2>📈 交易信号通知</h2>

        <p><strong>交易对:</strong> {symbol}</p>
        <p><strong>信号类型:</strong> {signal_type}</p>
        <p><strong>当前价格:</strong> {price:.2f}</p>
        <p><strong>检测时间:</strong> {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

        <h3>📊 详细信息:</h3>
        <pre>{details}</pre>

        <hr>
        <p style="color: #666; font-size: 12px;">
        此邮件由 Momentum1.66 量化系统自动发送，请勿直接回复。
        </p>
        """

        return await self.send_email_async(subject, content, 'html')

    async def send_file_async(self, file_path: str, subject: Optional[str] = None,
                              content: Optional[str] = None) -> bool:
        """异步发送带附件的邮件。"""
        try:
            success = await asyncio.to_thread(
                self._send_file_sync, file_path, subject, content
            )
            return success
        except Exception as e:
            self.logger.error(f"发送附件邮件失败: {e}")
            return False

    def _send_file_sync(self, file_path: str, subject: Optional[str],
                        content: Optional[str]) -> bool:
        """同步发送带附件的邮件（内部方法）。"""
        try:
            import os

            if not os.path.isfile(file_path):
                self.logger.error(f"附件文件不存在或不是文件: {file_path}")
                return False

            file_name = os.path.basename(file_path)
            mail_subject = subject or f"文件附件 - {file_name}"
            mail_content = content or "请查收附件。"

            msg = MIMEMultipart()
            msg['From'] = self.sender
            msg['To'] = self.receiver
            msg['Subject'] = mail_subject
            msg.attach(MIMEText(mail_content, 'plain', 'utf-8'))

            with open(file_path, 'rb') as attachment_file:
                attachment = MIMEBase('application', 'octet-stream')
                attachment.set_payload(attachment_file.read())

            encoders.encode_base64(attachment)
            attachment.add_header('Content-Disposition', f'attachment; filename="{file_name}"')
            msg.attach(attachment)

            smtp_server = 'smtp.qq.com'
            smtp_port = 587

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(self.sender, self.password)
                server.send_message(msg)

            self.logger.info(f"附件邮件发送成功: {mail_subject}")
            return True
        except smtplib.SMTPException as e:
            self.logger.error(f"SMTP 错误: {e}")
            return False
        except Exception as e:
            self.logger.error(f"发送附件邮件时发生未知错误: {e}")
            return False


# 提供便捷的全局函数
async def send_email(subject: str, content: str, content_type: str = 'plain') -> bool:
    """
    便捷函数：发送邮件。

    Args:
        subject: 邮件主题
        content: 邮件内容
        content_type: 内容类型

    Returns:
        bool: 发送是否成功
    """
    try:
        sender = EmailSender()
        return await sender.send_email_async(subject, content, content_type)
    except Exception as e:
        logger.error(f"发送邮件失败: {e}")
        return False
