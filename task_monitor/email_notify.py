#!/usr/bin/env python3
from __future__ import annotations

import mimetypes
import os
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import Iterable, Sequence
from dotenv import load_dotenv


ENV_FILE = (Path(__file__).parent / ".env").resolve()
load_dotenv(ENV_FILE)


def load_email_config_from_env() -> dict[str, str | int]:
    """
    从环境变量读取邮件配置。
    变量名可在 .env 中配置（占位符如下）：
    - MAIL_HOST
    - MAIL_PORT
    - MAIL_USER
    - MAIL_PASSWORD
    - MAIL_SENDER
    """
    return {
        "host": os.getenv("MAIL_HOST", "<MAIL_HOST>"),
        "port": int(os.getenv("MAIL_PORT", "465")),
        "user": os.getenv("MAIL_USER", "<MAIL_USER>"),
        "password": os.getenv("MAIL_PASSWORD", "<MAIL_PASSWORD>"),
        "sender": os.getenv("MAIL_SENDER", "<MAIL_SENDER>"),
    }


def send_notify_email(
    subject: str,
    body: str,
    recipients: Sequence[str],
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    sender: str,
    attachments: Iterable[str | Path] | None = None,
) -> None:
    """
    发送通知邮件（可用于流程启动前和执行完毕后）。

    参数:
    - subject: 邮件主题
    - body: 邮件正文（纯文本）
    - recipients: 收件人列表
    - smtp_host/smtp_port/smtp_user/smtp_password/sender: SMTP 配置（可由 .env 读取后传入）
    - attachments: 附件路径列表，默认为 None（无附件）
    """
    if not recipients:
        raise ValueError("recipients 不能为空")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    for item in attachments or []:
        file_path = Path(item)
        if not file_path.exists() or not file_path.is_file():
            raise FileNotFoundError(f"附件不存在: {file_path}")
        mime_type, _ = mimetypes.guess_type(str(file_path))
        if mime_type:
            maintype, subtype = mime_type.split("/", 1)
        else:
            maintype, subtype = "application", "octet-stream"
        msg.add_attachment(
            file_path.read_bytes(),
            maintype=maintype,
            subtype=subtype,
            filename=file_path.name,
        )

    with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
