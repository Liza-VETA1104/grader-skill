import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import os

logger = logging.getLogger(__name__)
load_dotenv()


def send_email_report(stats: dict, subject: str = "📊 Отчёт грйдер-скилл") -> bool:
    """
    Отправляет отчёт со статистикой на email.
    
    Args:
        stats: Словарь со статистикой
        subject: Тема письма
    
    Returns:
        bool: True если успешно, False если ошибка
    """
    try:
        # Получаем настройки из .env
        smtp_server = os.getenv('SMTP_SERVER')
        smtp_port = int(os.getenv('SMTP_PORT', 587))
        smtp_user = os.getenv('SMTP_USER')
        smtp_password = os.getenv('SMTP_PASSWORD')
        email_to = os.getenv('EMAIL_TO')
        
        # Проверяем, что все настройки есть
        if not all([smtp_server, smtp_user, smtp_password, email_to]):
            logger.error("❌ Не все настройки почты заполнены в .env")
            return False
        
        # Формируем тело письма
        body = f"""
📊 Отчёт грйдер-скилл
Дата: {stats.get('date', 'N/A')}

═══════════════════════════════════════

📈 Основная статистика:
   • Всего попыток: {stats.get('total_attempts', 0)}
   • Успешных: {stats.get('successful_attempts', 0)}
   • Неуспешных: {stats.get('failed_attempts', 0)}
   • Уникальных пользователей: {stats.get('unique_users', 0)}
   • Процент успеха: {stats.get('success_rate', 0)}%

📈 Дополнительные метрики:
   • Пользователей без успеха: {stats.get('users_who_failed_rate', 0)}%
   • Среднее попыток на пользователя: {stats.get('avg_attempts_per_user', 0)}
   • Среднее попыток до успеха: {stats.get('avg_attempts_to_success', 0)}
   • Среднее время между попытками: {stats.get('avg_time_between_attempts', 0)} сек
   • Пиковый час: {stats.get('peak_hour', 'N/A')}:00

═══════════════════════════════════════

Автоматическое уведомление от грйдер-скилл
        """
        
        # Создаём письмо
        msg = MIMEMultipart()
        msg['From'] = smtp_user
        msg['To'] = email_to
        msg['Subject'] = subject
        
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        
        # Отправляем
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        
        logger.info(f"✅ Email отправлен на {email_to}")
        return True
        
    except smtplib.SMTPAuthenticationError:
        logger.error("❌ Ошибка аутентификации SMTP (проверьте логин/пароль)")
        return False
    except smtplib.SMTPException as e:
        logger.error(f"❌ Ошибка SMTP: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка отправки email: {type(e).__name__}: {e}")
        return False