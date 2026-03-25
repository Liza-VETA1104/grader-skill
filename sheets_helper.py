import gspread
from google.oauth2.service_account import Credentials
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def upload_to_sheets(stats: dict, credentials_file: str, spreadsheet_id: str, sheet_name: str = "Лист1") -> bool:
    """
    Загружает полную статистику в Google Sheets.
    
    Ожидаемые колонки в таблице (по порядку):
    date | total_attempts | successful_attempts | failed_attempts | 
    unique_users | success_rate | users_who_failed_rate | 
    avg_attempts_per_user | avg_attempts_to_success | 
    avg_time_between_attempts | peak_hour
    """
    try:
        # Авторизация
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
        client = gspread.authorize(creds)
        
        # Открываем таблицу
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
        
        # 🔥 Формируем строку с ВСЕМИ метриками (в нужном порядке!)
        row = [
            stats.get('date', datetime.now().strftime('%Y-%m-%d')),           # date
            stats.get('total_attempts', 0),                                    # total_attempts
            stats.get('successful_attempts', 0),                               # successful_attempts
            stats.get('failed_attempts', 0),                                   # failed_attempts
            stats.get('unique_users', 0),                                      # unique_users
            f"{stats.get('success_rate', 0)}%",                                # success_rate
            f"{stats.get('users_who_failed_rate', 0)}%",                       # users_who_failed_rate
            stats.get('avg_attempts_per_user', 0),                             # avg_attempts_per_user
            stats.get('avg_attempts_to_success', 0),                           # avg_attempts_to_success
            f"{stats.get('avg_time_between_attempts', 0)} сек",                # avg_time_between_attempts
            f"{stats.get('peak_hour', 'N/A'):02d}:00" if isinstance(stats.get('peak_hour'), int) else 'N/A'  # peak_hour
        ]
        
        # Добавляем строку
        sheet.append_row(row)
        logger.info(f"✅ Статистика загружена в Google Sheets")
        return True
        
    except FileNotFoundError:
        logger.error(f"❌ Файл с ключом не найден: {credentials_file}")
        return False
    except gspread.exceptions.APIError as e:
        logger.error(f"❌ Ошибка Google API: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки в Google Sheets: {type(e).__name__}: {e}")
        return False