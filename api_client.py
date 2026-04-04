import requests
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Параметры API из .env
API_URL = os.getenv("API_URL")
CLIENT = os.getenv("CLIENT")
CLIENT_KEY = os.getenv("CLIENT_KEY")

# Проверка, что переменные загрузились
if not API_URL or not CLIENT or not CLIENT_KEY:
    logger.error("❌ Ошибка: не загружены переменные окружения (API_URL, CLIENT, CLIENT_KEY)")
    logger.error("   Проверьте файл .env")


def fetch_data(start_date: str, end_date: str) -> list:
    """
    Получает данные из API.
    """
    logger.info(f"📥 Начало загрузки данных: {start_date} → {end_date}")
    
    params = {
        'client': CLIENT,
        'client_key': CLIENT_KEY,
        'start': start_date,
        'end': end_date
    }
    
    try:
        response = requests.get(API_URL, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()
        logger.info(f"✅ Получено записей: {len(data)}")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка запроса к API: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Status code: {e.response.status_code}")
        return []
        
