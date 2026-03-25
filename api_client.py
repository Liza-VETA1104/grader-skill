import requests
import os
from dotenv import load_dotenv
from logger import setup_logger

logger = setup_logger()
load_dotenv()

# Параметры API
API_URL = "https://b2b.itresume.ru/api/statistics"
CLIENT = "Skillfactory"
CLIENT_KEY = "M2MGWS"

def fetch_data(start_date: str, end_date: str):
    """
    Получает данные из API
    start_date, end_date: строки в формате 'YYYY-MM-DD HH:MM:SS.ffffff'
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

# Тестовый запуск
if __name__ == "__main__":
    from datetime import datetime, timedelta
    
    # Берём данные за последние 24 часа
    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(hours=1)
    
    data = fetch_data(
        start_dt.strftime('%Y-%m-%d %H:%M:%S.%f'),
        end_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    )
    
    if data:
        logger.info(f"📄 Первая запись: {data[0]}")
    else:
        logger.warning("⚠️ Данные не получены")

        # Тестируем парсинг
    if data:
        first_record = data[0]
        logger.info(f"📄 Исходный passback_params: {first_record.get('passback_params')[:100]}...")
        
        from data_processor import parse_passback_params
        parsed = parse_passback_params(first_record.get('passback_params', ''))
        
        logger.info(f"✅ Распарсено: oauth_consumer_key='{parsed.get('oauth_consumer_key')}'")
        logger.info(f"✅ lis_result_sourcedid='{parsed.get('lis_result_sourcedid')[:50]}...'")    