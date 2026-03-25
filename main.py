import psycopg2
import os
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from collections import Counter, defaultdict
from logger import setup_logger
from api_client import fetch_data
from data_processor import parse_passback_params, validate_record

# Инициализируем логгер (ТОЛЬКО ЗДЕСЬ!)
logger = setup_logger()
load_dotenv()

DB_PARAMS = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}


def convert_is_correct(value):
    """Преобразует is_correct в boolean или None"""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes')
    return None


def convert_created_at(value):
    """Преобразует строку даты в datetime объект"""
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    
    formats = [
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    
    logger.warning(f"Не удалось распарсить дату: {value}")
    return None


def load_to_database(records: list) -> int:
    """Загружает обработанные записи в PostgreSQL"""
    if not records:
        logger.warning("⚠️ Нет записей для загрузки")
        return 0
    
    loaded = 0
    errors = 0
    
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            for idx, record in enumerate(records):
                try:
                    # 🔥 КОНВЕРТАЦИЯ данных
                    is_correct_value = convert_is_correct(record['is_correct'])
                    created_at_value = convert_created_at(record['created_at'])
                    
                    passback = record.get('passback_parsed', {})
                    
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO grader_attempts 
                            (user_id, oauth_consumer_key, lis_result_sourcedid, 
                             lis_outcome_service_url, is_correct, attempt_type, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            record['lti_user_id'],
                            passback['oauth_consumer_key'],
                            passback['lis_result_sourcedid'],
                            passback['lis_outcome_service_url'],
                            is_correct_value,
                            record['attempt_type'],
                            created_at_value
                        ))
                        conn.commit()
                        loaded += 1
                        
                except psycopg2.Error as e:
                    conn.rollback()
                    errors += 1
                    if errors <= 5:
                        logger.warning(f"⚠️ Ошибка при загрузке записи #{idx}: {e}")
                    elif errors == 6:
                        logger.warning("⚠️ ... и другие ошибки (не показываем все)")
                        
        logger.info(f"✅ Загружено записей: {loaded}, ошибок: {errors}")
        return loaded
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка подключения к БД: {e}")
        return 0


def calculate_statistics(valid_records: list) -> dict:
    """
    Рассчитывает статистику по валидным записям.
    Использует convert_is_correct() для единообразия.
    Возвращает словарь с метриками для лога и Google Sheets.
    """
    if not valid_records:
        return {}
    
    total = len(valid_records)
    
    # 🔥 Базовые метрики (используем convert_is_correct)
    successful = sum(1 for r in valid_records if convert_is_correct(r['is_correct']) is True)
    failed = sum(1 for r in valid_records if convert_is_correct(r['is_correct']) is False)
    unique_users = len(set(r['lti_user_id'] for r in valid_records))
    success_rate = round((successful / total * 100), 2) if total > 0 else 0
    
    # 🔥 Пиковый час
    hours = []
    for r in valid_records:
        dt = convert_created_at(r['created_at'])
        if dt:
            hours.append(dt.hour)
    
    peak_hour = None
    peak_hour_count = 0
    if hours:
        # ✅ Counter уже импортирован в начале файла (не импортируем внутри!)
        hour_counts = Counter(hours)
        peak_hour = max(hour_counts.keys(), key=lambda h: hour_counts[h])
        peak_hour_count = hour_counts[peak_hour]
    
    # 🔥 Среднее попыток на пользователя
    user_attempts = Counter(r['lti_user_id'] for r in valid_records)
    avg_attempts_per_user = round(total / unique_users, 2) if unique_users > 0 else 0
    
    # 🔥 Попытки до успеха
    user_data = defaultdict(lambda: {'attempts': 0, 'has_success': False})
    for r in valid_records:
        user_id = r['lti_user_id']
        user_data[user_id]['attempts'] += 1
        if convert_is_correct(r['is_correct']) is True:
            user_data[user_id]['has_success'] = True
    
    successful_users = [data['attempts'] for data in user_data.values() if data['has_success']]
    avg_attempts_to_success = (
        round(sum(successful_users) / len(successful_users), 2)
        if successful_users else 0
    )
    
    # 🔥 Доля пользователей без успеха
    users_without_success = sum(1 for data in user_data.values() if not data['has_success'])
    users_who_failed_rate = (
        round(users_without_success / unique_users * 100, 2)
        if unique_users > 0 else 0
    )
    
    # 🔥 Среднее время между попытками
    user_timestamps = defaultdict(list)
    for r in valid_records:
        dt = convert_created_at(r['created_at'])
        if dt:
            user_timestamps[r['lti_user_id']].append(dt)
    
    time_diffs = []
    for timestamps in user_timestamps.values():
        if len(timestamps) > 1:
            timestamps.sort()
            for i in range(1, len(timestamps)):
                diff_seconds = (timestamps[i] - timestamps[i-1]).total_seconds()
                if 1 <= diff_seconds <= 3600:
                    time_diffs.append(diff_seconds)
    
    avg_time_between_attempts = (
        round(sum(time_diffs) / len(time_diffs), 2)
        if time_diffs else 0
    )
    
    # ✅ Возвращаем ВСЕ поля, включая peak_hour_count
    return {
        'date': datetime.now().strftime('%Y-%m-%d'),
        'total_attempts': total,
        'successful_attempts': successful,
        'failed_attempts': failed,
        'unique_users': unique_users,
        'success_rate': success_rate,
        'peak_hour': peak_hour,
        'peak_hour_count': peak_hour_count,  # ✅ Добавлено!
        'avg_attempts_per_user': avg_attempts_per_user,
        'avg_attempts_to_success': avg_attempts_to_success,
        'users_who_failed_rate': users_who_failed_rate,
        'avg_time_between_attempts': avg_time_between_attempts
    }


def log_statistics(stats: dict):
    """Выводит компактную статистику в лог"""
    if not stats or not stats.get('total_attempts'):
        return
    
    logger.info("📊 Статистика:")
    logger.info(f"   Всего попыток: {stats['total_attempts']}")
    logger.info(f"   Успешных: {stats['successful_attempts']}")
    logger.info(f"   Неуспешных: {stats['failed_attempts']}")
    logger.info(f"   Уникальных пользователей: {stats['unique_users']}")
    logger.info(f"   Процент успеха: {stats['success_rate']}%")
    logger.info(f"   Пользователей без успеха: {stats['users_who_failed_rate']}%")
    logger.info(f"   Среднее попыток на пользователя: {stats['avg_attempts_per_user']}")
    logger.info(f"   Среднее попыток до успеха: {stats['avg_attempts_to_success']}")
    logger.info(f"   Среднее время между попытками: {stats['avg_time_between_attempts']} сек")
    
    # ✅ Теперь peak_hour_count будет корректным
    peak_hour = stats.get('peak_hour')
    peak_count = stats.get('peak_hour_count', 0)
    if peak_hour is not None:
        logger.info(f"   Пиковый час: {peak_hour:02d}:00 ({peak_count} попыток)")

def main():
    logger.info("🚀 Запуск основного скрипта")
    
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=24)
    
    # 1. Получаем данные из API
    raw_data = fetch_data(
        start_dt.strftime('%Y-%m-%d %H:%M:%S.%f'),
        end_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    )
    
    if not raw_data:
        logger.warning("⚠️ Данные не получены, завершаем")
        return
            
    # 2. Обрабатываем и валидируем
    valid_records = []
    skipped_records = []
    
    for idx, record in enumerate(raw_data):
        is_valid, error = validate_record(record)
        if is_valid:
            record['passback_parsed'] = parse_passback_params(record.get('passback_params', ''))
            valid_records.append(record)
        else:
            logger.warning(f"⚠️ Пропущена запись #{idx}: {error}")
            skipped_records.append(record)
    
    logger.info(f"✅ Валидных записей: {len(valid_records)} из {len(raw_data)}")
    
    # 3. Сохраняем пропущенные записи
    with open('skipped_records.json', 'w', encoding='utf-8') as f:
        json.dump(skipped_records, f, ensure_ascii=False, indent=2)
    logger.info(f"💾 Пропущенные записи сохранены в skipped_records.json ({len(skipped_records)} шт.)")
    
    # 4. Загружаем в БД
    load_to_database(valid_records)
    
    # 5. 🔥 Рассчитываем и логируем статистику
    stats = calculate_statistics(valid_records)
    log_statistics(stats)
    
    # 6. Загружаем в Google Sheets
    if stats:
        from sheets_helper import upload_to_sheets
        
        upload_to_sheets(
            stats=stats,
            credentials_file='grader-analytics-5083d7745be8.json',
            spreadsheet_id='1-3LFWxDjmicXsWSZxxOjaYclEW6jooTbFm2LSuqT4eo',
            sheet_name='Лист1'
        )

    # 7. Отправляем email с отчётом
    if stats:
        from email_helper import send_email_report
        
        send_email_report(
            stats=stats,
            subject=f"📊 Грайдер-скилл: {stats.get('date', 'N/A')}"
        )
    
    logger.info("🏁 Скрипт завершил работу")


if __name__ == "__main__":
    main()