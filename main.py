import psycopg2
import os
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from collections import Counter, defaultdict
from logger import setup_logger
from api_client import fetch_data
from data_processor import parse_passback_params, validate_record

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
    """Преобразование is_correct в boolean или None"""
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
    """Преобразование даты в datetime"""
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
    """Загружает уже валидированные записи в БД (batch insert)"""
    if not records:
        logger.warning("⚠️ Нет записей для загрузки")
        return 0
    
    loaded = 0
    errors = 0
    duplicates_skipped = 0
    
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            batch_data = []
            
            for idx, record in enumerate(records):
                try:
                    is_correct_value = convert_is_correct(record['is_correct'])
                    created_at_value = convert_created_at(record['created_at'])
                    passback = record.get('passback_parsed', {})
                                                          
                    batch_data.append((
                        record['lti_user_id'].strip(),
                        passback['oauth_consumer_key'],
                        passback.get('lis_result_sourcedid'),
                        passback.get('lis_outcome_service_url'),
                        is_correct_value,
                        record.get('attempt_type'),
                        created_at_value
                    ))
                    
                except Exception as e:
                    errors += 1
                    logger.warning(f"⚠️ Ошибка подготовки записи #{idx}: {e}")
            
            # 🔥 Батчевая вставка
            if batch_data:
                with conn.cursor() as cur:
                    # Вариант 1: с ON CONFLICT (если индекс есть)
                    cur.executemany("""
                        INSERT INTO grader_attempts 
                        (user_id, oauth_consumer_key, lis_result_sourcedid, 
                         lis_outcome_service_url, is_correct, attempt_type, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (user_id, lis_result_sourcedid, created_at) 
                        DO NOTHING
                    """, batch_data)
                    
                    # Вариант 2: без ON CONFLICT (если индекса нет)
                    # cur.executemany("""INSERT ... VALUES (%s, ...)""", batch_data)
                    
                    loaded = cur.rowcount
                    duplicates_skipped = len(batch_data) - loaded
                    conn.commit()
            
            # 🔥 Логируем результат
            log_msg = f"✅ Загружено: {loaded}, ошибок: {errors}"
            if duplicates_skipped > 0:
                log_msg += f", пропущено дублей: {duplicates_skipped}"
            logger.info(log_msg)
            
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
    
    
    successful = sum(1 for r in valid_records if convert_is_correct(r['is_correct']) is True)
    failed = sum(1 for r in valid_records if convert_is_correct(r['is_correct']) is False)
    unique_users = len(set(r['lti_user_id'] for r in valid_records))
    success_rate = round((successful / total * 100), 2) if total > 0 else 0
    
    # Пиковый час
    hours = []
    for r in valid_records:
        dt = convert_created_at(r['created_at'])
        if dt:
            hours.append(dt.hour)
    
    peak_hour = None
    peak_hour_count = 0
    if hours:
        hour_counts = Counter(hours)
        peak_hour = max(hour_counts.keys(), key=lambda h: hour_counts[h])
        peak_hour_count = hour_counts[peak_hour]
    
    # Среднее кол-во попыток на пользователя
    user_attempts = Counter(r['lti_user_id'] for r in valid_records)
    avg_attempts_per_user = round(total / unique_users, 2) if unique_users > 0 else 0
    
    # Кол-во попыток до успеха
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
    
    # Доля пользователей без успеха
    users_without_success = sum(1 for data in user_data.values() if not data['has_success'])
    users_who_failed_rate = (
        round(users_without_success / unique_users * 100, 2)
        if unique_users > 0 else 0
    )
    
    # Среднее время между попытками
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
    
    avg_mins_between_attempts = (
        round(sum(time_diffs) / len(time_diffs) / 60, 1)
        if time_diffs else 0
    )
    
    
    return {
        'date': datetime.now().strftime('%Y-%m-%d'),
        'total_attempts': total,
        'successful_attempts': successful,
        'failed_attempts': failed,
        'unique_users': unique_users,
        'success_rate': success_rate,
        'peak_hour': peak_hour,
        'peak_hour_count': peak_hour_count,  
        'avg_attempts_per_user': avg_attempts_per_user,
        'avg_attempts_to_success': avg_attempts_to_success,
        'users_who_failed_rate': users_who_failed_rate,
        'avg_mins_between_attempts': avg_mins_between_attempts
    }


def log_statistics(stats: dict):
    """Выводит компактную статистику в лог"""
    if not stats or not stats.get('total_attempts'):
        return
    
    logger.info("Статистика:")
    logger.info(f"   Всего попыток: {stats['total_attempts']}")
    logger.info(f"   Успешных: {stats['successful_attempts']}")
    logger.info(f"   Неуспешных: {stats['failed_attempts']}")
    logger.info(f"   Уникальных пользователей: {stats['unique_users']}")
    logger.info(f"   Процент успеха: {stats['success_rate']}%")
    logger.info(f"   Пользователей без успеха: {stats['users_who_failed_rate']}%")
    logger.info(f"   Среднее кол-во попыток на пользователя: {stats['avg_attempts_per_user']}")
    logger.info(f"   Среднее кол-во попыток до успеха: {stats['avg_attempts_to_success']}")
    logger.info(f"   Среднее время между попытками: {stats['avg_mins_between_attempts']} мин")
    
    
    peak_hour = stats.get('peak_hour')
    peak_count = stats.get('peak_hour_count', 0)
    if peak_hour is not None:
        logger.info(f"   Пиковый час: {peak_hour:02d}:00 ({peak_count} попыток)")

def main():
    logger.info("Запуск основного скрипта")

    try:
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=24)
        
        # 1. Получение данных из API
        raw_data = fetch_data(
            start_dt.strftime('%Y-%m-%d %H:%M:%S.%f'),
            end_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        )
    except Exception as e:
        logger.error(f"❌ Ошибка получения данных: {e}")
        return

    
    if not raw_data:
        logger.warning("⚠️ Данные не получены")
        return
            
    # 2. Обработка и валидация
    
    valid_records = []
    skipped_records = []

    try:   
        for idx, record in enumerate(raw_data):
            # Проверка lti_user_id
            if not record.get('lti_user_id'):
                logger.warning(f"⚠️ Пропущена запись #{idx}: нет lti_user_id")
                skipped_records.append(record)
                continue
            
            # Проверка attempt_type
            attempt_type = record.get('attempt_type')
            if attempt_type not in ['run', 'submit']:
                logger.warning(f"⚠️ Пропущена запись #{idx}: неверный attempt_type='{attempt_type}'")
                skipped_records.append(record)
                continue
            
            # Проверка passback_params
            if not record.get('passback_params'):
                logger.warning(f"⚠️ Пропущена запись #{idx}: нет passback_params")
                skipped_records.append(record)
                continue
            
            # Парсинг passback
            passback = parse_passback_params(record.get('passback_params', ''))
            
            # Проверка, что распарсилось в словарь с обязательными полями
            if not isinstance(passback, dict):
                logger.warning(f"⚠️ Пропущена запись #{idx}: passback не является словарем (тип: {type(passback).__name__})")
                skipped_records.append(record)
                continue
            required = ['lis_result_sourcedid', 'lis_outcome_service_url']
            missing_fields = [field for field in required if not passback.get(field)]

            if missing_fields:
                logger.warning(f"⚠️ Пропущена запись #{idx}: отсутствуют обязательные поля passback: {missing_fields}")
                skipped_records.append(record)
                continue
            
            record['passback_parsed'] = passback
            valid_records.append(record)
            
    except Exception as e:
        logger.error(f"❌ Ошибка валидации: {e}")
        return

    logger.info(f"✅ Валидных записей: {len(valid_records)} из {len(raw_data)}")
    
    # 3. Сохранение пропущенных записей
    try: 
        with open('skipped_records.json', 'w', encoding='utf-8') as f:
            json.dump(skipped_records, f, ensure_ascii=False, indent=2)
        logger.info(f"💾 Пропущенные записи сохранены в skipped_records.json ({len(skipped_records)} шт.)")    
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения skipped_records: {e}")

        
    if not valid_records:
        logger.warning("⚠️ Нет валидных записей. Завершение.")
        return
    
    # 4. Загрузка в БД
   
    loaded = load_to_database(valid_records)
    if loaded == 0:
        logger.warning("⚠️ Ни одной записи не загружено")
    
    # 5. Рассчет и логирование статистики
    try:
        stats = calculate_statistics(valid_records)
        log_statistics(stats)
    except Exception as e:
        logger.error(f"❌ Ошибка расчёта статистики: {e}")
        return
    
    # 6. Загрузка в Google Sheets
    if stats:
        try:    
            from sheets_helper import upload_to_sheets
            
            upload_to_sheets(
            stats=stats,
            credentials_file=os.getenv('GS_CREDENTIALS_FILE'),
            spreadsheet_id=os.getenv('GS_SPREADSHEET_ID'),
            sheet_name=os.getenv('GS_SHEET_NAME')
        )
        except ImportError:
            logger.warning("⚠️ Модуль sheets_helper не найден")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки в Google Sheets: {e}")    


    # 7. Отправка email с отчётом
    if stats:
        try:
            from email_helper import send_email_report
            
            send_email_report(
                stats=stats,
                subject=f"📊 Грэйдер-скилл: {stats.get('date', 'N/A')}"
            )
        except ImportError:
            logger.warning("⚠️ Модуль email_helper не найден")
        except Exception as e:
            logger.error(f"❌ Ошибка отправки email: {e}")

    logger.info("🏁 Скрипт завершил работу")


if __name__ == "__main__":
    main()
