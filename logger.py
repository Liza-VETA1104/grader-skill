import logging
import os
from datetime import datetime, timedelta


def setup_logger(log_dir="logs"):
    """
    Настраивает логгер с записью в файл и консоль.
    Удаляет логи старше 3 дней.
    """
    os.makedirs(log_dir, exist_ok=True)
    
    # Удаляем логи старше 3 дней
    today = datetime.now()
    for file in os.listdir(log_dir):
        if file.startswith("log_") and file.endswith(".txt"):
            try:
                file_date = datetime.strptime(file[4:-4], "%Y-%m-%d")
                if today - file_date > timedelta(days=3):
                    os.remove(os.path.join(log_dir, file))
            except ValueError:
                pass
    
    log_file = os.path.join(log_dir, f"log_{today.strftime('%Y-%m-%d')}.txt")
    
    # Проверяем, есть ли уже handlers (чтобы не дублировать)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)s | %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8', mode='a'),
                logging.StreamHandler()
            ]
        )
    
    return logging.getLogger(__name__)