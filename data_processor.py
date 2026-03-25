import ast
import logging

logger = logging.getLogger(__name__)


def parse_passback_params(passback_str: str) -> dict:
    """
    Преобразует строку passback_params в словарь.
    """
    if not passback_str:
        return {}
    
    try:
        return ast.literal_eval(passback_str)
    except (ValueError, SyntaxError) as e:
        logger.warning(f"Не удалось распарсить passback_params: {passback_str[:50]}... Ошибка: {e}")
        return {}


def validate_record(record: dict) -> tuple:
    """
    Проверяет, что все обязательные поля присутствуют.
    Возвращает кортеж (is_valid: bool, error_message: str)
    """
    if not record.get('lti_user_id'):
        return False, "Отсутствует lti_user_id"
    
    if record.get('attempt_type') not in ['run', 'submit']:
        return False, f"Неверный attempt_type: {record.get('attempt_type')}"
    
    passback = parse_passback_params(record.get('passback_params', ''))
    
    required_keys = ['oauth_consumer_key', 'lis_result_sourcedid', 'lis_outcome_service_url']
    for key in required_keys:
        if key not in passback:
            return False, f"В passback_params отсутствует: {key}"
    
    return True, ""