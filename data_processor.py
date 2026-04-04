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
