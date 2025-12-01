"""
Priority management system for StashKeeper
"""

import json
import os
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

PRIORITY_FILE = "priority_users.json"

# Приоритеты
DEFAULT_PRIORITY = 1
HIGH_PRIORITY = 2
ADMIN_PRIORITY = 3

def load_priority_users() -> Dict[str, int]:
    """Загружает список приоритетных пользователей из файла"""
    if os.path.exists(PRIORITY_FILE):
        try:
            with open(PRIORITY_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Конвертируем ключи в строки для консистентности
                return {str(k): int(v) for k, v in data.items()}
        except Exception as e:
            logger.error(f"Error loading priority users: {e}")
            return {}
    return {}

def save_priority_users(users: Dict[str, int]) -> None:
    """Сохраняет список приоритетных пользователей в файл"""
    try:
        with open(PRIORITY_FILE, 'w', encoding='utf-8') as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error saving priority users: {e}")

def get_user_priority(user_id: str) -> int:
    """Получает приоритет пользователя"""
    users = load_priority_users()
    return users.get(str(user_id), DEFAULT_PRIORITY)

def set_user_priority(user_id: str, priority: int) -> None:
    """Устанавливает приоритет пользователя"""
    users = load_priority_users()
    users[str(user_id)] = priority
    save_priority_users(users)

def remove_user_priority(user_id: str) -> None:
    """Удаляет приоритет пользователя"""
    users = load_priority_users()
    if str(user_id) in users:
        del users[str(user_id)]
        save_priority_users(users)

def set_multiple_users_priority(user_ids: List[str], priority: int) -> None:
    """Устанавливает приоритет для нескольких пользователей"""
    users = load_priority_users()
    for user_id in user_ids:
        users[str(user_id)] = priority
    save_priority_users(users)

def remove_multiple_users_priority(user_ids: List[str]) -> None:
    """Удаляет приоритет для нескольких пользователей"""
    users = load_priority_users()
    for user_id in user_ids:
        users.pop(str(user_id), None)
    save_priority_users(users)

def get_all_priority_users() -> Dict[str, int]:
    """Возвращает всех приоритетных пользователей"""
    return load_priority_users()

def clear_all_priorities() -> None:
    """Очищает все приоритеты"""
    save_priority_users({})