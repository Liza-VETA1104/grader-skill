# 📊 Grader-Skill: ETL-пайплайн для аналитики грйдера

Автоматизированный скрипт для сбора, обработки и анализа данных из API грйдер-системы SkillFactory.

---

## 🚀 Возможности

- ✅ **Загрузка данных из API** — получение статистики попыток студентов
- ✅ **Валидация и очистка** — фильтрация некорректных записей
- ✅ **Сохранение в PostgreSQL** — надёжное хранение всех попыток
- ✅ **Расширенная аналитика** — 10+ метрик для оценки эффективности
- ✅ **Google Sheets интеграция** — автоматическая выгрузка отчётов
- ✅ **Email уведомления** — отправка сводки на почту
- ✅ **Логирование** — с автоматической очисткой старых логов (> 3 дней)

---

## 📈 Собираемая статистика

| Метрика | Описание |
|---------|----------|
| `total_attempts` | Всего попыток за период |
| `successful_attempts` | Успешные попытки |
| `failed_attempts` | Неуспешные попытки |
| `unique_users` | Уникальные пользователи |
| `success_rate` | Процент успеха |
| `users_who_failed_rate` | Доля пользователей без успешных попыток |
| `avg_attempts_per_user` | Среднее попыток на пользователя |
| `avg_attempts_to_success` | Среднее попыток до первого успеха |
| `avg_time_between_attempts` | Среднее время между попытками |
| `peak_hour` | Час пиковой нагрузки |

---

## 🛠 Технологии

- **Python 3.10+**
- **PostgreSQL** — база данных
- **Google Sheets API** — визуализация отчётов
- **SMTP** — email уведомления
- **requests** — HTTP-запросы к API
- **psycopg2** — работа с PostgreSQL
- **gspread** — интеграция с Google Sheets

---

## 📦 Установка

### 1. Клонирование репозитория

```bash
git clone https://github.com/ваш-username/grader-skill.git
cd grader-skill


![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14+-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)
