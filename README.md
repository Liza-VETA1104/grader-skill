# Grader-Skill: ETL-пайплайн для аналитики грэйдера



Автоматизированный ETL-пайплайн для сбора, обработки и анализа данных из API грэйдер-системы онлайн-школы. Проект включает загрузку данных, валидацию, сохранение в PostgreSQL, расчёт метрик, выгрузку в Google Sheets и email-уведомления.


<img width="929" height="294" alt="image" src="https://github.com/user-attachments/assets/c01bc87a-dc70-478a-8361-31152541427d" />

---

## Оглавление

- [Возможности](#-возможности)
- [Технологии](#-технологии)
- [Установка](#-установка)
- [Настройка](#-настройка)
- [Структура базы данных](#-структура-базы-данных)
- [Запуск](#-запуск)
- [Собираемые метрики](#-собираемые-метрики)
- [Примеры работы](#-примеры-работы)
- [Структура проекта](#-структура-проекта)


---

## Возможности

| Функция | Описание |
|---------|----------|
| ** Загрузка из API** | Получение статистики попыток за последние 24 часа |
| ** Валидация** | Проверка LTI-параметров, фильтрация некорректных записей |
| ** PostgreSQL** | Надёжное хранение всех попыток с корректными типами данных |
| ** Аналитика** | 10+ метрик для оценки эффективности обучения |
| ** Google Sheets** | Автоматическая выгрузка дневных отчётов в таблицу |
| ** Email** | Ежедневные уведомления со сводкой на почту |
| ** Логирование** | Детальное логирование с автоматической очисткой старых логов (>3 дней) |

---

## Технологии

- Python 3.10+
- PostgreSQL
- Google Sheets API
- SMTP для email
- requests
- psycopg2
- gspread

  ---


## Установка

### 1. Клонирование репозитория

```bash
git clone https://github.com/Liza-VETA1104/grader-skill.git
cd grader-skill
 ``` 

### 2.  Создание виртуального окружения
### Windows
```bash
python -m venv venv
venv\Scripts\activate
```

### Linux/Mac
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Установка зависимостей
```bash
pip install -r requirements.txt
```

---

## Настройка

### 1. Создайте файл .env
[Пример](adds/.env.example)

### 2. Настройка Google Sheets

Создайте сервисный аккаунт в [Google Cloud Console](https://console.cloud.google.com/welcome/new?spm=a2ty_o01.29997173.0.0.76e75171sNV0Uc)
Включите Google Sheets API
Скачайте JSON-ключ и положите в корень проекта
Поделитесь таблицей с email сервисного аккаунта (права редактора)

### 3. Создание таблицы в БД

[SQL](adds/grader.sql)

---

## Структура базы данных


| Поле                      | Тип             | Описание                     |
|---------------------------|-----------------|------------------------------|
| `id`                      | SERIAL          | Первичный ключ               |
| `user_id`                 | VARCHAR(255)    | ID пользователя              |
| `oauth_consumer_key`      | VARCHAR(255)    | OAuth ключ                   |
| `lis_result_sourcedid`    | TEXT            | ID результата LTI            |
| `lis_outcome_service_url` | TEXT            | URL для отправки оценки      |
| `is_correct`              | BOOLEAN         | Правильно/неправильно        |
| `attempt_type`            | VARCHAR(50)     | Тип попытки (run/submit)     |
| `created_at`              | TIMESTAMP       | Время создания               |

---

## Запуск

### Ручной запуск
```bash
python main.py
```

### Автоматизация (cron)
```bash
crontab -e
0 8 * * * cd /path/to/grader-skill && python main.py >> cron.log 2>&1
```

---


## Собираемые метрики

| Метрика | Что показывает |
|---------|----------------|
| `total_attempts` | Всего попыток за период |
| `successful_attempts` | Успешные попытки (is_correct=True) |
| `failed_attempts` | Неуспешные попытки (is_correct=False) |
| `unique_users` | Уникальные пользователи |
| `success_rate` | Процент успеха |
| `users_who_failed_rate` | Доля пользователей без успешных попыток |
| `avg_attempts_per_user` | Среднее количество попыток на пользователя |
| `avg_attempts_to_success` | Среднее количество попыток до первого успеха |
| `avg_time_between_attempts` | Среднее время между попытками (в секундах) |
| `peak_hour` | Час с максимальной активностью |

---
## Примеры работы
### Логи

<img width="896" height="380" alt="image" src="https://github.com/user-attachments/assets/3d5be04f-ac5e-483b-9bc9-1fd71c27d000" />


## Google Sheets

<img width="1304" height="185" alt="image" src="https://github.com/user-attachments/assets/829d0049-3e0a-4af9-9f7c-9c0adfc558aa" />

## Email 

<img width="1265" height="50" alt="image" src="https://github.com/user-attachments/assets/89b169f4-b3d5-4db5-941f-80d622132a24" />

<img width="605" height="590" alt="image" src="https://github.com/user-attachments/assets/ea6c237e-2786-4f4a-b9fa-d0547ceb9dd4" />

---

## Структура проекта

<img width="919" height="591" alt="image" src="https://github.com/user-attachments/assets/f6a6db53-7767-4ccb-894a-931cc45baef0" />




> ⚠️ **Важно:** Файлы `.env`, `*.json` (ключи Google), `logs/` и `skipped_records.json` не должны попадать в репозиторий.
















