# test.py

import sqlite3
import requests
import pytz
import time
from datetime import datetime, timedelta

URL = 'https://api.hh.ru/vacancies'

schedule_descriptions = {
    'fullDay': 'Полный день',
    'shift': 'Сменный график',
    'flexible': 'Гибкий график',
    'remote': 'Удаленная работа',
    'flyInFlyOut': 'Вахтовый метод'
}

regions = [
    1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
    26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
    49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71,
    72, 73, 74, 75, 76, 77, 78, 79, 83, 86, 89, 93, 94, 95, 100, 101, 102, 109, 113
]

def split_interval(date_from, date_to):
    """Функция для разбиения интервала на два равных интервала."""
    half_duration = (date_to - date_from) / 2
    middle_date = date_from + half_duration
    return (date_from, middle_date), (middle_date, date_to)

def get_vacancies(url, params):
    vacancies = []
    page = 0
    while True:
        time.sleep(0.04)  # Задержка между запросами
        params['page'] = page
        response = requests.get(url, params=params)

        if response.status_code != 200:
            print(f'Ошибка при выполнении запроса к API hh.ru: {response.status_code}, График работы: ' + str(params['schedule']))
            break

        data = response.json()

        for item in data['items']:
            employer = item.get('employer', {})
            vacancies.append({
                'Наименование вакансии': item.get('name'),
                'Ссылка': item.get('alternate_url'),
                'Имя компании': employer.get('name', 'Не указано'),
                'Ссылка на компанию': employer.get('alternate_url', 'Не указано'),
                'График работы': schedule_descriptions.get(params.get('schedule'), 'Не указано'),
                'Регион': params['area']
            })

        if (page + 1) >= data['pages']:
            break
        page += 1
    time.sleep(1.2)
    return vacancies

def save_vacancy_to_db(vacancy):
    db = sqlite3.connect('vacancy.db')
    c = db.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS vacancies
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT,
                  url TEXT,
                  company_name TEXT,
                  company_url TEXT,
                  schedule TEXT,
                  region TEXT,
                  date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    c.execute('''INSERT INTO vacancies
                 (name, url, company_name, company_url, schedule, region)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (vacancy['Наименование вакансии'],
               vacancy['Ссылка'],
               vacancy['Имя компании'],
               vacancy['Ссылка на компанию'],
               vacancy['График работы'],
               vacancy['Регион']))
    db.commit()
    db.close()

def delete_old_vacancy_records():
    db = sqlite3.connect('vacancy.db')
    c = db.cursor()
    # Создание таблицы, если она не существует
    c.execute('''CREATE TABLE IF NOT EXISTS vacancies
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT,
                  url TEXT,
                  company_name TEXT,
                  company_url TEXT,
                  schedule TEXT,
                  region TEXT,
                  date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    # Удаление старых записей
    c.execute("DELETE FROM vacancies WHERE date_created < date('now', '-30 days')")
    db.commit()
    db.close()

def fetch_vacancies(job_title):
    time_zone = pytz.timezone('Europe/Moscow')
    times_fullday = []
    day_shift = 1

    for day in range(3):
        date_to = datetime.now(time_zone) - timedelta(days=day)
        date_from = date_to - timedelta(days=day_shift)
        first_half, second_half = split_interval(date_from, date_to)
        times_fullday.extend([first_half, second_half])

    formatted_times_fullday = []
    for interval in times_fullday:
        date_from_str = interval[0].strftime("%Y-%m-%dT%H:%M:%S%z")
        date_to_str = interval[1].strftime("%Y-%m-%dT%H:%M:%S%z")
        formatted_times_fullday.append((date_from_str, date_to_str))

    times_fullday = formatted_times_fullday

    previous_date_to = datetime.now(time_zone) - timedelta(days=3)

    for i in range(2):
        date_to = previous_date_to
        date_from = date_to - timedelta(days=1)
        times_fullday.append((date_from.strftime("%Y-%m-%dT%H:%M:%S%z"), date_to.strftime("%Y-%m-%dT%H:%M:%S%z")))
        previous_date_to = date_from

    for i in range(2):
        date_to = previous_date_to
        date_from = date_to - timedelta(days=2)
        times_fullday.append((date_from.strftime("%Y-%m-%dT%H:%M:%S%z"), date_to.strftime("%Y-%m-%dT%H:%M:%S%z")))
        previous_date_to = date_from

    date_to = previous_date_to
    date_from = date_to - timedelta(weeks=1)
    times_fullday.append((date_from.strftime("%Y-%m-%dT%H:%M:%S%z"), date_to.strftime("%Y-%m-%dT%H:%M:%S%z")))
    previous_date_to = date_from

    date_to = previous_date_to
    date_from = date_to - timedelta(days=30)
    times_fullday.append((date_from.strftime("%Y-%m-%dT%H:%M:%S%z"), date_to.strftime("%Y-%m-%dT%H:%M:%S%z")))

    times_fullday = sorted(times_fullday, key=lambda x: x[0])

    times_remote = []

    for i in range(0, len(times_fullday) - 1, 2):
        start = times_fullday[i][0]
        end = times_fullday[i + 1][1]
        times_remote.append((start, end))

    end_of_last_interval = datetime.strptime(times_fullday[-1][1], "%Y-%m-%dT%H:%M:%S%z")

    week_interval_end = end_of_last_interval
    week_interval_start = week_interval_end - timedelta(weeks=1)

    month_interval_end = week_interval_start
    month_interval_start = month_interval_end - timedelta(days=30)

    week_interval = (
        week_interval_start.strftime("%Y-%m-%dT%H:%M:%S%z"), week_interval_end.strftime("%Y-%m-%dT%H:%M:%S%z"))
    month_interval = (
        month_interval_start.strftime("%Y-%m-%dT%H:%M:%S%z"), month_interval_end.strftime("%Y-%m-%dT%H:%M:%S%z"))

    times_else = [week_interval, month_interval]

    all_vacancies = []

    for region in regions:
        print(f'Обработка региона: {region}')
        for schedule in schedule_descriptions.keys():
            print(f'  График работы: {schedule_descriptions[schedule]}')
            if schedule == 'fullDay':
                for date_from_str, date_to_str in times_fullday:
                    print(f'    Интервал: {date_from_str} - {date_to_str}')
                    params = {
                        'text': job_title,
                        'area': region,
                        'per_page': '100',
                        'search_field': 'name',
                        'schedule': schedule,
                        'date_from': date_from_str,
                        'date_to': date_to_str,
                    }
                    vacancies = get_vacancies(URL, params)
                    for vacancy in vacancies:
                        save_vacancy_to_db(vacancy)
                    all_vacancies.extend(vacancies)
            elif schedule == 'remote':
                for date_from_str, date_to_str in times_remote:
                    print(f'    Интервал: {date_from_str} - {date_to_str}')
                    params = {
                        'text': job_title,
                        'area': region,
                        'per_page': '100',
                        'search_field': 'name',
                        'schedule': schedule,
                        'date_from': date_from_str,
                        'date_to': date_to_str,
                    }
                    vacancies = get_vacancies(URL, params)
                    for vacancy in vacancies:
                        save_vacancy_to_db(vacancy)
                    all_vacancies.extend(vacancies)
            else:
                for date_from_str, date_to_str in times_else:
                    print(f'    Интервал: {date_from_str} - {date_to_str}')
                    params = {
                        'text': job_title,
                        'area': region,
                        'per_page': '100',
                        'search_field': 'name',
                        'schedule': schedule,
                        'date_from': date_from_str,
                        'date_to': date_to_str,
                    }
                    vacancies = get_vacancies(URL, params)
                    for vacancy in vacancies:
                        save_vacancy_to_db(vacancy)
                    all_vacancies.extend(vacancies)

    return all_vacancies

def get_all_vacancies_from_db():
    db = sqlite3.connect('vacancy.db')
    c = db.cursor()
    c.execute('''SELECT name, url, company_name, company_url, schedule, region FROM vacancies''')
    rows = c.fetchall()
    db.close()
    vacancies = []
    for row in rows:
        vacancies.append({
            'Наименование вакансии': row[0],
            'Ссылка': row[1],
            'Имя компании': row[2],
            'Ссылка на компанию': row[3],
            'График работы': row[4],
            'Регион': row[5]
        })
    return vacancies
