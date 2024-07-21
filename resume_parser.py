# -*- coding: utf-8 -*-
import time
import requests
from bs4 import BeautifulSoup
import sqlite3
import ran

def create_table():
    db = sqlite3.connect('resume.db')
    c = db.cursor()

    c.execute("""CREATE TABLE IF NOT EXISTS resume (
        id INTEGER PRIMARY KEY,
        name TEXT,
        age INTEGER,
        salary TEXT,
        tags TEXT,
        link TEXT,
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    db.commit()
    db.close()

    print("Таблица создана")

def delete_old_records():
    # Delete records older than 30 days (adjust as needed)
    db = sqlite3.connect('resume.db')
    c = db.cursor()
    c.execute("DELETE FROM resume WHERE date_created < date('now', '-30 days')")
    db.commit()
    db.close()

def get_links(text):
    create_table()

    total_records = 2000
    records_fetched = 0

    for page in range(1, 100):  # Assuming maximum of 100 pages
        try:
            data = requests.get(
                url=f"https://hh.ru/search/resume?text={text}&area=1&isDefaultArea=true&exp_period=all_time&logic=normal&pos=full_text&page={page}",
                headers={"User-Agent": "User-Agent"}
            )
            if data.status_code != 200:
                continue
            soup = BeautifulSoup(data.content, "lxml")
            for a in soup.find_all("a", attrs={"data-qa": "serp-item__title"}):
                yield f"https://hh.ru{a.attrs['href'].split('?')[0]}"
                records_fetched += 1
                if records_fetched >= total_records:
                    return
        except Exception as e:
            print(f"Error fetching links: {e}")

def get_data(link):
    delete_old_records()  # Call delete function before fetching new data

    data = requests.get(
        url=link,
        headers={"User-Agent": "User-Agent"}
    )
    if data.status_code != 200:
        return None

    soup = BeautifulSoup(data.content, "lxml")
    id = random.randint(1, 10000)
    try:
        name = soup.find(attrs={"class": "resume-block__title-text"}).text
    except:
        name = ""
    try:
        age = soup.find(attrs={"data-qa": "resume-personal-age"}).text.replace("\xa0", "")
    except:
        age = ""
    try:
        salary = soup.find(attrs={"class": "resume-block__salary"}).text.replace("\u2009", "").replace("\xa0", "")
    except:
        salary = ""
    try:
        tags = [tag.text for tag in
                soup.find(attrs={"class": "bloko-tag-list"}).find_all(attrs={"class": "bloko-tag__section_text"})]
    except:
        tags = []

    db = sqlite3.connect('resume.db')
    c = db.cursor()

    c.execute('''INSERT OR IGNORE INTO resume
    (id, name, age, salary, tags, link) 
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (id, name, age, salary, tags[0] if tags else "", link))

    db.commit()
    db.close()

    resume = {
        "name": name,
        "age": age,
        "salary": salary,
        "tags": tags,
        "link": link
    }
    return resume