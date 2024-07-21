from flask import Flask, render_template, request, jsonify, Response
from resume_parser import get_links, get_data
from test import fetch_vacancies, delete_old_vacancy_records, get_all_vacancies_from_db
import threading
import time
import json
import queue

app = Flask(__name__)

resume_results = []
vacancy_results = []
is_parsing_resumes = False
is_fetching_vacancies = False
listeners = []

@app.route('/')
def index():
    vacancies = get_all_vacancies_from_db()  # Получаем все вакансии из базы данных
    return render_template('index.html', vacancies=vacancies)

@app.route('/parse_resumes', methods=['POST'])
def parse_resumes():
    global is_parsing_resumes, resume_results
    if is_parsing_resumes:
        return jsonify({"status": "error", "message": "Parsing resumes already in progress"})

    search_text = request.form.get('search_text')
    if not search_text:
        return jsonify({"status": "error", "message": "Search text is required"})

    is_parsing_resumes = True
    resume_results = []
    threading.Thread(target=parse_resumes_thread, args=(search_text,)).start()
    return jsonify({"status": "success", "message": "Parsing resumes started"})

@app.route('/fetch_vacancies', methods=['POST'])
def fetch_vacancies_route():
    global is_fetching_vacancies, vacancy_results
    if is_fetching_vacancies:
        return jsonify({"status": "error", "message": "Fetching vacancies already in progress"})

    job_title = request.form.get('job_title')
    if not job_title:
        return jsonify({"status": "error", "message": "Job title is required"})

    is_fetching_vacancies = True
    vacancy_results = []
    threading.Thread(target=fetch_vacancies_thread, args=(job_title,)).start()
    return jsonify({"status": "success", "message": "Fetching vacancies started"})

@app.route('/status')
def status():
    global is_parsing_resumes, is_fetching_vacancies, resume_results, vacancy_results
    return jsonify({
        "is_parsing_resumes": is_parsing_resumes,
        "is_fetching_vacancies": is_fetching_vacancies,
        "resume_results": resume_results,
        "vacancy_results": vacancy_results
    })

@app.route('/stream')
def stream():
    def event_stream():
        global listeners
        q = queue.Queue()
        listeners.append(q)
        try:
            while True:
                result = q.get()
                yield f'data: {json.dumps(result)}\n\n'
        except GeneratorExit:
            listeners.remove(q)

    return Response(event_stream(), content_type='text/event-stream')

def notify_clients(data):
    for listener in listeners:
        listener.put(data)

def parse_resumes_thread(search_text):
    global is_parsing_resumes, resume_results
    count = 0
    for link in get_links(search_text):
        if count >= 2000:
            break
        resume = get_data(link)
        if resume:
            resume_results.append(resume)
            notify_clients({"type": "resume", "data": resume})
        time.sleep(1)
        count += 1
    is_parsing_resumes = False

def fetch_vacancies_thread(job_title):
    global is_fetching_vacancies, vacancy_results
    delete_old_vacancy_records()  # Удаляем старые записи перед началом парсинга
    vacancies = fetch_vacancies(job_title)
    for vacancy in vacancies:
        vacancy_results.append(vacancy)
        notify_clients({"type": "vacancy", "data": vacancy})
    is_fetching_vacancies = False

if __name__ == '__main__':
    app.run(debug=True)
