# Платформа автоматического разбора и структурирования сценариев

## 1. Локальная проверка 

Чтобы проверить решение локально, нужно выполнить следующие команды:

1) Запустить терминал из папки, где лежит `main.py`

2) Импортировать все необходимые зависимости:

`python -m pip install -r requirements.txt`

`python -m spacy download ru_core_news_sm`

3) Запустить скрипт:

`python main.py <input_file> <output_file>`

**Пример команды:**

`python main.py "E:\WINK\Input.docx" "E:\WINK\Output.xlsx"`

## 2. Docker

Чтобы установить Docker, нужно выполнить следующую команду:

`docker build -t doc2xlsx .`

Чтобы запустить Docker вместе с приложением, нужно выполнить следующую команду:

`docker run -d --name doc2xlsx_container -p 8000:8000 doc2xlsx`

Проверить работу решения можно с помощью следующей команды:

`curl -X POST "http://localhost:8000/convert" -F "file=@E:\WINK\Input.docx" --output Output.xlsx`

ИЛИ

Запустить в адресной строке `http://localhost:8000/docs` и выбрать файл вручную.

## 3. Декстопный клиент

Приложение можно протестировать на десктопном приложении на Windows.

URL: https://disk.yandex.ru/d/XwyNkNLi6Qc-vQ