from __future__ import annotations
import re
from collections import defaultdict
from zipfile import ZipFile
from lxml import etree
import PyPDF2
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Set, Optional
import spacy
from transformers import pipeline, AutoTokenizer, AutoModelForTokenClassification
import torch
import pandas as pd
import argparse
from pymorphy3 import MorphAnalyzer
import io
import os
import ru_core_news_sm
import sys
import traceback
import tempfile
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import StreamingResponse

os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

# === API ===

app = FastAPI()

def df_to_xlsx_bytes(df: pd.DataFrame) -> io.BytesIO:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    buffer.seek(0)  # перемотать в начало, чтобы FastAPI читал с 0
    return buffer

@app.post("/convert")
async def convert_to_xlsx(file: UploadFile = File(...)):
    """
    Принимает DOCX или PDF, возвращает XLSX.
    """
    filename = file.filename or "input"
    ext = os.path.splitext(filename)[1].lower()

    if ext not in [".docx", ".pdf"]:
        raise HTTPException(status_code=400, detail="Поддерживаются только .docx и .pdf")

    # Сохраняем во временный файл
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name
            
        excelDataFrame = full_pipeline_1(tmp_path)
        xlsx_io = df_to_xlsx_bytes(excelDataFrame)

        # Готовим ответ
        output_name = os.path.splitext(filename)[0] + ".xlsx"
        return StreamingResponse(
            xlsx_io,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="{output_name}"'
            },
        )

    finally:
        # Чистим временный файл, если он был создан
        try:
            os.remove(tmp_path)
        except Exception:
            pass
            
@app.get("/ping")
async def health():
    return {"status": "ok"}

# ==== Relative Path ====

def get_base_path() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).parent

def resource_path(relative: str) -> Path:
    """
    Возвращает путь к ресурсу, который лежит рядом со скриптом
    (при обычном запуске) или внутри папки PyInstaller (_MEIPASS)
    при запуске из .exe.
    """
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        # Запуск из упакованного exe
        base_path = Path(sys._MEIPASS)
    else:
        # Обычный запуск .py
        base_path = Path(__file__).parent

    return base_path / relative
    
BASE_DIR = get_base_path()

def get_device():
    # универсально: если есть CUDA — используем её, иначе CPU
    if torch.cuda.is_available():
        return 0
    return -1  # CPU

# ==== 1. Загрузка текста из файла ====

def read_docx_with_full_numbering(docx_path):
    ns = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}

    with ZipFile(docx_path) as z:
        doc_xml = etree.fromstring(z.read("word/document.xml"))
        styles_xml = etree.fromstring(z.read("word/styles.xml"))

    # --- 1️⃣ Стили → numId / ilvl ---
    style_map = {}
    for style in styles_xml.findall(".//w:style[@w:type='paragraph']", ns):
        style_id = style.get("{%s}styleId" % ns["w"])
        numPr = style.find(".//w:numPr", ns)
        if numPr is not None:
            numId = numPr.find("./w:numId", ns)
            ilvl = numPr.find("./w:ilvl", ns)
            style_map[style_id] = {
                "numId": int(numId.get("{%s}val" % ns["w"])) if numId is not None else None,
                "ilvl": int(ilvl.get("{%s}val" % ns["w"])) if ilvl is not None else 0,
            }

    # --- 2️⃣ Абзацы + определение нумерации ---
    paragraphs = []
    for p in doc_xml.findall(".//w:p", ns):
        text = "".join(t.text for t in p.findall(".//w:t", ns) if t.text)
        text = text.strip()
        styleEl = p.find(".//w:pStyle", ns)
        style_id = styleEl.get("{%s}val" % ns["w"]) if styleEl is not None else None
        numPr = p.find(".//w:numPr", ns)

        numId = ilvl = None
        if numPr is not None:
            numIdEl = numPr.find("./w:numId", ns)
            ilvlEl = numPr.find("./w:ilvl", ns)
            numId = int(numIdEl.get("{%s}val" % ns["w"])) if numIdEl is not None else None
            ilvl = int(ilvlEl.get("{%s}val" % ns["w"])) if ilvlEl is not None else 0
        elif style_id in style_map:
            numId = style_map[style_id]["numId"]
            ilvl = style_map[style_id]["ilvl"]

        paragraphs.append({
            "text": text,
            "numId": numId,
            "ilvl": ilvl if ilvl is not None else 0,
        })

    # --- 3️⃣ Восстанавливаем нумерацию ---
    counters = defaultdict(lambda: [0]*9)
    lines = []

    for p in paragraphs:
        numId = p["numId"]
        ilvl = p["ilvl"]
        text = p["text"]

        # если номер уже есть в тексте, не добавляем
        if re.match(r"^\s*\d+([-.]\s*\d+){0,2}([-.]\s*[A-ZА-Я])?\.", text):
            lines.append(text)
            continue

        # если есть нумерация Word — восстанавливаем
        if numId is not None:
            counters[numId][ilvl] += 1
            for j in range(ilvl + 1, len(counters[numId])):
                counters[numId][j] = 0

            num_str = "-".join(str(x) for x in counters[numId][:ilvl + 1] if x > 0) + "."
            lines.append(f"{num_str} {text}")
        else:
            lines.append(text)

    return "\n".join(lines)

def upload_file(link):
    suffix = Path(link).suffix.lower()
    if suffix == '.docx':
        print('Файл успешно загружен.')
        document_text = read_docx_with_full_numbering(link)
        return document_text

    elif suffix == '.pdf':
        with open(link, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            document_text = "\n<<<PAGE_BREAK>>>\n".join(
                page.extract_text() for page in reader.pages if page.extract_text()
            )
        print('Файл успешно загружен.')
        return document_text

    else:
        return "Выберите файл в формате docx или pdf."

# ==== 2. Парсер эпизодов и первых колонок ====

def parse_episode_from_text(scene_num, text, match_start):
    """Определяет номер серии на основе различных паттернов."""
    episode_num = None
    prev_text = text[:match_start]
    next_text = text[match_start:match_start + 200]  # немного после тоже смотрим

    # --- 1️⃣ По сцене: 3-1, 3-2-A и т.д. ---
    if scene_num:
        ep_match = re.match(r"^(\d+)", scene_num)
        if ep_match:
            episode_num = ep_match.group(1)

    # --- 2️⃣ Техно-формат типа С1Э03 или С02Е05 (если используется) ---
    pattern_tech = re.findall(
        r"[СC]\s*(\d+)\s*[ЭE]\s*(\d+)",
        text,
        flags=re.IGNORECASE
    )
    if pattern_tech:
        season, episode = pattern_tech[-1]
        episode_num = episode

    # --- 2a) Человеческий формат: "1 СЕЗОН 1 СЕРИЯ" ---
    season_ep = re.findall(
        r"(\d+)\s*СЕЗОН[^\n]*?(\d+)\s*СЕРИ",
        text,
        flags=re.IGNORECASE
    )
    if season_ep:
        season, episode = season_ep[-1]
        episode_num = episode

    # --- 3️⃣ Текстовый формат: «третья серия», «конец третьей серии» ---
    # сначала ищем перед сценой
    word_match = re.search(
        r"(?:КОНЕЦ\s+)?([А-Яа-яё\s-]+?)\s+СЕРИИ?",
        prev_text,
        flags=re.IGNORECASE
    )

    # если не нашли — смотрим немного после (некоторые сценаристы ставят после сцен)
    if not word_match:
        word_match = re.search(
            r"(?:КОНЕЦ\s+)?([А-Яа-яё\s-]+?)\s+СЕРИИ?",
            next_text,
            flags=re.IGNORECASE
        )

    # если всё ещё не нашли — смотрим "хвост" текста, где обычно "КОНЕЦ ... СЕРИИ"
    if not word_match:
        tail_text = text[-2000:]
        word_match = re.search(
            r"(?:КОНЕЦ\s+)?([А-Яа-яё0-9\s-]+?)\s+СЕРИИ?",
            tail_text,
            flags=re.IGNORECASE
        )

    raw_words = ""
    num_match = None
    if word_match:
        raw_words = word_match.group(1).strip()
        words_up = raw_words.upper()
        num_match = re.search(r"\b(\d{1,3})\b", words_up)

    if num_match:
        # если в фрагменте явно есть цифра (напр. "1 СЕРИИ"), берём её
        episode_num = num_match.group(1)
    else:
        # иначе пробуем перевести словесное числительное -> число
        episode_num = russian_ordinal_to_int(raw_words.lower()) or episode_num

    return str(episode_num or "")


def russian_ordinal_to_int(phrase: str) -> int | None:
    """Преобразует русские порядковые числительные в число."""
    phrase = phrase.replace("-", " ").replace("Ё", "Е").upper()
    ones = {
        "ПЕРВАЯ": 1, "ВТОРАЯ": 2, "ТРЕТЬЯ": 3, "ЧЕТВЕРТАЯ": 4, "ПЯТАЯ": 5,
        "ШЕСТАЯ": 6, "СЕДЬМАЯ": 7, "ВОСЬМАЯ": 8, "ДЕВЯТАЯ": 9, "ДЕСЯТАЯ": 10,
        "ОДИННАДЦАТАЯ": 11, "ДВЕНАДЦАТАЯ": 12, "ТРИНАДЦАТАЯ": 13,
        "ЧЕТЫРНАДЦАТАЯ": 14, "ПЯТНАДЦАТАЯ": 15, "ШЕСТНАДЦАТАЯ": 16,
        "СЕМНАДЦАТАЯ": 17, "ВОСЕМНАДЦАТАЯ": 18, "ДЕВЯТНАДЦАТАЯ": 19,
        "ФИНАЛЬНАЯ": 999
    }
    tens = {
        "ДВАДЦАТАЯ": 20, "ТРИДЦАТАЯ": 30, "СОРОКОВАЯ": 40, "ПЯТИДЕСЯТАЯ": 50,
        "ШЕСТИДЕСЯТАЯ": 60, "СЕМЬДЕСЯТАЯ": 70, "ВОСЬМИДЕСЯТАЯ": 80,
        "ДЕВЯНОСТАЯ": 90, "СТАЯ": 100
    }

    words = phrase.split()
    total = 0
    for word in words:
        if word in ones:
            total += ones[word]
        elif word in tens:
            total += tens[word]
        elif word.startswith("ДВАДЦ"): total += 20
        elif word.startswith("ТРИДЦ"): total += 30
        elif word.startswith("СОРОК"): total += 40
        elif word.startswith("ПЯТ"): total += 5
        elif word.startswith("ШЕСТ"): total += 6
        elif word.startswith("СЕМ"): total += 7
        elif word.startswith("ВОС"): total += 8
        elif word.startswith("ДЕВ"): total += 9
        elif word.startswith("СТО"): total += 100

    return total if total > 0 else None

def normalize_time(text: str) -> str:
    """
    Ищет в тексте упоминания времени суток и нормализует:
    -> НОЧЬ / УТРО / ДЕНЬ / ВЕЧЕР
    """
    if not text:
        return ''
    
    t = text.lower().replace('ё', 'е')

    if re.search(r'\b(ночью|ночь|поздно ночью|глубокой ночью)\b', t):
        return 'НОЧЬ'
    if re.search(r'\b(утром|утро|к рассвету|на рассвете|под утро|рассвет(е|а|у)|рассвет)\b', t):
        return 'УТРО'
    if re.search(r'\b(днем|днем|день|в полдень|светло)\b', t):
        return 'ДЕНЬ'
    if re.search(r'\b(вечером|вечер|поздним вечером|сумерки|в сумерках|к сумеркам|из сумерек)\b', t):
        return 'ВЕЧЕР'
    return ''

def parse_script_with_episode(pdf_text: str):
    """
    Разбивает сценарий на сцены и извлекает:
    - episode_num (номер серии)
    - scene_num (номер сцены)
    - location (ИНТ, ЭКСТ, НАТ, комбинации)
    - place (место действия, очищенное)
    - time (НОЧЬ, ДЕНЬ, УТРО, ВЕЧЕР — из заголовка или fallback по контексту)
    - text (тело сцены)
    """

    # --- 0️⃣ Предобработка текста ---
    text = (pdf_text
    .replace('\xa0', ' ')
    .replace('–', '-')
    .replace('—', '-')
    .replace(' ', ' ')
    .replace('\r', '\n')
)

    # добавляем перевод строки перед сценами, НО не между числами (чтобы 1.17 не ломалось)
    text = re.sub(r'(?<!\d)\.(?=\d{1,2}\s*[-.])', '.\n', text)
    text = re.sub(r'((\r?\n\s*){5,})', '\n<<<PAGE_BREAK_GAP>>>\n', text)

    # --- 1️⃣ Основной шаблон ---
    pattern = re.compile(r'''(?imx)
        ^
        \s*
        (?:СЦЕНА\s*)?
        (?P<scene_num>
            \d+(?:[.-]\s*\d+){0,2}(?:[\s.-]*[A-ZА-Я0-9]{1,3})?
        )
        (?=\s*(?:\.|ИНТ|ЭКСТ|НАТ|\n|<<<PAGE_BREAK))     # 👈 добавили защиту от склейки через page break
        \.?\s*
        (?:ФЛЕШБЕК[^\n:.]*[:.]?|ФЛЕШБЭК[^\n:.]*[:.]?|FLASHBACK[^\n:.]*[:.]?)?\s*
        (?P<location>
            (?:ИНТ(?![а-я])(?:\.|ЕРЬЕР)?|
            ЭКСТ(?![а-я])(?:\.|ЕРЬЕР)?|
            НАТ(?![а-я])(?:\.|УРА)?)
            (?:\s*/\s*
                (?:ИНТ(?![а-я])(?:\.|ЕРЬЕР)?|
                ЭКСТ(?![а-я])(?:\.|ЕРЬЕР)?|
                НАТ(?![а-я])(?:\.|УРА)?)
            )?
        )
        [.\s:/-]*                                       # допустимые разделители
        (?P<place>                                      # локация
            (?:[^\n\r]*?)
            (?=
                (?:ДЕНЬ|НОЧЬ|УТРО|ВЕЧЕР|РАССВЕТ|СУМЕРКИ)                # останавливаемся перед временем
                |$
            )
        )
        [.\s:-]*
        (?P<time>(?:ДЕНЬ|НОЧЬ|УТРО|ВЕЧЕР|РАССВЕТ|СУМЕРКИ))?             # время суток без границ слова
        [^\n]*\n?
        ''', re.IGNORECASE | re.MULTILINE | re.VERBOSE)


    # --- 2️⃣ Извлекаем сцены ---
    matches = list(re.finditer(pattern, text))
    scenes = []
    current_episode = None  # автонаследование номера серии

    for i, match in enumerate(matches):
        start = match.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        block_text = text[start:end].strip()

        scene_num = (match.group("scene_num") or "").strip()
        location = (match.group("location") or "").strip().upper()
        raw_place = (match.group("place") or "").strip()
        time = (match.group("time") or "").strip().upper()
        scene_num = re.sub(r'\s+', '', (match.group("scene_num") or ""))
        scene_num = scene_num.strip('.-')

        raw_place = re.sub(r"\(\s*СМ\.[^)]*\)", "", raw_place, flags=re.IGNORECASE)
        raw_place = re.sub(r'[\s–:;.,-]+$', '', raw_place)
        place_norm = re.sub(r'\s+', ' ', raw_place).strip().upper()
        object_ = ""
        subobject = ""
        if place_norm:
            # режем по точке, но выбрасываем пустые куски
            parts = [p.strip() for p in place_norm.split('.') if p.strip()]
            if parts:
                object_ = parts[0]
                if len(parts) > 1:
                    # всё, что после первой "смысловой" части, считаем подобъектом
                    subobject = ". ".join(parts[1:])

        scene_num = re.sub(r'\s+', '', (match.group("scene_num") or ""))
        scene_num = scene_num.strip('.-')

        # --- Определяем серию ---
        episode_num = parse_episode_from_text(scene_num, text, match.start())
        

        # --- Определяем и нормализуем время суток ---
        if not time:  # если не извлекли из заголовка
            snippet = block_text[:250]
            time = normalize_time(snippet)
        else:
            time = normalize_time(time)


        scenes.append({
            "episode_num": episode_num or "",
            "scene_num": scene_num,
            "location": location,
            "object": object_,      
            "subobject": subobject,   
            "time": time,
            "text": block_text,
        })

    return scenes

# ==== 3. Парсер следующих колонок: персонажи, групповка, массовка ====

# -------------------- Константы и служебные штуки --------------------

RUS_UP = "А-ЯЁ"
RUS_LO = "а-яё"

HEADER_TOKENS = {"ИНТ", "НАТ", "ЭКСТ", "ДЕНЬ", "НОЧЬ", "УТРО", "ВЕЧЕР"}
SERV_MARKERS = {"ЗК", "З/К", "V.O.", "VO", "OFF"}
TRANSITION_MARKERS = {
    "cut", "cut to", "fade", "fade in", "fade out",
    "dissolve", "smash cut", "match cut", "super", "title", "slugline", "титр", "слаглайн", "переход"
}
OFFSCREEN_LINE_MARKERS = {"ГЗК", "ГОЛОС ЗА КАДРОМ"}


STOP_SINGLE = {"громко", "шутливо", "всем", "вверху"}

ROLE_KEYWORDS = {
    "охранник", "охранница", "кассир", "кассирша", "водитель", "шофёр",
    "врач", "доктор", "медсестра", "санитар", "адвокат", "юрист",
    "официант", "официантка", "менеджер", "секретарь",
    "таксист", "таксистка", "полицейский", "следователь",
    "директор", "начальник", "декан", "продавец", "продавщица",
    "оператор", "дежурный", "бармен", "барменша"
}

ROLE_SPEAKER_HEADS = {
    "ЧИНОВНИК",
    "СОТРУДНИК",
    "ОХРАННИК",
    "СЛЕДОВАТЕЛЬ",
    "ВЕДУЩИЙ",
    "СУДЬЯ",
    "АДВОКАТ"
}

# Небольшой список маркеров для "говорят все"
SPEECH_MODIFIERS = {"НАПЕРЕБОЙ", "ВСЕ", "ВСЁ", "ХОРОМ"}

PREPOSITIONS = {
    "к", "в", "во", "на", "у", "о", "об", "обо", "от",
    "по", "с", "со", "за", "для", "из", "из-за", "под",
    "над", "при", "через", "между", "перед", "про"
}

# === Игровой транспорт: словарь лемм → канон ===
TRANSPORT_MAP = {
    "МАШИНА": {
        "машина", "автомобиль", "авто", "тачка",
        "легковушка", "джип", "грузовик", "фура",
        "микроавтобус", "таксомотор"
    },
    "АВТОБУС": {"автобус", "маршрутка", "маршрутное", "пазик"},
    "ПОЕЗД": {"поезд", "электричка", "состав", "метропоезд"},
    "ТРАМВАЙ": {"трамвай"},
    "ТРОЛЛЕЙБУС": {"троллейбус"},
    "МЕТРО": {"метро"},
    "САМОЛЁТ": {"самолет", "самолёт", "аэроплан", "лайнер", "борт"},
    "ВЕРТОЛЁТ": {"вертолет", "вертолёт", "вертушка"},
    "ЛОДКА": {"лодка", "шлюпка", "байдарка", "каноэ"},
    "КАТЕР": {"катер", "катерок"},
    "ЯХТА": {"яхта"},
    "КОРАБЛЬ": {"корабль", "судно", "пароход", "баржа"},
    "МОТОЦИКЛ": {"мотоцикл", "байк", "харлей"},
    "ВЕЛОСИПЕД": {"велосипед", "байк-велосипед", "велик"},
    "КВАДРОЦИКЛ": {"квадроцикл"},
    "САНКИ": {"сани", "санки"},
    "ТАКСИ": {"такси"},
    "ПЛОТ": {"плот"}
}

IMPLICIT_MASS_GROUP_LEMMAS = {"толпа", "люди", "прохожие", "туристы", "болельщики", "зрители", "публика", "посетители", "гости"}

PUBLIC_LOCATION_LEMMAS = {"город", "улица", "площадь", "парк", "сквер", "набережная", "метро", "станция", "вокзал", "аэропорт", "торговый", "центр", "рынок", "универмаг", "тц"}

SMALL_GROUP_NUM_WORDS = r"(двое|трое|четверо|пятеро|шестеро|семеро|восьмеро|девятеро|десятеро)"
TEXT_GROUP_NOUNS = {"отряд", "участник", "команда", "группа", "ребята"}
TEXT_GROUP_CANON = {
    "отряд": "Отряд",
    "участник": "Участники",
    "команда": "Команда",
    "группа": "Группа", 
    "ребята": "Ребята"
}

GRIM_NOUN_LEMMAS = {"грим", "макияж", "тональник", "тональный", "пудра", "румяна", "тушь", "помада", "помадка", "подводка", "ресницы", "борода", "бинт",
"усы", "парик", "шрам", "шрамы", "синяк", "синяки", "ссадина", "царапина", "рана", "шрамирование", "кровь", "кровища", "грязь", "пластырь", "тату", "татуха", "татуировка"}

GRIM_ADJ_LEMMAS = {"замазанный", "избитый", "подбитый", "кровавый", "кровоточащий", "синюшный", "синеватый", "грязный", "раскрашенный", "загримированный", "округлившийся",
                   "похудевший", "впалый"}

GRIM_NOUN_LEMMAS_NORM = {
    w.replace("ё", "е") for w in GRIM_NOUN_LEMMAS
}
GRIM_ADJ_LEMMAS_NORM = {
    w.replace("ё", "е") for w in GRIM_ADJ_LEMMAS
}

# === КОСТЮМ / ОДЕЖДА ===

COSTUME_NOUN_LEMMAS = {
    "костюм", "китель",
    "форма", "униформа",
    "фуражка", "кепка", "шапка", "капюшон",
    "рубашка", "блузка", "футболка", "майка",
    "свитер", "кофта", "толстовка", "худи",
    "пальто", "куртка", "плащ", "пиджак", "жилет",
    "комбинезон", "спецовка", "халат",
    "платье", "юбка",
    "джинсы", "штаны", "брюки",
    "ботинки", "кроссовки", "туфли", "берцы", "сапоги",
    "галстук", "бабочка",
    "маска", "трусы", "стринги", "стринг", "одежда"
}

COSTUME_ADJ_LEMMAS = {
    "школьный",
    "военный",
    "парадный",
    "спортивный",
    "деловой",
    "рабочий",
    "форменный",
    "походный",
    "спасательный"
}

COSTUME_NOUN_ROOTS = {
    "форм",      # форма, форменной, в форме…
    "униформ",
}

# нормализуем "е/ё"
COSTUME_NOUN_LEMMAS_NORM = {
    w.replace("ё", "е") for w in COSTUME_NOUN_LEMMAS
}
COSTUME_ADJ_LEMMAS_NORM = {
    w.replace("ё", "е") for w in COSTUME_ADJ_LEMMAS
}

# === словари для Декорация / Пиротехника / Каскадер / Спецэффект ===

# ДЕКОРАЦИЯ: что относится к оформлению пространства, а не к мелкому реквизиту
# локации, которые считаем "природой" (не декорация)
NATURAL_PLACES = {
    "горы", "гора", "лес", "поле", "луг", "степь",
    "река", "озеро", "море", "пляж", "пустыня",
    "серпантин", "дорога", "трасса", "улица", "город"
}

# явно рукотворные места, которые хотим считать декорацией,
# даже если сцена НАТ
MANMADE_PLACES = {
    "лагерь", "палаточный лагерь", "база", "станция",
    "лодочная станция", "лодочная", "пристань", "пристани",
    "порт", "вокзал", "станция метро", "платформа",
    "школа", "больница", "суд", "клуб", "бар", "кафе",
    "ресторан", "магазин", "рынок", "гостиница", "отель",
    "двор", "подъезд", "подземный переход", "подземка"
}

# общие стопы уровня "ГОРОД", "УЛИЦА", "ГОРЫ" и т.п. —
# если сегмент *состоит только* из такого слова, декорацией не считаем
GENERIC_PLACE_STOP = NATURAL_PLACES | {
    "город", "улица", "место", "площадка", "территория"
}



# ПИРОТЕХНИКА
PYRO_NOUNS = {
    "взрыв", "взрывы", "взрывчатка",
    "фейерверк", "салют", "петарда", "петарды",
    "ракета", "ракеты", "пиротехника", "залп", "осколки",
    "огненный шар", "огненный столб", "костер"
}
PYRO_VERBS = {
    "взрываться", "взорваться", "подрывать", "детонировать",
    "стрелять", "выстрелить", "рвануть", "полыхнуть", "полыхать"
}

# КАСКАДЕР
STUNT_WORDS = {
    "каскадер", "каскадеры", "каскадерский",
    "дублер", "дублеры", "дублёр", "дублёры",
    "трюкач", "трюкачи"
}

# СПЕЦЭФФЕКТЫ
FX_NOUNS = {
    "спецэффект", "спецэффекты", "эффект", "эффекты",
    "дымовая завеса", "дым", "туман", "искра", "искры", "дымка",
    "cg", "cgi", "vfx", "анимация", "компьютерная графика",
    "slowmotion", "слоу-мо", "слоумо", "замедленная съемка", "флешбек", "флэшбек", "флешбэк", "flashback"
}
FX_KEYWORDS = {
    "замедленной съемке", "замедленной съёмке",
    "замедленно", "слоу-мо", "slow motion", "slow-motion",
    "компьютерной графикой", "компьютерная графика",
}

PYRO_NOUNS_N  = {w.replace("ё", "е") for w in PYRO_NOUNS}
PYRO_VERBS_N  = {w.replace("ё", "е") for w in PYRO_VERBS}

STUNT_WORDS_N = {w.replace("ё", "е") for w in STUNT_WORDS}

FX_NOUNS_N    = {w.replace("ё", "е") for w in FX_NOUNS}
FX_KEYWORDS_N = {w.replace("ё", "е") for w in FX_KEYWORDS}


def load_ru():
    """
    Загружаем любую доступную ru-модель spaCy.
    """
    #for name in ("ru_core_news_lg", "ru_core_news_md", "ru_core_news_sm"):
    for name in ("ru_core_news_sm"):
        try:
            #return spacy.load(name)
            nlp = ru_core_news_sm.load()
            return nlp
        except OSError:
            continue
    raise RuntimeError(
        "Не найден ru_core_news_*. Установи модель, например:\n"
        "python -m spacy download ru_core_news_sm"
    )

nlp = load_ru()

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

#PYMORPHY_DICT_DIR = BASE_DIR / "pymorphy3_dicts_ru"
PYMORPHY_DICT_DIR = resource_path("pymorphy3_dicts_ru")
morph = MorphAnalyzer()
#morph = MorphAnalyzer(dict_path=str(PYMORPHY_DICT_DIR))

def ru_lemma(token_text: str) -> str:
    """
    Берём лемму через pymorphy3.
    Если вдруг pymorphy не справился — просто возвращаем lowercase.
    """
    p = morph.parse(token_text)
    if not p:
        return token_text.lower()
    return p[0].normal_form.lower()

def _strip_punct_tail(s: str) -> str:
    return re.sub(r"[!?…\.:,;]+$", "", s).strip()


def _is_caps_line(s: str) -> bool:
    """
    Строка, которая выглядит как КАПС (диалоговый маркер).
    """
    s = s.strip()
    if not s:
        return False
    # целиком в скобках — ремарка, а не спикер
    if s.startswith("(") and s.endswith(")"):
        return False
    letters = re.sub(r"[^A-Za-zА-Яа-яЁё]", "", s)
    if not letters:
        return False
    return letters == letters.upper() and len(letters) >= 2


def _looks_like_header(s: str) -> bool:
    """
    Похоже ли на строку-заголовок сцены.
    """
    S = s.strip().upper()
    return (
        bool(re.search(r"\d+\s*[-–.]", S))
        or any(k in S for k in HEADER_TOKENS)
        or S.count(".") >= 2
    )


def _clean_caps_name(line: str) -> str:
    """
    Убираем служебные скобки из КАПС-строки:
    'СТЭЛЛА (ЗК)' -> 'СТЭЛЛА'
    """
    s = re.sub(r"\s*\(.*?\)\s*", "", line)
    s = s.strip(" .:-")
    return s


def _is_service_caps_line(s: str) -> bool:
    """
    КАПС-строка с монтажным/служебным маркером (не спикер).
    """
    if not _is_caps_line(s):
        return False
    us = s.strip().upper().strip(" .:-")
    if any(marker in us for marker in OFFSCREEN_LINE_MARKERS):
        return True
    # можно расширять по мере обнаружения мусора
    if us in HEADER_TOKENS or us in SERV_MARKERS:
        return True
    return False


def _clean_person_name(name: str) -> str:
    """
    Подчищаем финальные тире, пробелы и хвостовую пунктуацию.
    'Арине -' -> 'Арине', 'Массе...' -> 'Массе'
    """
    name = re.sub(r"\s*[-–—]+\s*$", "", name)
    name = _strip_punct_tail(name)
    return _norm(name)

def _has_digit(s: str) -> bool:
    return any(ch.isdigit() for ch in s)

# Разрешённые части речи для реквизита
ALLOWED_REKV_POS = {"NOUN", "PROPN", "ADJ"}

# Простейший фильтр шума — потом сможешь пополнить
REKV_NOISE_PAT = re.compile(r"^[А-Яа-яЁё]+$")  # только кириллица, без цифр и знаков


def _basic_rekv_filter(text: str, min_len: int = 2) -> bool:
    """
    Грубый фильтр: выкидываем явно мусорные строки.
    Возвращает True, если кандидат *проходит* фильтр.
    """
    s = text.strip()
    if len(s) < min_len:
        return False
    # если вообще нет букв
    if not re.search(r"[А-Яа-яЁё]", s):
        return False
    # если слишком "странные" символы (оставим только буквы/пробел/дефис/кавычки)
    if re.search(r"[^А-Яа-яЁё \-«»\"']", s):
        return False
    return True


ALLOWED_REKV_POS = {"ADJ", "NOUN", "PROPN"}

def normalize_phrase_adj_noun(phrase: str, nlp) -> str:
    """
    Универсальный нормализатор 'прилагательное + существительное' 
    для реквизита/костюма.
    Возвращает ОДНУ нормализованную фразу (ключ кластера).
    """
    doc = nlp(phrase)
    tokens = [t for t in doc if t.is_alpha and t.pos_ in ALLOWED_REKV_POS]
    if not tokens:
        return ""

    # (lemma, POS) с pymorphy3 для лемм
    lemmas_pos = [(ru_lemma(t.text), t.pos_) for t in tokens]

    phrases = []
    i = 0
    while i < len(lemmas_pos):
        lem, pos = lemmas_pos[i]

        # ADJ + NOUN/PROPN
        if pos == "ADJ" and i + 1 < len(lemmas_pos) and lemmas_pos[i + 1][1] in {"NOUN", "PROPN"}:
            lem2, _ = lemmas_pos[i + 1]
            phrases.append(f"{lem} {lem2}")   # "походный одежда"
            i += 2
        elif pos in {"NOUN", "PROPN"}:
            phrases.append(lem)                # одиночное существительное
            i += 1
        else:
            i += 1

    if not phrases:
        return ""

    norm = phrases[0].replace("ё", "е")
    return norm[:1].upper() + norm[1:]



def clean_requisite_entities(raw_ents, nlp, min_score: float = 0.4):
    """
    raw_ents: [{"entity","word","score"}, ...] для РЕКВИЗИТ.
    Возвращает список:
      {
        "lemma":   "Фонарик",            # нормализованный канон
        "surface": ["Фонарика", ...],    # исходные формы
        "score":   0.849                 # максимальный score в группе
      }
    """
    buckets = []
    vowels = set("аеёиоуыэюя")

    for ent in raw_ents:
        score = float(ent.get("score", 0.0))
        if score < min_score:
            continue

        phrase = (ent.get("word") or "").strip()
        if len(phrase) < 2:
            continue

        if not re.search(r"[А-Яа-яЁё]", phrase):
            continue

        # --- 1) нормализованный ключ через твою функцию ---
        norm = normalize_phrase_adj_noun(phrase, nlp)   # ← вот тут "Фонарика" должна стать "Фонарик"
        if not norm:
            continue

        norm_low = norm.lower()
        # минимальная защита от совсем странных ключей
        if len(norm_low) < 3 or not any(ch in vowels for ch in norm_low):
            continue

        buckets.append(
            {
                "lemma": norm,        # "Фонарик"
                "surface": phrase,    # "Фонарика" / "Фонарик"
                "score": score,
            }
        )

    if not buckets:
        return []

    # 2) склейка по базе леммы (если хочешь объединять ФОНАРЬ/ФОНАРИК)
    cluster_map = defaultdict(list)
    for item in buckets:
        base = _lemma_base(item["lemma"].lower())   # например, "фонар"
        cluster_map[base].append(item)

    cleaned = []
    for base, items in cluster_map.items():
        # выбираем лучшую форму по score
        best = max(items, key=lambda x: x["score"])
        canon = best["lemma"]     # уже "Фонарик"

        surfaces = []
        scores   = []
        for it in items:
            if it["surface"] not in surfaces:
                surfaces.append(it["surface"])
            scores.append(it["score"])

        cleaned.append(
            {
                "lemma": canon,                # что уйдёт в таблицу
                "surface": surfaces,           # какие формы встретились
                "score": round(max(scores), 3),
            }
        )

    cleaned.sort(key=lambda x: -x["score"])
    return cleaned


# -------------------- Данные: упоминания и персонажи --------------------

@dataclass
class Mention:
    text: str                  # сырой текст упоминания
    kind: str                  # 'dialog', 'header', 'ner'
    span: Tuple[int, int]      # (start_char, end_char) в пределах сцены
    line_idx: int              # номер строки (0-based)
    lemmas: Tuple[str, ...]    # леммы spaCy (нижний регистр)
    is_anchor: bool = False    # диалог/шапка → сильный сигнал


@dataclass
class Character:
    id: int
    canonical_name: str
    aliases: Set[str] = field(default_factory=set)
    is_main: bool = True               # есть ли диалоговое имя
    source: str = ""                   # 'dialog', 'header', 'mixed', ...
    confidence: float = 1.0


# -------------------- Шаг 1. Извлечение диалоговых имён --------------------

def extract_dialog_speakers(
    text: str,
    nlp,
) -> List[Tuple[str, int, int, int]]:
    """
    Находит КАПС-строки-спикеры.
    Возвращает список (name, line_idx, start_char, end_char).

    Считаем строку персонажем, если:
      - это КАПС, не заголовок и не сервис,
      - в строке нет .?!…,:,
      - и ЛИБО есть PROPN в анализе этой строки,
        ЛИБО её токены встречаются в NER-PER по всей сцене,
        ЛИБО её токены встречаются в сцене больше 1 раза.
    """

    # прогоняем весь текст один раз
    doc_full = nlp(text)

    # частоты токенов по сцене
    token_freq: Dict[str, int] = {}
    for tok in doc_full:
        if tok.is_alpha:
            key = tok.text.lower()
            token_freq[key] = token_freq.get(key, 0) + 1

    # множество имён из NER (PER)
    per_tokens: Set[str] = set()
    for ent in doc_full.ents:
        if ent.label_ != "PER":
            continue
        for t in ent:
            if t.is_alpha:
                per_tokens.add(t.text.lower())

    speakers: List[Tuple[str, int, int, int]] = []

    lines = text.splitlines(keepends=True)
    offset = 0
    for i, raw in enumerate(lines):
        line = raw.rstrip("\n")

        # 1. КАПС + не заголовок сцены
        if not _is_caps_line(line) or _looks_like_header(line):
            offset += len(raw)
            continue

        # 2. сервис/монтаж
        if _is_service_caps_line(line):
            offset += len(raw)
            continue

        cleaned = _clean_caps_name(line)
        if not cleaned:
            offset += len(raw)
            continue

        # 🔹 отрезаем всё, что в круглых скобках: "ТИМУР (З/К, ШУТЛИВО)" → "ТИМУР"
        cleaned = re.sub(r"\(.*", "", cleaned).strip()
        if not cleaned:
            offset += len(raw)
            continue

        up = cleaned.upper()
        lo = cleaned.lower()

        # 3. если есть .?!…,: — это текст реплики, а не имя
        if any(ch in cleaned for ch in ".!?…,:"):
            offset += len(raw)
            continue

        # 4. строки с запятыми — это шапки со списками имён, разберём отдельно
        if "," in cleaned:
            offset += len(raw)
            continue

        # 5. монтажные маркеры
        if lo in TRANSITION_MARKERS or up in HEADER_TOKENS or up in SERV_MARKERS:
            offset += len(raw)
            continue

        parts = [p for p in re.split(r"[,\s]+", cleaned) if p]
        if len(parts) >= 2 and parts[-1].upper() in SPEECH_MODIFIERS:
            offset += len(raw)
            continue

        # --- главный фильтр: PROPN / NER / частота ---
        doc_line = nlp(cleaned)
        tokens_alpha = [t for t in doc_line if t.is_alpha]
        tokens_lower = [t.text.lower() for t in tokens_alpha]

        # 0) Явный мусор типа "КОНЕЦ ПЕРВОЙ СЕРИИ" — как у тебя уже есть
        if any(w in tokens_lower for w in ("конец", "серии", "серия", "сезона", "сезон")):
            offset += len(raw)
            continue

        # 🔹 1) одиночные стоп-слова: ГРОМКО, ШУТЛИВО, ВВЕРХУ, ВСЕМ
        if len(tokens_alpha) == 1 and tokens_lower[0] in STOP_SINGLE:
            offset += len(raw)
            continue

        # 🔹 2) короткие предлоговые конструкции: "НА СТЕНА", "НА СТЕНЕ", "В ВЕРХУ"
        if len(tokens_alpha) <= 2 and tokens_lower and tokens_lower[0] in PREPOSITIONS:
            offset += len(raw)
            continue


        has_propn = any(t.pos_ == "PROPN" for t in tokens_alpha)
        has_ner_match = any(tok in per_tokens for tok in tokens_lower)
        has_freq = any(token_freq.get(tok, 0) > 1 for tok in tokens_lower)

        # 🔹 НОВОЕ: ролевые "головы" — ЧИНОВНИК, СОТРУДНИК и т.п.
        # если это одна такая роль + номер → тоже считаем валидным спикером
        head = tokens_alpha[0].text.upper() if tokens_alpha else ""
        is_role_speaker = (
            head in ROLE_SPEAKER_HEADS
            and len(tokens_alpha) <= 2   # ЧИНОВНИК или ЧИНОВНИК 1
        )

        if not (has_propn or has_ner_match or has_freq or is_role_speaker):
            offset += len(raw)
            continue

        # если дошли сюда — это персонаж
        start_char = offset
        end_char = offset + len(line)
        speakers.append((cleaned, i, start_char, end_char))

        offset += len(raw)

    return speakers

# -------------------- Шаг 2. Имена из "шапки" кастинга --------------------

def extract_prim_names(text: str) -> List[Tuple[str, int, int, int]]:
    """
    Ищем блоки вида:
      (ПРИМ: ... Катя, Лев, Макс, Матвей, ...)

    Внутри блока вытаскиваем все слова вида [А-ЯЁ][а-яё]+
    и фильтруем очевидные служебные слова.

    Важное ограничение:
      - обрабатываем ТОЛЬКО те примечания, которые похожи
        на описание группы людей (есть 'компания', 'человек', цифры и т.п.).
    """

    prim_pat = re.compile(r"\(\s*ПРИМ[^)]*\)", flags=re.IGNORECASE | re.DOTALL)
    name_pat = re.compile(r"\b[А-ЯЁ][а-яё]+\b")

    # слова, которые точно не имена в таких примечаниях
    STOP = {
        "прим", "основная", "основной", "компания",
        "человек", "человеков", "человека", "людей", "чел",
        "плюс", "еще", "ещё",
        "которого", "которые", "который",
        "вернут", "сразу", "отстанут", "побега", "порог",
        "основная", "компания",
    }

    results: List[Tuple[str, int, int, int]] = []

    for m in prim_pat.finditer(text):
        block = m.group(0)          # '(ПРИМ: ... )'
        inner = block[1:-1]         # без внешних скобок
        inner_lower = inner.lower()

        # 🔹 Новый фильтр: обрабатываем только те ПРИМ, которые похожи
        #    на описание компании / группы людей
        if not re.search(r"\d", inner_lower) and not any(
            key in inner_lower
            for key in ("компан", "человек", "чел", "ребят", "подрост", "подростков")
        ):
            # пример: (ПРИМ: Цитата из «Как она меня выносит» Матанга)
            # тут нет ни цифр, ни 'компания', ни 'человек' → пропускаем целиком
            continue

        for nm in name_pat.finditer(inner):
            word = nm.group(0)
            if word.lower() in STOP:
                continue

            # глобальные позиции в тексте
            global_start = m.start() + nm.start()
            global_end = global_start + len(word)
            line_idx = text.count("\n", 0, global_start)

            results.append((word, line_idx, global_start, global_end))

    return results


def extract_header_names(
    text: str,
) -> List[Tuple[str, int, int, int]]:
    """
    Ищем строки-шапки с перечислением имён:
      СОКОЛОВ, КОМСОМОЛКА, СОТРУДНИК-1 ГРАЖДАНСКОЙ АВИАЦИИ, ...

    Возвращает список (name, line_idx, start_char, end_char),
    где name — один элемент шапки.

    Дополнительно:
      - выкидываем массовку 'ЧУКЧИ (10 ЧЕЛ)' и 'КОМСОМОЛКА (18–25)',
      - выкидываем шум: 'ГРОМКО', 'ШУТЛИВО', 'ВСЕМ', 'ВВЕРХУ',
      - выкидываем 'КОНЕЦ ПЕРВОЙ СЕРИИ' и подобные,
      - обрезаем служебное 'З/К', 'ЗК' и содержимое скобок: 'РИТА З/К' → 'РИТА'.
    """
    results: List[Tuple[str, int, int, int]] = []

    time_pat = re.compile(r"\(\s*\d{1,2}:\d{2}(?::\d{2})?\s*\)")
    lines = text.splitlines(keepends=True)
    offset = 0

    for i, raw in enumerate(lines):
        line = raw.rstrip("\n")
        s = line.strip()
        if not s:
            offset += len(raw)
            continue

        if re.match(r"^[A-ZА-ЯЁ0-9]+(?:\s+[A-ZА-ЯЁ0-9]+)*\s*\(", s):
            offset += len(raw)
            continue

        # убираем таймкод вида (01:10)
        s_head = time_pat.sub("", s)

        # нужна запятая: это указание на список
        if "," not in s_head:
            offset += len(raw)
            continue

        # достаточно ли строка "заглавная", как у шапки
        letters = re.sub(rf"[^A-Za-z{RUS_UP}{RUS_LO}Ёё]", "", s_head)
        if not letters:
            offset += len(raw)
            continue
        upper = sum(1 for ch in letters if ch == ch.upper())
        upper_ratio = upper / len(letters)
        if upper_ratio < 0.6:
            offset += len(raw)
            continue

        # отрежем заголовок сцены (до последней точки)
        chunk = s_head.split(".")[-1].strip() if "." in s_head else s_head

        # если есть МАССОВКА:, берём только левую часть до неё
        mass_split = re.split(r"\bМАССОВКА\b\s*:", chunk, maxsplit=1, flags=re.IGNORECASE)
        main_part = mass_split[0].strip()

        # если вся строка про конец серии/сезона — выкидываем целиком
        if re.search(r"\bКОНЕЦ\b.*\bСЕРИ", main_part, flags=re.IGNORECASE):
            offset += len(raw)
            continue

        # в main_part не должно быть нормальных предложений
        if any(p in main_part for p in [".", "!", "?", ":"]):
            offset += len(raw)
            continue

        for part in main_part.split(","):
            name = part.strip().strip(".:()")
            if not name:
                continue


            # 1) игнорируем чистую массовку 'ЧУКЧИ (10 ЧЕЛ)' прямо в шапке
            if re.search(
                r"\(\s*\d+\s*(?:чел|человек|человека)\s*\)",
                name,
                flags=re.IGNORECASE,
            ):
                continue

            # 2) игнорируем явно возраст: 'КОМСОМОЛКА (18-25)'
            if re.search(r"\(\s*\d+\s*[-–]\s*\d+\s*\)", name):
                continue

            # 3) чистим скобки: 'РИТА (ВСЕМ' → 'РИТА'
            name = re.sub(r"\(.*", "", name).strip()

            # 4) обрезаем служебное 'З/К', 'ЗК' и хвост после него:
            #    'РИТА З/К' → 'РИТА', 'ПАРЕНЬ 1 З/К' → 'ПАРЕНЬ 1'
            name = re.sub(r"\s+З\s*/?\s*[КK]\b.*", "", name, flags=re.IGNORECASE).strip()

            if not name:
                continue

            low_name = name.lower()

            # 5) явный мусор из финальных надписей, если вдруг долетел
            if ("конец" in low_name and "сери" in low_name) or "сезон" in low_name:
                continue

            tokens = [t for t in name.split() if t]
            if not tokens:
                continue

            if len(tokens) >= 2:
                first = tokens[0].lower()
                if re.search(r"(ет|ёт|ит|ал|ала|али|ют|ут|ешь|аешь|ает|яет)$", first):
                    # очень грубо, но 'бьет', 'говорит', 'идет', 'стреляет' сюда попадут
                    continue

            # лёгкая проверка на форму имени: слова должны быть или КАПС, или TitleCase
            ok_tokens = 0
            for t in tokens:
                if t.isupper() or t.istitle():
                    ok_tokens += 1
            if ok_tokens / len(tokens) < 0.7:
                # это что-то типа "ты не" и прочий мусор
                continue

            letters_name = re.sub(rf"[^A-Za-z{RUS_UP}{RUS_LO}Ёё]", "", name)
            if len(letters_name) < 2:
                continue

            # если дошли сюда — это нормальный элемент шапки
            start_in_line = line.index(name)
            start_char = offset + start_in_line
            end_char = start_char + len(name)
            results.append((name, i, start_char, end_char))

        offset += len(raw)

    return results



def _normalize_mass_label(label: str) -> str:
    """
    'Массовка - челюскинцы' -> 'Челюскинцы'
    'массовка: туристы'     -> 'Туристы'
    """
    s = _norm(label)
    # убираем префикс "массовка", "массовка -" и "массовка:"
    s = re.sub(r"(?i)^массовка\s*[-:–—]\s*", "", s)
    # нормализуем регистр: первая буква заглавная, остальное как в lower()
    if not s:
        return s
    s = s.lower()
    return s[:1].upper() + s[1:]


def _extract_massovka(text: str) -> Set[str]:
    ms = set()
    # A) “МАССОВКА: … (N)” — оставляем как есть
    for grp in re.findall(r"МАССОВКА[:\-\s]+(.+)", text, flags=re.IGNORECASE):
        for label, num in re.findall(
            rf"([{RUS_UP}{RUS_LO} \-]+?)\s*\(\s*(\d+)",
            grp
        ):
            ms.add(f"{_norm(label).capitalize()} ({int(num)})")

    # B) В любом месте текста — ТОЛЬКО если внутри скобок есть маркер людей
    for label, num in re.findall(
        rf"([{RUS_UP}{RUS_LO} \-]+?)\s*\(\s*(\d+)\s*(?:чел|человек|человека)\s*\)",
        text,
        flags=re.IGNORECASE,
    ):
        ms.add(f"{_norm(label).capitalize()} ({int(num)})")

    return ms

from typing import Tuple, Set


def _extract_massovka_and_grouping(text: str) -> Tuple[Set[str], Set[str]]:
    massovka = set()
    grouping = set()

    # A) "МАССОВКА: ЧУКЧИ (10 ЧЕЛ), ТЕХНИКИ (2 ЧЕЛ)"
    for grp in re.findall(r"МАССОВКА[:\-\s]+(.+)", text, flags=re.IGNORECASE):
        matches = re.findall(
            rf"([{RUS_UP}{RUS_LO} \-]+?)\s*\(\s*(\d+)\s*(?:чел|человек|человека)\s*\)",
            grp,
            flags=re.IGNORECASE,
        )
        for idx, (label, num) in enumerate(matches):
            norm_label = _normalize_mass_label(label)
            if not norm_label:
                continue
            item = f"{norm_label} ({int(num)})"
            if idx == 0:
                massovka.add(item)   # первый — массовка
            else:
                grouping.add(item)   # остальные — групповка

    # B) Остальные "ХХХ (N чел)" по всему тексту
    for label, num in re.findall(
        rf"([{RUS_UP}{RUS_LO} \-]+?)\s*\(\s*(\d+)\s*(?:чел|человек|человека)\s*\)",
        text,
        flags=re.IGNORECASE,
    ):
        norm_label = _normalize_mass_label(label)
        if not norm_label:
            continue
        item = f"{norm_label} ({int(num)})"
        if item in massovka or item in grouping:
            continue
        massovka.add(item)

    return massovka, grouping


def extract_implicit_massovka(
    text: str,
    object_: str,
    subobject: str,
    nlp,
    *,
    n_chars: int | None = None,
    min_chars: int = 3,
) -> set[str]:
    """
    Скрытая массовка:
      - ищем в тексте фоновые группы людей ('толпа', 'люди', 'туристы' и т.п.),
      - добавляем только если:
          * сцена в публичном месте (город / улица / площадь / парк / метро ...),
          * и именованных персонажей в сцене мало (n_chars <= min_chars),
            если n_chars передан.
    """

    # 1) если явно много персонажей — не добавляем скрытую массовку
    if n_chars is not None and n_chars > min_chars:
        return set()

    # 2) проверяем, что локация "публичная"
    loc_text = f"{object_ or ''} {subobject or ''}".strip()
    if loc_text:
        doc_loc = nlp(loc_text)
        loc_lemmas = {t.lemma_.lower() for t in doc_loc if t.is_alpha}
        if not (loc_lemmas & PUBLIC_LOCATION_LEMMAS):
            # не город/улица/площадь/метро и т.п. → не считаем массовку
            return set()
    else:
        # вообще нет объекта/подобъекта → лучше не придумывать массовку
        return set()

    # 3) ищем фоновые групповые сущности в тексте
    doc = nlp(text)
    found: set[str] = set()

    for tok in doc:
        if not tok.is_alpha:
            continue
        lemma = tok.lemma_.lower()
        if lemma in IMPLICIT_MASS_GROUP_LEMMAS:
            found.add(lemma.capitalize())  # 'толпа' → 'Толпа'

    return found

SMALL_GROUP_NUM_WORDS = r"(двое|трое|четверо|пятеро|шестеро|семеро|восьмеро|девятеро|десятеро)"

# групповые существительные, которые хотим считать "групповкой"
TEXT_GROUP_NOUNS = {
    "отряд",
    "участники",
    "участник",
    # сюда же можно добавить "группа", "команда" и т.п., если понадобится
}

def _extract_small_numeric_grouping(text: str, nlp=None) -> Set[str]:
    res: Set[str] = set()

    # 1) Числительное + существительное: "двое мужчин", "трое парней"...
    pattern = re.compile(
        rf"\b{SMALL_GROUP_NUM_WORDS}\s+([A-Za-zА-Яа-яЁё]+)",
        flags=re.IGNORECASE
    )

    for m in pattern.finditer(text):
        num_word = m.group(1)
        noun    = m.group(2)

        phrase = f"{num_word} {noun}".strip()
        phrase_norm = phrase[:1].upper() + phrase[1:].lower()
        res.add(phrase_norm)

    # 2) Одиночные групповые существительные: "отряд", "участники"/"участников" и т.п.
    lemma_groups: Set[str] = set()

    if nlp is not None:
        doc = nlp(text)
        for tok in doc:
            if not tok.is_alpha:
                continue
            lemma = tok.lemma_.lower()
            if lemma in TEXT_GROUP_NOUNS:
                lemma_groups.add(lemma)
    else:
        # fallback без spaCy: грубо ищем по тексту
        lower_text = text.lower()
        for lemma in TEXT_GROUP_NOUNS:
            # ищем любой "хвост" формы: участник/участники/участников..., отряд/отряда...
            if re.search(rf"\b{lemma}\w*\b", lower_text):
                lemma_groups.add(lemma)

    # 3) Добавляем каноны по леммам (одна форма на лемму)
    for lemma in lemma_groups:
        canon = TEXT_GROUP_CANON.get(lemma)
        if canon:
            res.add(canon)

    return res

# -------------------- Шаг 3. NER (только привязанные к героям) --------------------

def extract_ner_persons(
    text: str,
    nlp,
    anchor_names: List[str],
) -> List[Tuple[str, int, int, int]]:
    """
    Извлекаем NER-PER.

    Основной режим (если есть anchor_names):
      - только те, что лемматически связаны c anchor_names,
      - фильтруем описательные формы с повторяющейся фамилией,
      - срезаем ведущие предлоги: 'К Алисе' -> 'Алисе'.

    Fallback-режим (если якорей нет):
      - берём все PER с PROPN внутри,
      - срезаем ведущие предлоги и чистим имя.
    """
    doc = nlp(text)

    # --- подготовка line_offsets для вычисления номера строки ---
    lines = text.splitlines(keepends=True)
    line_offsets = []
    offset = 0
    for raw in lines:
        line_offsets.append(offset)
        offset += len(raw)

    def line_index_from_pos(pos: int) -> int:
        idx = 0
        for i, off in enumerate(line_offsets):
            if off <= pos:
                idx = i
            else:
                break
        return idx

    results: List[Tuple[str, int, int, int]] = []

    # --- 1) леммы якорных имён ---
    anchor_lemmas: Set[str] = set()
    anchor_last_tokens: Set[str] = set()
    for name in anchor_names:
        d = nlp(name)
        tokens = [t for t in d if t.is_alpha]
        for t in tokens:
            anchor_lemmas.add(t.lemma_.lower())
        if tokens:
            anchor_last_tokens.add(tokens[-1].text.lower())

    # === Fallback-режим: якорей нет → берём все PER ===
    if not anchor_lemmas:
        for ent in doc.ents:
            if ent.label_ != "PER":
                continue

            ent_tokens = [t for t in ent if t.is_alpha]
            if not ent_tokens:
                continue

            # 🔹 защита от глагольных "имён" типа "Бежит"
            # если одно слово и spaCy считает его VERB/AUX — выкидываем
            if len(ent_tokens) == 1 and ent_tokens[0].pos_ in ("VERB", "AUX"):
                continue

            has_propn = any(t.pos_ == "PROPN" for t in ent_tokens)

            # если нет PROPN, всё равно разрешаем однословные сущности,
            # которые начинаются с заглавной буквы и не являются глаголом:
            # Макс, Тимур, Матвей и т.п.
            if not has_propn:
                if not (len(ent_tokens) == 1 and ent_tokens[0].text[:1].isupper()):
                    continue

            # дальше как было: обрезка предлогов, _clean_person_name, добавление в results
            start_idx = 0
            while (
                start_idx < len(ent_tokens)
                and ent_tokens[start_idx].text.lower() in PREPOSITIONS
            ):
                start_idx += 1

            core_tokens = ent_tokens[start_idx:] if start_idx < len(ent_tokens) else ent_tokens
            if not core_tokens:
                continue

            core_text = " ".join(t.text for t in core_tokens)
            name_raw = _clean_person_name(core_text)
            if not name_raw:
                continue

            line_idx = line_index_from_pos(ent.start_char)
            results.append((name_raw, line_idx, ent.start_char, ent.end_char))

        return results



    # === Основной режим: есть anchor_lemmas → жёсткий фильтр по якорям ===
    for ent in doc.ents:
        if ent.label_ != "PER":
            continue
        if not any(t.pos_ == "PROPN" for t in ent):
            continue

        # связь по леммам с якорями
        ent_lemmas = {t.lemma_.lower() for t in ent if t.is_alpha}
        if not ent_lemmas & anchor_lemmas:
            continue

        # токены энтити, пригодные для анализа
        ent_tokens = [t for t in ent if t.is_alpha]

        # фильтр "повторяющаяся фамилия" для многословных описаний
        if len(ent_tokens) > 1:
            last_tok = ent_tokens[-1].text.lower()
            if last_tok in anchor_last_tokens:
                continue

        if len(ent_tokens) == 1:
            if ent_tokens[0].lemma_.lower() in anchor_lemmas:
                continue

        # --- обрезаем ведущие предлоги: 'К Алисе' -> 'Алисе' ---
        start_idx = 0
        while (
            start_idx < len(ent_tokens)
            and ent_tokens[start_idx].text.lower() in PREPOSITIONS
        ):
            start_idx += 1

        core_tokens = ent_tokens[start_idx:] if start_idx < len(ent_tokens) else ent_tokens
        if not core_tokens:
            continue

        # собираем текст из "ядра" (без предлогов)
        core_text = " ".join(t.text for t in core_tokens)
        name_raw = _clean_person_name(core_text)
        if not name_raw:
            continue

        line_idx = line_index_from_pos(ent.start_char)
        results.append((name_raw, line_idx, ent.start_char, ent.end_char))

    return results


# -------------------- Шаг 4. Нормализация и кластеризация имён --------------------

DIMINUTIVE_SUFFIXES = (
    "ша", "ка", "очка", "ечка", "енька", "юшка", "юша",
    "ик", "чик", "ёк", "ек", "юха", "юня", "уля", "инка", "онька"
)

PATRONYMIC_SUFFIXES = {"ыч", "ыча", "ич", "ича"}


def _nickname_matches_base(a: str, b: str) -> bool:
    """
    Прозвища / уменьшительные:
      - Иван ~ Иваныч
      - Арина ~ Ариша
      - Митя ~ Митька (потенциально)
    Работает только для однословных имён.
    """

    a_l, b_l = a.lower(), b.lower()
    if a_l == b_l:
        return False
    if len(a_l) < 3 or len(b_l) < 3:
        return False

    # 1) патронимические: Иван ~ Иваныч
    #   (длинная форма = короткая + суффикс из PATRONYMIC_SUFFIXES)
    for short, long in ((a_l, b_l), (b_l, a_l)):
        if len(long) <= len(short):
            continue
        if long.startswith(short):
            suf = long[len(short):]
            if suf in PATRONYMIC_SUFFIXES:
                return True

    # 2) уменьшительные: общий префикс ≥ 3 букв, а оставшийся хвост у одного — типичный
    #    diminutive суффикс, а у другого — "женский" хвост (а/я/на) или пусто.
    def common_prefix_len(x: str, y: str) -> int:
        n = min(len(x), len(y))
        i = 0
        while i < n and x[i] == y[i]:
            i += 1
        return i

    cp = common_prefix_len(a_l, b_l)
    if cp < 3:
        return False

    tail_a = a_l[cp:]
    tail_b = b_l[cp:]

    # допустимые "базовые" окончания у полного имени
    BASE_ENDINGS = {"", "а", "я", "на"}

    # вариант 1: a = база, b = уменьшительное
    if tail_a in BASE_ENDINGS and tail_b in DIMINUTIVE_SUFFIXES:
        return True
    # вариант 2: b = база, a = уменьшительное
    if tail_b in BASE_ENDINGS and tail_a in DIMINUTIVE_SUFFIXES:
        return True

    return False

def _same_name_case_variant(a: str, b: str) -> bool:
    """
    Падежные варианты одного и того же имени:
    Арина ~ Арину ~ Арине, Прохор ~ Прохора, Соколов ~ Соколова и т.п.

    Работает ТОЛЬКО для однословных имён.
    Идея:
      1) длинный общий префикс,
      2) различия только в хвосте из типичных падежных гласных/суффиксов.
    """
    a_l, b_l = a.lower(), b.lower()
    if len(a_l) < 3 or len(b_l) < 3:
        return False

    # 1) считаем длину общего префикса
    n = min(len(a_l), len(b_l))
    i = 0
    while i < n and a_l[i] == b_l[i]:
        i += 1

    # общий префикс должен покрывать хотя бы (min_len - 1) символ:
    # тогда отличаться будет только последний символ/суффикс
    if i < n - 1:
        return False

    # 2) проверяем, что отличающийся хвост — типичное падежное окончание
    bad_ends = (
        "а", "я", "ы", "и", "е", "ю", "у",
        "ой", "ей", "ом", "ем", "ою", "ею"
    )

    def strip_bad_end(s: str) -> str:
        for suf in bad_ends:
            if s.endswith(suf) and len(s) > len(suf) + 1:
                return s[:-len(suf)]
        # если нет "классического" окончания, попробуем просто убрать последнюю гласную
        vowels = "аеёиоуыэюя"
        if s[-1] in vowels and len(s) > 3:
            return s[:-1]
        return s

    stem_a = strip_bad_end(a_l)
    stem_b = strip_bad_end(b_l)

    # стемы должны совпадать и быть не слишком короткими
    if len(stem_a) < 3 or len(stem_b) < 3:
        return False

    return stem_a == stem_b

def build_clusters(
    mentions: List[Mention],
    nlp,
) -> List[Set[int]]:
    """
    Формируем кластеры имён по индексам mentions:
      - совпадение лемм,
      - включение однословного имени в ФИО,
      - простые прозвища типа "Иваныч"~"Иван".
    Возвращает список множеств индексов упоминаний.
    """
    n = len(mentions)
    if n == 0:
        return []

    # DSU
    parent = list(range(n))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i: int, j: int):
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[rj] = ri

    # предрасчёт нормализованных форм и лемм
    name_lemmas: List[Set[str]] = []
    tokens: List[List[str]] = []
    for m in mentions:
        names = [m.text for m in mentions]
        name_lemmas.append(set(m.lemmas))
        tokens.append([t.lower() for t in m.text.split()])

    docs = [nlp(m.text) for m in mentions]

    # 1) одинаковые множества лемм → склеиваем
    #    Для имён с цифрами:
    #      - НЕ склеиваем разные ('СОТРУДНИК' vs 'СОТРУДНИК 2'),
    #      - НО склеиваем полностью совпадающие ('ПОДЖИГАТЕЛЬ 2' vs 'ПОДЖИГАТЕЛЬ 2').
    for i in range(n):
        for j in range(i + 1, n):
            if not (name_lemmas[i] and name_lemmas[i] == name_lemmas[j]):
                continue

            ni = names[i].strip().lower()
            nj = names[j].strip().lower()

            if _has_digit(ni) or _has_digit(nj):
                # если оба с цифрами и полностью совпадают — склеиваем
                if ni == nj:
                    union(i, j)
                # иначе (разные цифры / база) — не трогаем
                continue

            # обычные имена без цифр — склеиваем как раньше
            union(i, j)


    # 2) однословное имя ⊂ ФИО (общая лемма первого токена)
    for i in range(n):
        for j in range(i + 1, n):
            if _has_digit(names[i]) or _has_digit(names[j]):
                continue
            toks_i, toks_j = tokens[i], tokens[j]

            # одно слово vs два и более
            if len(toks_i) == 1 and len(toks_j) >= 2:
                if mentions[i].lemmas and mentions[j].lemmas:
                    if mentions[i].lemmas[0] == mentions[j].lemmas[0]:
                        union(i, j)
            elif len(toks_j) == 1 and len(toks_i) >= 2:
                if mentions[i].lemmas and mentions[j].lemmas:
                    if mentions[j].lemmas[0] == mentions[i].lemmas[0]:
                        union(i, j)

    # 3) описательные алиасы: "Пьяная Котникова" ~ "КОТНИКОВА"
    #    однословное PROPN <-> многословное, где:
    #      - первый токен ADJ,
    #      - последний токен PROPN с той же леммой, что и однословное имя
    for i in range(n):
        for j in range(i + 1, n):
            toks_i, toks_j = tokens[i], tokens[j]
            doc_i,  doc_j  = docs[i],   docs[j]

            # вариант: i — однословное имя, j — "Пьяная Котникова"
            if len(toks_i) == 1 and len(toks_j) >= 2:
                if len(doc_i) == 1 and doc_i[0].pos_ == "PROPN":
                    first_j = doc_j[0]
                    last_j  = doc_j[-1]
                    if (
                        first_j.pos_ == "ADJ"
                        and last_j.pos_ == "PROPN"
                        and last_j.lemma_.lower() == doc_i[0].lemma_.lower()
                    ):
                        union(i, j)
                        continue

            # симметричный вариант: j — однословное имя, i — "Пьяная Котникова"
            if len(toks_j) == 1 and len(toks_i) >= 2:
                if len(doc_j) == 1 and doc_j[0].pos_ == "PROPN":
                    first_i = doc_i[0]
                    last_i  = doc_i[-1]
                    if (
                        first_i.pos_ == "ADJ"
                        and last_i.pos_ == "PROPN"
                        and last_i.lemma_.lower() == doc_j[0].lemma_.lower()
                    ):
                        union(i, j)
                        continue


    # 3) прозвища 'Иваныч' ~ 'Иван'
    for i in range(n):
        for j in range(i + 1, n):
            t_i = tokens[i]
            t_j = tokens[j]
            if len(t_i) == 1 and len(t_j) == 1:
                a, b = t_i[0], t_j[0]
                if _nickname_matches_base(a, b) or _nickname_matches_base(b, a):
                    union(i, j)

    # 4) падежные варианты одного имени: Прохор ~ Прохора
    for i in range(n):
        for j in range(i + 1, n):
            t_i = tokens[i]
            t_j = tokens[j]
            if len(t_i) == 1 and len(t_j) == 1:
                if _same_name_case_variant(t_i[0], t_j[0]):
                    union(i, j)

    # собираем кластеры
    clusters_map: Dict[int, Set[int]] = {}
    for i in range(n):
        r = find(i)
        clusters_map.setdefault(r, set()).add(i)

    return list(clusters_map.values())

import re

def choose_canonical_for_cluster(
    cluster: list[int],
    mentions: list,
    freq: dict[str, int],
) -> tuple[str, bool, str, float]:
    """
    Выбираем канон и метаданные для одного кластера.

    ЛОГИКА:
      1) Канон берём ТОЛЬКО из якорей (КАПС-спикеры и шапки), если они есть.
      2) Если якорей нет — fallback: выбираем из любых упоминаний кластера.
      3) По возможности НЕ используем каноном строки, где несколько имён сразу:
         'ГЕНА, КАТЯ', 'ГЕНЫ И КАТИ' и т.п.
      4) Приоритет: dialog > header > prim > ner,
         потом частота, потом «простота» формы.
    """

    if not cluster:
        return "", False, "", 0.0

    # --- вспомогательные функции ---

    def is_multi_person_name(name: str) -> bool:
        """
        Признаки "многоголового" имени:
          - есть 'и' между двумя Capitalized словами,
          - или есть запятая и как минимум два слова вида [А-ЯЁ][а-яё]+.
        """
        # два слова "Имя и Имя"
        if re.search(r"\b[А-ЯЁ][а-яё]+\b\s+и\s+\b[А-ЯЁ][а-яё]+\b", name):
            return True
        # или "Имя, Имя"
        caps_words = re.findall(r"\b[А-ЯЁ][а-яё]+\b", name)
        if "," in name and len(caps_words) >= 2:
            return True
        return False

    def kind_rank(k: str) -> int:
        # чем больше — тем приоритетнее
        return {
            "dialog": 3,
            "header": 2,
            "prim":   1,
            "ner":    0,
        }.get(k, 0)

    def score(idx: int) -> tuple:
        m = mentions[idx]
        name = m.text or ""
        # отрицательный флаг для многолюдных имён
        multi = is_multi_person_name(name)
        # количество "слов" в имени
        tokens = re.findall(r"\b[A-Za-zА-Яа-яЁё]+\b", name)
        return (
            kind_rank(getattr(m, "kind", "")),  # 1) dialog > header > ...
            0 if multi else 1,                  # 2) одиночное имя лучше многоголового
            freq.get(name, 1),                  # 3) чаще встречающееся лучше
            -len(tokens),                       # 4) меньше слов → предпочтительнее
            -len(name),                         # 5) короче строка → предпочтительнее
            -getattr(m, "line_idx", 0),         # 6) раньше в тексте → чуть лучше
        )

    # --- 1) делим mentions на якорные и остальные ---

    anchor_idxs = [
        i for i in cluster
        if getattr(mentions[i], "is_anchor", False)
        and getattr(mentions[i], "kind", "") in ("dialog", "header")
    ]

    non_anchor_idxs = [i for i in cluster if i not in anchor_idxs]

    # --- 2) кандидаты для канона ---

    candidates = anchor_idxs or non_anchor_idxs

    # из кандидатов по возможности убираем "ГЕНА, КАТЯ" и пр.
    single_person_candidates = [
        i for i in candidates if not is_multi_person_name(mentions[i].text or "")
    ]
    if single_person_candidates:
        candidates = single_person_candidates

    # --- 3) выбираем лучший по score ---

    best_idx = max(candidates, key=score)
    best_mention = mentions[best_idx]
    canonical = best_mention.text or ""

    # --- 4) метка is_main: если есть диалоговый спикер в кластере ---
    is_main = any(
        getattr(mentions[i], "kind", "") == "dialog"
        for i in cluster
    )

    # --- 5) source: из каких типов якорей пришли упоминания в кластере ---
    src_bits = set()
    for i in cluster:
        m = mentions[i]
        if getattr(m, "is_anchor", False):
            src_bits.add(getattr(m, "kind", ""))
    source = "+".join(sorted(src_bits)) if src_bits else "other"

    # --- 6) confidence: примитивно, но честно ---
    if anchor_idxs:
        conf = 1.0   # есть якоря → уверены в каноне
    else:
        conf = 0.7   # только неякорные упоминания

    return canonical, is_main, source, conf


# -------------------- Главная функция: извлечь героев сцены --------------------
def cleanup_char_name(name: str) -> str | None:
    """
    Чистит сырое имя персонажа из шапок/списков:
    - обрезает З/К / ЗК,
    - убирает скобки,
    - выбрасывает 'КОНЕЦ ПЕРВОЙ СЕРИИ' и подобное,
    - выбрасывает конструкции типа 'БЬЕТ ТИМУРА'.

    Возвращает:
      - очищенное имя, или
      - None, если это мусор.
    """
    if not name:
        return None

    s = name.strip()

    # убираем всё в скобках: 'РИТА (ВСЕМ' -> 'РИТА'
    s = re.sub(r"\(.*", "", s).strip()

    # обрезаем служебное З/К / ЗК (кириллица/латиница) и хвост после него:
    # 'МИША З/К' -> 'МИША', 'ПАРЕНЬ 1 З/К' -> 'ПАРЕНЬ 1'
    s = re.sub(r"\s+З\s*/?\s*[КK]\b.*", "", s, flags=re.IGNORECASE).strip()

    if not s:
        return None

    low = s.lower()

    # явные финальные подписи — 'КОНЕЦ ПЕРВОЙ СЕРИИ', 'КОНЕЦ СЕРИИ', 'КОНЕЦ СЕЗОНА'
    if ("конец" in low and "сери" in low) or "сезон" in low:
        return None

    # грубая эвристика против 'БЬЕТ ТИМУРА' и т.п.:
    parts = s.split()
    if len(parts) >= 2:
        first = parts[0].lower()
        # если первое слово выглядит как глагол (бьет, идет, говорит, стреляет...)
        if re.search(r"(ет|ёт|ит|ал|ала|али|ют|ут|ешь|аешь|ает|яет)$", first):
            return None

    # после всех чисток возвращаем, если что-то осталось
    return s if s.strip() else None

def extract_scene_characters(
    scene_text: str,
    nlp=None,
) -> List[Character]:
    """
    Основная функция для одной сцены.
    На вход: текст сцены (с заголовком, описанием, диалогами).
    На выход: список Character с канонами и алиасами.
    """
    nlp = nlp or load_ru()

    def _to_nom_caps(name: str) -> str:
        """
        Приводит имя к чему-то похожему на именительный падеж и КАПС:
          - по умолчанию берём лемму spaCy,
          - если слово стоит НЕ в именительном падеже,
            аккуратно переписываем типичные «косвенные» формы имён:
              Сашей → Саша, Геной → Гена, Риту → Рита, Кати → Катя,
          - всё приводим к ВЕРХНЕМУ РЕГИСТРУ.
        """
        if not name:
            return ""

        doc_name = nlp(name)
        parts = []

        for t in doc_name:
            if not t.is_alpha:
                parts.append(t.text)
                continue

            surf = t.text
            surf_low = surf.lower()
            lemma_low = t.lemma_.lower()

            # по умолчанию берём лемму
            base = lemma_low

            # смотрим падеж
            cases = t.morph.get("Case")
            is_nom = "Nom" in cases  # True, если есть именительный

            # только если слово НЕ в именительном падеже,
            # пытаемся «откатить» типичные косвенные формы
            if not is_nom:
                # Типичные формы типа "Сашей", "Геной" → "Саша", "Гена"
                if len(surf_low) > 3 and surf_low.endswith(("ой", "ей", "ёй")):
                    base = surf_low[:-2] + "а"
                # Формы на "у/ю": "Риту", "Катю" → "Рита", "Катя"
                elif len(surf_low) > 3 and surf_low.endswith(("у", "ю")):
                    base = surf_low[:-1] + "а"
                # Генитив/дательный "Кати" → "Катя"
                elif len(surf_low) > 3 and surf_low.endswith("и"):
                    base = surf_low[:-1] + "я"

            parts.append(base)

        norm = " ".join(parts).strip()
        norm = norm.replace("ё", "е").replace("Ё", "Е")
        return norm.upper()


    # 1) КАПС-спикеры
    dialog_raw = extract_dialog_speakers(scene_text, nlp)
    # 2) имена из шапки
    header_raw = extract_header_names(scene_text)

    prim_raw = extract_prim_names(scene_text)

    # 3) NER, привязанный к якорям
    anchor_names = [name for (name, *_rest) in dialog_raw] + [
        name for (name, *_rest) in header_raw
    ]
    ner_raw = extract_ner_persons(scene_text, nlp, anchor_names)

    # 4) Собираем mentions
    mentions: List[Mention] = []

    # вспомогательно: считаем частоты форм
    freq: Dict[str, int] = {}

    def add_mention(name: str, kind: str, line_idx: int, start: int, end: int, is_anchor: bool):
        # 0) сначала санитизируем сырое имя
        base = cleanup_char_name(name)
        if not base:
            return

        # 1) дальше твоя обычная очистка
        name_clean = _clean_person_name(base)
        if not name_clean:
            return

        doc = nlp(name_clean)
        lemmas = tuple(t.lemma_.lower() for t in doc if t.is_alpha)
        if not lemmas:
            return

        m = Mention(
            text=name_clean,
            kind=kind,
            span=(start, end),
            line_idx=line_idx,
            lemmas=lemmas,
            is_anchor=is_anchor,
        )
        mentions.append(m)
        freq[name_clean] = freq.get(name_clean, 0) + 1


    for name, line_idx, start, end in dialog_raw:
        add_mention(name, "dialog", line_idx, start, end, True)

    for name, line_idx, start, end in header_raw:
        add_mention(name, "header", line_idx, start, end, True)

    for name, line_idx, start, end in prim_raw:
        add_mention(name, "prim", line_idx, start, end, False)

    for name, line_idx, start, end in ner_raw:
        add_mention(name, "ner", line_idx, start, end, False)

    if not mentions:
        return []

    # 5) Кластеры ко-референции по именам
    clusters = build_clusters(mentions, nlp)

    # 6) Для каждого кластера выбираем канон и алиасы
    characters: List[Character] = []
    for cid, cluster in enumerate(clusters):
        canonical, is_main, src, conf = choose_canonical_for_cluster(
            cluster, mentions, freq
        )
        aliases = {mentions[i].text for i in cluster if mentions[i].text != canonical}
        characters.append(
            Character(
                id=cid,
                canonical_name=canonical,
                aliases=aliases,
                is_main=is_main,
                source=src,
                confidence=conf,
            )
        )

    # --- НОРМАЛИЗАЦИЯ: ИМЕНИТЕЛЬНЫЙ ПАДЕЖ + КАПС ---
    for ch in characters:
        if ch.canonical_name:
            ch.canonical_name = _to_nom_caps(ch.canonical_name)
        else:
            ch.canonical_name = ""

        if ch.aliases:
            ch.aliases = {_to_nom_caps(a) for a in ch.aliases}
        else:
            ch.aliases = set()

    # сортировка по "важности" (диалоговые вперёд)
    characters.sort(key=lambda c: (not c.is_main, -c.confidence, c.canonical_name))

    return characters
    
def extract_scene_entities(
    scene_text: str,
    object_: str = "",
    subobject: str = "",
    nlp=None,
):
    """
    Высокоуровневая функция:
      - characters: список Character (герои с каноном и алиасами),
      - grouping:   технарские/групповые сущности (например, 'Техники (2)'),
      - massovka:   массовка (например, 'Чукчи (10)', 'Толпа').
    """
    nlp = nlp or load_ru()

    # 1) персонажи
    characters = extract_scene_characters(scene_text, nlp)

    try:
        main_chars = [c for c in characters if getattr(c, "is_main", True)]
        n_chars = len(main_chars)
    except Exception:
        n_chars = len(characters)

    # 2) явная массовка и групповка
    massovka_explicit, grouping = _extract_massovka_and_grouping(scene_text)

    # 3) скрытая массовка — только фон (толпа, люди, туристы...) в публичных локациях
    implicit_mass = extract_implicit_massovka(
        scene_text,
        object_=object_,
        subobject=subobject,
        nlp=nlp,
        n_chars=n_chars,
        min_chars=3,
    )

    massovka_all = sorted(set(massovka_explicit) | implicit_mass)

    small_groups = _extract_small_numeric_grouping(scene_text)
    grouping = set(grouping) | small_groups

    return {
        "characters": characters,
        "grouping": sorted(grouping),
        "massovka": massovka_all,
    }

MODEL_DIR = resource_path("ner_rubert_best_1763076515/ner_rubert_best")

# ==== 3. Парсер следующих колонок: использование НН ====
ner_pipe = pipeline(
    "token-classification",
    model=str(MODEL_DIR),
    tokenizer=str(MODEL_DIR),
    aggregation_strategy=None,
    device=0
    #device=get_device()
)

# === 1️⃣ Параметры ===
#MODEL_PATH = r"D:\WINK_1\ner_rubert_best_1763076515\ner_rubert_best"
MODEL_PATH = MODEL_DIR

entity_cols = [
    'Грим', 'Костюм', 'Реквизит', 'Декорация',
    'Пиротехника', 'Каскадер', 'Спецэффект',
]

tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
device = 'cuda' if torch.cuda.is_available() else 'cpu'


# === 2️⃣ Извлечение сущностей из одного текста ===
def extract_entities_from_text(text, model, threshold=0.2, max_length=512):
    # токенизация с "оконцами"
    inputs = tokenizer(
        text,
        return_overflowing_tokens=True,
        stride=50,
        max_length=max_length,
        truncation=True,
        return_offsets_mapping=True
    )

    all_results = []

    special_tokens = {"[CLS]", "[SEP]", "[PAD]"}

    for i in range(len(inputs["input_ids"])):
        input_ids      = torch.tensor([inputs["input_ids"][i]]).to(device)
        attention_mask = torch.tensor([inputs["attention_mask"][i]]).to(device)
        offsets        = inputs["offset_mapping"][i]   # <<< важное

        with torch.no_grad():
            outputs = model(input_ids, attention_mask=attention_mask)
            logits  = outputs.logits
            probs   = torch.softmax(logits, dim=-1)
            scores, preds = torch.max(probs, dim=-1)

        tokens = tokenizer.convert_ids_to_tokens(inputs["input_ids"][i])

        # текущее состояние сущности
        current_entity = None
        span_start     = None
        span_end       = None
        current_scores = []

        for token, offset, pred_id, score in zip(
            tokens,
            offsets,
            preds[0].cpu().numpy(),
            scores[0].cpu().numpy()
        ):
            start_char, end_char = offset

            # пропускаем спец-токены и позиции без оффсета
            if token in special_tokens or (start_char == 0 and end_char == 0):
                continue

            label = model.config.id2label[pred_id]
            label = label.replace("B-", "").replace("I-", "")

            # не сущность → закрываем, если что-то шло
            if label == "O":
                if current_entity is not None and span_start is not None and span_end is not None:
                    span_text = text[span_start:span_end].strip()
                    if len(span_text) > 1:
                        avg_score = round(sum(current_scores) / len(current_scores), 3)
                        all_results.append({
                            "entity": current_entity,
                            "word": span_text,
                            "score": avg_score,
                        })
                # сбрасываем состояние
                current_entity = None
                span_start     = None
                span_end       = None
                current_scores = []
                continue

            # продолжается та же сущность
            if label == current_entity:
                # расширяем правую границу
                span_end = end_char
                current_scores.append(score)
            else:
                # началась новая сущность → закрываем старую
                if current_entity is not None and span_start is not None and span_end is not None:
                    span_text = text[span_start:span_end].strip()
                    if len(span_text) > 1:
                        avg_score = round(sum(current_scores) / len(current_scores), 3)
                        all_results.append({
                            "entity": current_entity,
                            "word": span_text,
                            "score": avg_score,
                        })
                # открываем новую
                current_entity = label
                span_start     = start_char
                span_end       = end_char
                current_scores = [score]

        # закрываем последнюю сущность в чанке
        if current_entity is not None and span_start is not None and span_end is not None:
            span_text = text[span_start:span_end].strip()
            if len(span_text) > 1:
                avg_score = round(sum(current_scores) / len(current_scores), 3)
                all_results.append({
                    "entity": current_entity,
                    "word": span_text,
                    "score": avg_score,
                })

    # убираем дубли (перекрывающиеся окна и т.п.)
    df = pd.DataFrame(all_results)
    if not df.empty:
        df = df.drop_duplicates(subset=["entity", "word"])

    return df.to_dict("records")


DIMINUTIVE_SUFFIXES = ("ик", "чик", "щик", "ок", "ек", "ечек", "ушк", "юшк", "ишк")

def _lemma_base(lemma: str) -> str:
    """
    Строим более «грубую» базу леммы для склейки похожих предметов:
      фонарик / фонарь → фонар
      нож / ножик → нож  (при желании можно НЕ склеивать, если это критично)
    """
    s = lemma.lower().replace("ё", "е")
    # убираем конечный мягкий знак
    if s.endswith("ь"):
        s = s[:-1]
    # снимаем уменьшительные суффиксы
    for suf in DIMINUTIVE_SUFFIXES:
        if s.endswith(suf) and len(s) > len(suf) + 1:
            s = s[: -len(suf)]
            break
    return s

def clean_requisite_entities(raw_ents, nlp, min_score: float = 0.4):
    """
    raw_ents: список dict'ов вида {"entity","word","score"} для классов РЕКВИЗИТ/ОБЪЕКТ/ПОДОБЪЕКТ.
    Возвращает список словарей:
        {
          "lemma":   "Фонарик",           # канон по группе
          "surface": ["фонарика", ...],  # формы из текста
          "score":   0.849               # max по группе
        }
    """
    buckets = []
    vowels = set("аеёиоуыэюя")

    for ent in raw_ents:
        score = float(ent.get("score", 0.0))
        if score < min_score:
            continue

        phrase = (ent.get("word") or "").strip()
        if len(phrase) < 2:
            continue

        # только кириллица + пробелы/дефисы
        if not re.search(r"[А-Яа-яЁё]", phrase):
            continue

        # ---- 0) фильтр совсем короткого мусора (Шки и подобное) ----
        if len(phrase) <= 3:
            doc_short = nlp(phrase)
            toks_short = [t for t in doc_short if t.is_alpha]
            # если нет нормальных токенов — выкидываем
            if not toks_short:
                continue
            # если все токены OOV и слово короткое — почти наверняка шум
            if all(t.is_oov for t in toks_short):
                continue

        doc = nlp(phrase)
        tokens = [t for t in doc if t.is_alpha]
        if not tokens:
            continue

        # ищем существительные / имена
        nouns = [t for t in tokens if t.pos_ in {"NOUN", "PROPN"}]
        if not nouns:
            continue

        head = nouns[-1]
        lemma_raw = ru_lemma(head.text).replace("ё", "е")

        # защита от совсем обрезанных лемм
        if len(lemma_raw) < 3 or not any(ch in vowels for ch in lemma_raw):
            continue

        canon = lemma_raw[:1].upper() + lemma_raw[1:]

        buckets.append(
            {
                "lemma": canon,        # "Фонарик", "Фонарь", "Банк"
                "lemma_raw": lemma_raw,  # "фонарик"/"фонарь"/"банк"
                "surface": phrase,     # как в тексте
                "score": score,
            }
        )

    if not buckets:
        return []

    # 2) объединяем по «базе» леммы, чтобы склеивать Фонарик/Фонарь/Фонарика
    cluster_map = defaultdict(list)
    for item in buckets:
        base = _lemma_base(item["lemma_raw"])  # например, "фонар"
        cluster_map[base].append(item)

    cleaned = []
    for base, items in cluster_map.items():
        # канон — по наибольшему score
        best = max(items, key=lambda x: x["score"])
        canon = best["lemma"]

        surfaces = []
        scores   = []
        for it in items:
            if it["surface"] not in surfaces:
                surfaces.append(it["surface"])
            scores.append(it["score"])

        cleaned.append(
            {
                "lemma": canon,                 # что покажем в таблице
                "surface": surfaces,            # какие формы встретились
                "score": round(max(scores), 3),
            }
        )

    cleaned.sort(key=lambda x: -x["score"])
    return cleaned

# === 3️⃣ Обработка DataFrame ===
def process_dataframe(df, model, nlp, text_col="text", threshold=0.5):
    """
    df        — датафрейм со сценами
    model     — твоя NER-модель
    nlp       — spaCy (load_ru())
    text_col  — колонка с текстом сцены
    threshold — минимальный score для записи сущности
    """
    REKV_LABELS = {"РЕКВИЗИТ"}  # как в id2label после обрезки B-/I-

    results = []

    for _, row in df.iterrows():
        text = str(row[text_col])

        # 1) все сущности из нейросети
        ents = extract_entities_from_text(text, model, threshold=threshold)

        # 2) делим на реквизит и остальные
        req_ents   = [e for e in ents if e["entity"] in REKV_LABELS]
        other_ents = [e for e in ents if e["entity"] not in REKV_LABELS]

        # 3) заготовка строки: все NN-колонки + text
        row_result = {col: "" for col in entity_cols}
        row_result["text"] = text

        # 4) заполняем ВСЕ КОЛОНКИ, кроме "Реквизит", как раньше
        for ent in other_ents:
            col = ent["entity"]
            for target_col in entity_cols:
                # "Реквизит" пропускаем, он будет ниже
                if target_col == "Реквизит":
                    continue

                if col.lower() in target_col.lower().replace(".", "").replace("_", " "):
                    if row_result[target_col]:
                        row_result[target_col] += ", "
                    row_result[target_col] += f"{ent['word']} ({ent['score']:.3f})"
                    break

        # 5) теперь аккуратно обрабатываем именно реквизит
        clean_rekv = clean_requisite_entities(
            req_ents,
            nlp=nlp,
            min_score=threshold,
        )

        if clean_rekv:
            # можешь менять формат вывода как тебе удобно
            row_result["Реквизит"] = "; ".join(
                f"{item['lemma']}"
                for item in clean_rekv
            )
        else:
            row_result["Реквизит"] = ""

        results.append(row_result)

    return pd.DataFrame(results)

model = AutoModelForTokenClassification.from_pretrained(MODEL_PATH).to(device)


def extract_game_transport_for_scene(
    object_: str,
    subobject: str,
    text: str,
    nlp,
) -> list[str]:
    """
    Возвращает список канонических названий транспорта (МАШИНА, АВТОБУС, ...)
    по данным сцены.
    Источники:
      - object / subobject (шапка),
      - текст сцены (описания / ремарки / диалоги).
    """
    found: set[str] = set()

    def scan_chunk(chunk: str):
        if not chunk:
            return
        doc = nlp(chunk)
        for tok in doc:
            if not tok.is_alpha:
                continue
            lemma = tok.lemma_.lower()
            for canon, lemmas in TRANSPORT_MAP.items():
                if lemma in lemmas:
                    found.add(canon)
                    break

    # 1) сначала шапка — здесь обычно явный транспорт: "МАШИНА ГЕНЫ", "АВТОБУС"
    scan_chunk(object_ or "")
    scan_chunk(subobject or "")

    # 2) потом текст сцены — "подъезжает автобус", "садятся в машину"
    scan_chunk(text or "")

    # возвращаем отсортированный список канонов (верхний регистр уже заложен)
    return sorted(found)


def extract_grim_from_text(text: str, nlp) -> list[str]:
    """
    Rule-based извлечение грима/макияжа/ран на лице из текста сцены.
    Работает независимо от нейронки.
    Возвращает список фраз (уникальных), уже человеко-понятных.
    """
    if not text:
        return []

    doc = nlp(text)
    candidates: set[str] = set()

    for token in doc:
        # пропускаем чистую пунктуацию/цифры, но НЕ режем по дефису
        if not any(ch.isalpha() for ch in token.text):
            continue

        lemma_raw = ru_lemma(token.text)        # 👈 pymorphy
        lemma = lemma_raw.replace("ё", "е")
        text_norm = token.text.lower().replace("ё", "е")

        # 1) обычное совпадение по словарю
        in_noun_dict = (lemma in GRIM_NOUN_LEMMAS_NORM)
        in_adj_dict  = lemma in GRIM_ADJ_LEMMAS_NORM

        # 2) совпадение по корню: всё, что начинается с "тату-"
        from_root = any(
            lemma.startswith(root) or text_norm.startswith(root)
            for root in GRIM_NOUN_LEMMAS
        )

        is_grim_noun = in_noun_dict or from_root
        is_grim_adj  = in_adj_dict

        if not (is_grim_noun or is_grim_adj):
            continue

        # --- строим небольшую фразу вокруг токена ---
        span_tokens = {token}

        # если это прилагательное, цепляем голову-существительное
        if is_grim_adj and token.head.pos_ in ("NOUN", "PROPN") and token.head.sent == token.sent:
            span_tokens.add(token.head)
            # добавим ещё другие прилагательные к тому же существительному
            for ch in token.head.children:
                if ch.pos_ == "ADJ" and ch.sent == token.sent:
                    span_tokens.add(ch)

        # если это существительное, добавляем прилегающие прилагательные
        if is_grim_noun and token.pos_ == "NOUN":
            for ch in token.children:
                if ch.pos_ == "ADJ" and ch.sent == token.sent:
                    span_tokens.add(ch)
            # иногда прилагательное стоит слева как "разбитая губа"
            if token.i > 0:
                left = doc[token.i - 1]
                if left.pos_ == "ADJ" and left.sent == token.sent:
                    span_tokens.add(left)

        # строим спан
        start_i = min(t.i for t in span_tokens)
        end_i   = max(t.i for t in span_tokens) + 1
        span    = doc[start_i:end_i]
        # оставляем только содержательные слова: сущ., прил., (иногда собственные имена)
        content_tokens = [
            t for t in span
            if t.is_alpha and t.pos_ in ("NOUN", "ADJ", "PROPN")
        ]

        if not content_tokens:
            continue

        # лемматизируем → приближаемся к "именительному базовому виду"
        lemma_tokens = [
            t.lemma_.lower().replace("ё", "е")
            for t in content_tokens
        ]

        lemma_phrase = " ".join(lemma_tokens).strip()
        if len(lemma_phrase) < 2:
            continue

        # первая буква заглавная, остальное как есть
        pretty = lemma_phrase[0].upper() + lemma_phrase[1:]
        candidates.add(pretty)

    # небольшая чистка: убираем совсем общие "Кровь" / "Грязь", если есть более конкретные фразы
    # (очень мягко, чтобы ничего не ломать)
    filtered = set(candidates)
    for cand in list(candidates):
        low = cand.lower()
        if low in {"кровь", "грязь"}:
            # если есть что-то более длинное с этим словом — убираем голое слово
            if any(low in other.lower() and other != cand for other in candidates):
                filtered.discard(cand)

    return sorted(filtered)

def add_grim_column(df: pd.DataFrame, nlp) -> pd.DataFrame:
    """
    Добавляет/перезаписывает колонку 'Грим' по rule-based-логике,
    полностью игнорируя, что там навычисляла нейронка.
    """
    df = df.copy()
    values = []

    for _, row in df.iterrows():
        text = str(row.get("text", ""))
        grim_items = extract_grim_from_text(text, nlp)

        if grim_items:
            values.append("; ".join(grim_items))
        else:
            values.append("")

    df["Грим"] = values
    return df

def inflect_adj_to_noun(adj_text: str, noun_text: str) -> str:
    """
    Согласовать прилагательное с существительным: род/число + именительный.
    Если не получилось — вернуть нормальную форму прилагательного.
    """
    pa = morph.parse(adj_text)
    pn = morph.parse(noun_text)
    if not pa or not pn:
        return adj_text.lower()

    pa = pa[0]
    pn = pn[0]

    grammemes = {"nomn"}  # именительный
    # род
    if "masc" in pn.tag:
        grammemes.add("masc")
    if "femn" in pn.tag:
        grammemes.add("femn")
    if "neut" in pn.tag:
        grammemes.add("neut")
    # число
    if "plur" in pn.tag:
        grammemes.add("plur")
    if "sing" in pn.tag:
        grammemes.add("sing")

    inflected = pa.inflect(grammemes)
    if inflected:
        return inflected.word.lower()

    # fallback — нормальная форма
    return pa.normal_form.lower()

def extract_costume_from_text(text: str, nlp) -> list[str]:
    """
    Rule-based извлечение костюма / одежды / формы из текста сцены.
    Работает независимо от нейронки.
    Возвращает список уникальных фраз (в "почти именительном").
    """
    if not text:
        return []

    doc = nlp(text)
    candidates: set[str] = set()

    for token in doc:
        # пропускаем чистую пунктуацию/цифры
        if not any(ch.isalpha() for ch in token.text):
            continue

        lemma_raw = ru_lemma(token.text)
        text_raw  = token.text.lower()

        lemma = lemma_raw.replace("ё", "е")
        text_norm = text_raw.replace("ё", "е")

        # 1) обычное совпадение по словарю
        in_noun_dict = (
            lemma in COSTUME_NOUN_LEMMAS_NORM     # "стринг" ∈ {...}?
            or text_norm in COSTUME_NOUN_LEMMAS_NORM  # "стринги" ∈ {...}?
        )
        in_adj_dict  = lemma in COSTUME_ADJ_LEMMAS_NORM

        # 2) совпадение по корням (форма/униформа)
        from_root = any(
            lemma.startswith(root) or text_norm.startswith(root)
            for root in COSTUME_NOUN_ROOTS
        )

        is_costume_noun = in_noun_dict or from_root
        is_costume_adj  = in_adj_dict

        if not (is_costume_noun or is_costume_adj):
            continue

        if is_costume_adj:
            head = token.head
            if head.pos_ != "NOUN":
                continue

            head_lemma = ru_lemma(head.text).replace("ё", "е")
            head_is_costume_noun = (
                head_lemma in COSTUME_NOUN_LEMMAS_NORM
                or any(head_lemma.startswith(root) for root in COSTUME_NOUN_ROOTS)
            )
            if not head_is_costume_noun:
                # 'походный метод', 'старый походный способ' и т.п. — не костюм
                continue

        # --- строим спан вокруг опорного токена ---
        span_tokens = {token}

        # если это прилагательное — поднимаемся к существительному (форма, костюм, платье…)
        if is_costume_adj and token.head.pos_ in ("NOUN", "PROPN") and token.head.sent == token.sent:
            span_tokens.add(token.head)
            for ch in token.head.children:
                if ch.pos_ == "ADJ" and ch.sent == token.sent:
                    span_tokens.add(ch)

        if is_costume_noun and token.pos_ == "NOUN":
            for ch in token.children:
                if ch.sent != token.sent:
                    continue

                # берём только "костюмные" прилагательные — не тащим 'видные', 'последние' и т.п.
                if ch.pos_ == "ADJ":
                    ch_lemma = ru_lemma(ch.text).replace("ё", "е")
                    if ch_lemma in COSTUME_ADJ_LEMMAS_NORM:
                        span_tokens.add(ch)

                # бренд / владелец: Макдоналдса, полиции, ГИБДД, и т.п.
                if ch.pos_ == "PROPN":
                    span_tokens.add(ch)

            # слева тоже только "костюмные" прилагательные
            if token.i > 0:
                left = doc[token.i - 1]
                if left.sent == token.sent and left.pos_ == "ADJ":
                    left_lemma = ru_lemma(left.text).replace("ё", "е")
                    if left_lemma in COSTUME_ADJ_LEMMAS_NORM:
                        span_tokens.add(left)


        # строим границы спана
        start_i = min(t.i for t in span_tokens)
        end_i   = max(t.i for t in span_tokens) + 1
        span    = doc[start_i:end_i]

        # лемматизируем, чтобы приблизиться к "именительному"
        content_tokens = [
            t for t in span
            if t.is_alpha and t.pos_ in ("NOUN", "ADJ", "PROPN")
        ]
        if not content_tokens:
            continue

        # ищем главное существительное в спане
        head_noun = None
        for t in content_tokens:
            if t.pos_ == "NOUN":
                head_noun = t
                break

        head_noun_text = head_noun.text if head_noun is not None else None

        lemma_tokens = []
        for t in content_tokens:
            if t.pos_ == "PROPN":
                # бренды/имена — как в тексте
                tok = t.text
            elif t.pos_ == "NOUN":
                # существительное — нормальная форма (И.п.)
                tok = ru_lemma(t.text)
            else:  # ADJ
                # пытаемся согласовать прилагательное с существительным
                if head_noun_text is not None:
                    tok = inflect_adj_to_noun(t.text, head_noun_text)
                else:
                    tok = ru_lemma(t.text)
            lemma_tokens.append(tok.lower().replace("ё", "е"))

        lemma_phrase = " ".join(lemma_tokens).strip()
        if len(lemma_phrase) < 2:
            continue

        pretty = lemma_phrase[0].upper() + lemma_phrase[1:]
        candidates.add(pretty)

    return sorted(candidates)

def add_costume_column(df: pd.DataFrame, nlp) -> pd.DataFrame:
    """
    Добавляет/перезаписывает колонку 'Костюм' по rule-based-логике,
    игнорируя то, что выдала нейронка.
    """
    df = df.copy()
    values = []

    for _, row in df.iterrows():
        text = str(row.get("text", ""))
        costume_items = extract_costume_from_text(text, nlp)

        if costume_items:
            values.append("; ".join(costume_items))
        else:
            values.append("")

    df["Костюм"] = values
    return df

def _collect_keyword_spans(
    text: str,
    nlp,
    *,
    noun_lemmas: set[str] | None = None,
    adj_lemmas: set[str] | None = None,
    verb_lemmas: set[str] | None = None,
    window: int = 3,
) -> list[str]:
    """
    Общий хелпер:
      - бегаем по токенам,
      - ищем триггеры по леммам (существительные / прилагательные / глаголы),
      - расширяем спан в пределах одного предложения +- window токенов,
      - собираем только содержательные токены (ADJ/NOUN/PROPN/VERB по контексту),
      - нормализуем до "красивого" текста.
    """
    if not text:
        return []

    doc = nlp(text)
    candidates: set[str] = set()

    noun_lemmas = noun_lemmas or set()
    adj_lemmas  = adj_lemmas or set()
    verb_lemmas = verb_lemmas or set()

    for sent in doc.sents:
        sent_tokens = list(sent)
        for i, tok in enumerate(sent_tokens):
            if not tok.is_alpha:
                continue
            form = tok.text.lower().replace("ё", "е")
            lemma = ru_lemma(tok.text).replace("ё", "е")

            is_trigger = False

            if noun_lemmas and lemma in noun_lemmas:
                is_trigger = True
            if adj_lemmas and lemma in adj_lemmas:
                is_trigger = True
            if verb_lemmas and lemma in verb_lemmas:
                is_trigger = True

            # доп. триггер для спецэффектов по ключевым фразам
            if not is_trigger and FX_KEYWORDS_N:
                for kw in FX_KEYWORDS_N:
                    if kw in sent.text.lower().replace("ё", "е"):
                        is_trigger = True
                        break

            if not is_trigger:
                continue

            # строим маленькое окно вокруг триггера
            left = max(0, i - window)
            right = min(len(sent_tokens), i + window + 1)
            span_tokens = sent_tokens[left:right]

            content = []
            for t in span_tokens:
                if not t.is_alpha:
                    continue
                if t.pos_ in ("DET", "PART", "CCONJ", "SCONJ", "ADP", "PRON"):
                    # артикли/частицы/союзы/предлоги/местоимения выкидываем
                    continue
                content.append(t.text.lower().replace("ё", "е"))

            if not content:
                continue

            phrase = " ".join(content)
            if len(phrase) < 2:
                continue

            pretty = phrase[0].upper() + phrase[1:]
            candidates.add(pretty)

    return sorted(candidates)

def extract_pyro_from_text(text: str, nlp) -> list[str]:
    """
    Пиротехника: возвращаем только ОДНОСЛОВНЫЕ канонические названия
    (костер, фейерверк, салют, петарда и т.п.), без окон и контекста.
    """
    if not text:
        return []

    doc = nlp(text)
    result: set[str] = set()

    for sent in doc.sents:
        tokens = list(sent)
        for i, tok in enumerate(tokens):
            if not tok.is_alpha:
                continue

            form = tok.text.lower().replace("ё", "е")
            lemma = ru_lemma(tok.text).replace("ё", "е")

            # триггер по словарю существительных
            if lemma not in PYRO_NOUNS_N and form not in PYRO_NOUNS_N:
                continue

            # спец-фильтр: "взрыв хохота/смеха" — не пиротехника
            if lemma == "взрыв":
                window = tokens[max(0, i - 3): i + 4]
                neigh_lemmas = {ru_lemma(t.text).lower() for t in window}
                if {"смех", "хохот"} & neigh_lemmas:
                    continue

            # канон: одна лемма с заглавной буквы
            canon = lemma[0].upper() + lemma[1:]
            result.add(canon)

    return sorted(result)



def extract_fx_from_text(text: str, nlp) -> list[str]:
    """
    Спецэффекты: аккуратно берём только одно слово-канон
    (Флешбек, Туман, Дым и т.п.), без хвостов типа 'ИНТ', 'Кати доносится голос'.
    """
    # сначала берём кандидатов как раньше — маленькие фразы вокруг триггеров
    phrases = _collect_keyword_spans(
        text,
        nlp,
        noun_lemmas=FX_NOUNS_N,
        verb_lemmas=None,
        adj_lemmas=None,
        window=4,
    )

    cleaned: set[str] = set()

    for ph in phrases:
        low = ph.lower().replace("ё", "е").strip()
        if not low:
            continue

        # берём только первое слово из фразы
        first = low.split()[0]

        lemma = ru_lemma(first).replace("ё", "е")

        # специальные случаи, чтобы было красиво
        if lemma in {"флешбек", "flashback", "флешбэк"}:
            canon = "Флешбек"
        elif lemma in {"туман", "дым"}:
            canon = lemma.capitalize()
        else:
            # по умолчанию — просто лемма с заглавной
            canon = lemma[:1].upper() + lemma[1:] if lemma else ph

        cleaned.add(canon)

    return sorted(cleaned)


def extract_stunts_from_text(text: str, nlp) -> list[str]:
    """
    Каскадёры / дублёры в описании.
    """
    return _collect_keyword_spans(
        text,
        nlp,
        noun_lemmas=STUNT_WORDS_N,
        adj_lemmas=None,
        verb_lemmas=None,
        window=3,
    )

def _normalize_place_segment(seg: str) -> str:
    """
    Приводим кусочек place/object/subobject к аккуратному виду:
    - режем по «(см. сц.8)» и т.п.,
    - чистим лишние пробелы и точки,
    - делаем просто Строчное С Заглавной.
    """
    if not seg:
        return ""
    s = seg.strip()
    # убираем ссылки вида (СМ. СЦ.8)
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"\s+", " ", s)
    s = s.strip(" .:/-")
    if not s:
        return ""
    low = s.lower()
    # title для русского ок, жить можно
    return low[0].upper() + low[1:]


def extract_decoration_from_place(
    location: str,
    object_: str,
    subobject: str,
) -> list[str]:
    """
    Строим 'Декорация' только из Object / Subobject.
    location — 'ИНТ', 'НАТ', 'ИНТ/НАТ' и т.п.
    """
    loc = (location or "").upper().replace("Ё", "Е")

    # разбиваем object/subobject на сегменты по точкам и слэшам
    raw_segments = []

    for part in (object_ or "").split("/"):
        raw_segments.extend(p.strip() for p in part.split(".") if p.strip())

    for part in (subobject or "").split("/"):
        raw_segments.extend(p.strip() for p in part.split(".") if p.strip())

    decorations: set[str] = set()

    for seg in raw_segments:
        norm_seg = _normalize_place_segment(seg)
        if not norm_seg:
            continue

        low = norm_seg.lower()

        # если сегмент — одно из "ГОРОД/УЛИЦА/ГОРЫ/РЕКА" и т.п. → пропускаем
        if low in GENERIC_PLACE_STOP:
            continue

        # если локация ИНТ → почти всё, что не generic, можно считать декорацией
        if loc.startswith("ИНТ"):
            decorations.add(norm_seg)
            continue

        # если НАТ → берём только явно рукотворные места (лагерь, станция, пристань…)
        if loc.startswith("НАТ"):
            # для NAT допускаем декорацию только если сегмент
            # содержит ключ из MANMADE_PLACES
            if any(key in low for key in MANMADE_PLACES):
                decorations.add(norm_seg)
            continue

        # на случай ЭКСТ / смешанных режимов:
        # используем ту же логику, что и для НАТ — только MANMADE
        if any(key in low for key in MANMADE_PLACES):
            decorations.add(norm_seg)

    return sorted(decorations)

def add_decoration_from_place(df: pd.DataFrame) -> pd.DataFrame:
    """
    Пересчитывает колонку 'Декорация' только из
    'Инт / нат' + 'Объект' + 'Подобъект'.
    """
    df = df.copy()
    decos = []

    for _, row in df.iterrows():
        location  = str(row.get("location", ""))
        object_   = str(row.get("object", ""))
        subobject = str(row.get("subobject", ""))

        items = extract_decoration_from_place(location, object_, subobject)
        decos.append("; ".join(items) if items else "")

    df["Декорация"] = decos
    return df


def add_pyro_column(df: pd.DataFrame, nlp) -> pd.DataFrame:
    df = df.copy()
    vals = []
    for _, row in df.iterrows():
        text = str(row.get("text", ""))
        items = extract_pyro_from_text(text, nlp)
        vals.append("; ".join(items) if items else "")
    df["Пиротехника"] = vals
    return df


def add_fx_column(df: pd.DataFrame, nlp) -> pd.DataFrame:
    df = df.copy()
    vals = []
    for _, row in df.iterrows():
        text = str(row.get("text", ""))
        items = extract_fx_from_text(text, nlp)
        vals.append("; ".join(items) if items else "")
    df["Спецэффект"] = vals
    return df


def add_stunt_column(df: pd.DataFrame, nlp) -> pd.DataFrame:
    """
    Каскадёры: комбинируем rule-based по тексту + при желании выдёргиваем из 'Групповка'.
    """
    df = df.copy()
    vals = []

    for _, row in df.iterrows():
        text = str(row.get("text", ""))
        items = set(extract_stunts_from_text(text, nlp))

        # если у тебя есть колонка 'Групповка' со строкой вида "Техники (2); Каскадер – оперативник 1"
        grp = str(row.get("Групповка", ""))
        if grp:
            for chunk in re.split(r"[;,]", grp):
                if "каскадер" in chunk.lower().replace("ё", "е"):
                    cleaned = chunk.strip()
                    if cleaned:
                        items.add(cleaned)

        vals.append("; ".join(sorted(items)) if items else "")

    df["Каскадер"] = vals
    return df


# ==== 4. Объединяем функции ====


def build_scenes_dataframe(script_path: str) -> pd.DataFrame:
    """
    1) Читает docx/pdf.
    2) Режет на сцены parse_script_with_episode.
    3) Для каждой сцены достаёт персонажей / массовку / групповку.
    На выходе — DataFrame по сценам с базовыми колонками + 3 «геройскими».
    """
    # 1. читаем файл в текст
    full_text = upload_file(script_path)

    # 2. режем на сцены и берём первые колонки
    print('Режем на сцены и берём первые колонки.')
    scenes = parse_script_with_episode(full_text)

    # 3. инициализируем spaCy
    print('Инициализируем spaCy СТАРТ')
    nlp = load_ru()
    print('Инициализируем spaCy ФИНИШ')

    rows = []
    for scene in scenes:
        scene_text = scene.get("text", "") or ""
        entities = extract_scene_entities(
                    scene_text,
                    object_=scene.get("object") or scene.get("Объект") or "",
                    subobject=scene.get("subobject") or scene.get("Подобъект") or "",
                    nlp=nlp,
                )
        characters = entities["characters"]
        grouping = entities["grouping"]
        massovka = entities["massovka"]

        # персонажи — берём канон, обычно is_main=True
        char_names = sorted({c.canonical_name for c in characters})
        characters_str = "; ".join(char_names)

        grouping_str = "; ".join(grouping)
        massovka_str = "; ".join(massovka)

        row = dict(scene)
        row["Персонажи"] = characters_str
        row["Групповка"] = grouping_str
        row["Массовка"] = massovka_str

        rows.append(row)

    df_scenes = pd.DataFrame(rows)
    return df_scenes


def run_nn_block(df_scenes: pd.DataFrame, threshold: float = 0.5) -> pd.DataFrame:
    """
    4) Прогоняет те же сцены через твою NER-модель и process_dataframe.
    Возвращает только NN-колонки (+ «text» внутри).
    """
    # модель и device у тебя уже импортированы и есть MODEL_PATH / device
    model_local = AutoModelForTokenClassification.from_pretrained(MODEL_PATH).to(device)
    nlp = load_ru()
    df_ents = process_dataframe(df_scenes, model=model_local, nlp=nlp, text_col="text", threshold=threshold)
    return df_ents

def add_game_transport_column(df_scenes: pd.DataFrame, nlp) -> pd.DataFrame:
    """
    Добавляет в df_scenes колонку "Игровой транспорт"
    на основе object / subobject / text.
    """
    values = []
    for _, row in df_scenes.iterrows():
        obj = str(row.get("object", "") or row.get("Объект", "") or "")
        sub = str(row.get("subobject", "") or row.get("Подобъект", "") or "")
        txt = str(row.get("text", ""))

        canon_list = extract_game_transport_for_scene(obj, sub, txt, nlp)
        if canon_list:
            pretty = [c.capitalize() for c in canon_list]  # 'МАШИНА' → 'Машина'
            values.append("; ".join(pretty))
        else:
            values.append("")

    df_scenes = df_scenes.copy()
    df_scenes["Игровой транспорт"] = values
    return df_scenes

def full_pipeline_1(script_path: str, threshold: float = 0.5) -> pd.DataFrame:
    """
    Полный маршрут:
      файл → текст → сцены (4 колонки) → + Персонажи/Групповка/Массовка → + NN-колонки.
    """
    # 1) базовые колонки + персонажи/массовка/групповка
    df_scenes = build_scenes_dataframe(script_path)

    df_scenes = add_game_transport_column(df_scenes, nlp=nlp)

    # 2) NN-блок поверх той же таблицы
    print('NN-блок поверх той же таблицы')
    df_ents = run_nn_block(df_scenes, threshold=threshold)

    # 3) склеиваем: метаданные сцен + NN-колонки (без второго столбца text)
    print('Склейка')
    df_final = pd.concat(
        [
            df_scenes.reset_index(drop=True),
            df_ents.drop(columns=["text"], errors="ignore").reset_index(drop=True),
        ],
        axis=1,
    )

    df_final = add_grim_column(df_final, nlp=nlp)
    df_final = add_costume_column(df_final, nlp=nlp)
    df_final = add_decoration_from_place(df_final)
    df_final = add_pyro_column(df_final, nlp)
    df_final = add_fx_column(df_final, nlp)
    df_final = add_stunt_column(df_final, nlp)

    df_final = df_final.rename(columns={"episode_num": "Эпизод", "scene_num": "Сцена", "location": "Инт/Нат",
     "object": "Объект", "subobject": "Подобъект", "time": "Режим", "text": "Текст", "Спецэффект": "Спецэффекты"})
    col = df_final["Режим"]
    df_final = df_final.drop("Режим", axis=1)
    df_final.insert(3, "Режим", col)

    # 4) сохраняем
    print('Сохраняем')
    #df_final.to_excel(output_path, index=False)
    print(f"Готово, результат сохранён")

    return df_final

def full_pipeline(script_path: str, output_path: str, threshold: float = 0.5) -> pd.DataFrame:
    """
    Полный маршрут:
      файл → текст → сцены (4 колонки) → + Персонажи/Групповка/Массовка → + NN-колонки.
    """
    # 1) базовые колонки + персонажи/массовка/групповка
    df_scenes = build_scenes_dataframe(script_path)

    df_scenes = add_game_transport_column(df_scenes, nlp=nlp)

    # 2) NN-блок поверх той же таблицы
    print('NN-блок поверх той же таблицы')
    df_ents = run_nn_block(df_scenes, threshold=threshold)

    # 3) склеиваем: метаданные сцен + NN-колонки (без второго столбца text)
    print('Склейка')
    df_final = pd.concat(
        [
            df_scenes.reset_index(drop=True),
            df_ents.drop(columns=["text"], errors="ignore").reset_index(drop=True),
        ],
        axis=1,
    )

    df_final = add_grim_column(df_final, nlp=nlp)
    df_final = add_costume_column(df_final, nlp=nlp)
    df_final = add_decoration_from_place(df_final)
    df_final = add_pyro_column(df_final, nlp)
    df_final = add_fx_column(df_final, nlp)
    df_final = add_stunt_column(df_final, nlp)

    df_final = df_final.rename(columns={"episode_num": "Эпизод", "scene_num": "Сцена", "location": "Инт/Нат",
     "object": "Объект", "subobject": "Подобъект", "time": "Режим", "text": "Текст", "Спецэффект": "Спецэффекты"})
    col = df_final["Режим"]
    df_final = df_final.drop("Режим", axis=1)
    df_final.insert(3, "Режим", col)

    # 4) сохраняем
    df_final.to_excel(output_path, index=False)
    print(f"Готово, результат сохранён")

    return df_final


if __name__ == "__main__":

    log_file = resource_path("main.log")

    parser = argparse.ArgumentParser(description="Полный пайплайн для сценария.")
    parser.add_argument("input", help="Путь к .docx или .pdf сценарию")
    parser.add_argument("output", help="Путь к .xlsx с результатом")
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.5,
        help="Порог для score в NН (по умолчанию 0.5)",
    )

    try:
        with log_file.open("a", encoding="utf-8") as f:
            f.write(f"\n=== START ===\n")
            f.flush()
        args = parser.parse_args()
        full_pipeline(args.input, args.output, threshold=args.threshold)
    
    except Exception:
        with log_file.open("a", encoding="utf-8") as f:
            f.write(f"\n=== EXCEPTION ===\n")
            traceback.print_exc(file=f)
            f.flush()
        traceback.print_exc()
        sys.exit(1)