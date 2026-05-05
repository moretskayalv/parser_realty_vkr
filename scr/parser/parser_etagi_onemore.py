import asyncio
import csv
import re
import time
from pathlib import Path
from typing import Optional, Tuple, List, Set, Dict

import pandas as pd
from playwright.async_api import async_playwright


SEARCH_GROUPS_FILE = "search_group.csv"

MAX_PAGES_PER_GROUP = 1000
ZERO_STREAK_LIMIT = 6

SLEEP_LIST = 2
SLEEP_CARD = 1.5

LINKS_FILE = "links.csv"
RESULT_FILE = "result.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


# ---------- UTILS ----------

def clean(text: str) -> str:
    return re.sub(r"\s+", " ", str(text)).strip()

def load_search_groups(filepath: str = SEARCH_GROUPS_FILE) -> Dict[str, str]:
    df = pd.read_csv(filepath)

    required_columns = {"source_group", "url"}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(f"В {filepath} нет колонок: {missing_columns}")

    df = df.dropna(subset=["source_group", "url"])

    return {
        str(row["source_group"]).strip(): str(row["url"]).strip()
        for _, row in df.iterrows()
    }

def to_int(text: Optional[str]) -> Optional[int]:
    if not text:
        return None

    digits = re.sub(r"[^\d]", "", str(text))
    return int(digits) if digits else None


def to_float(text: Optional[str]) -> Optional[float]:
    if not text:
        return None

    text = str(text).replace(",", ".")
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    return float(match.group(1)) if match else None


def build_page_url(base_url: str, page: int) -> str:
    if page == 1:
        return base_url

    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}page={page}"


# ---------- PLAYWRIGHT ----------

async def close_popups(page) -> None:
    for selector in [
        'button:has-text("Принять")',
        'button:has-text("Согласен")',
        'button:has-text("Понятно")',
        'button:has-text("Закрыть")',
        'button[aria-label="Закрыть"]',
    ]:
        try:
            await page.locator(selector).first.click(timeout=1200)
            await page.wait_for_timeout(300)
        except Exception:
            pass


async def expand_characteristics(page) -> None:
    """
    Раскрывает именно кнопку 'Показать больше' в характеристиках.
    Рабочий вариант: кликаем последний элемент с текстом 'Показать больше' через JS.
    """
    try:
        await page.evaluate("""
        () => {
            const all = Array.from(document.querySelectorAll('button, div, span, a'));

            const candidates = all.filter(el =>
                el.innerText &&
                el.innerText.trim() === 'Показать больше'
            );

            if (!candidates.length) return false;

            const el = candidates[candidates.length - 1];
            el.scrollIntoView({behavior: 'instant', block: 'center'});
            el.click();

            return true;
        }
        """)
        await page.wait_for_timeout(2500)
    except Exception:
        pass


async def click_map_for_coordinates(page) -> None:
    """
    Кликает 'На карте', чтобы в HTML появились координаты дома.
    """
    try:
        await page.locator('text="На карте"').first.click(timeout=5000, force=True)
        await page.wait_for_timeout(3000)
    except Exception:
        pass


async def fetch_card_page_playwright(url: str) -> Tuple[str, str]:
    """
    Возвращает:
    text — текст страницы после раскрытия характеристик;
    html — HTML после клика по карте, чтобы достать координаты.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        page = await browser.new_page(
            viewport={"width": 1440, "height": 2200},
            user_agent=HEADERS["User-Agent"],
        )

        await page.goto(url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(4000)

        await close_popups(page)
        await expand_characteristics(page)

        text = await page.locator("body").inner_text()

        await click_map_for_coordinates(page)
        html = await page.content()

        await browser.close()

    return text, html


# ---------- LINK COLLECTION ----------

async def fetch_links_from_page(page_url: str) -> List[str]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        page = await browser.new_page(
            viewport={"width": 1440, "height": 2200},
            user_agent=HEADERS["User-Agent"],
        )

        await page.goto(page_url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(5000)

        await close_popups(page)

        for _ in range(6):
            await page.mouse.wheel(0, 2500)
            await page.wait_for_timeout(1200)

        hrefs = await page.locator('a[href*="/realty/"]').evaluate_all(
            """
            elements => elements
                .map(el => el.getAttribute('href'))
                .filter(Boolean)
            """
        )

        await browser.close()

    links = []

    for href in hrefs:
        href = href.split("?")[0]

        if re.search(r"/realty/\d+/?$", href):
            if href.startswith("http"):
                links.append(href)
            else:
                links.append("https://msk.etagi.com" + href)

    return sorted(set(links))


def collect_links_for_group_playwright(
    group_name: str,
    base_url: str,
    max_pages: int = MAX_PAGES_PER_GROUP,
    zero_streak_limit: int = ZERO_STREAK_LIMIT,
) -> List[Dict[str, str]]:

    collected: List[Dict[str, str]] = []
    seen_links: Set[str] = set()
    zero_streak = 0

    for page_num in range(1, max_pages + 1):
        page_url = build_page_url(base_url, page_num)

        try:
            page_links = asyncio.run(fetch_links_from_page(page_url))

            new_count = 0

            for link in page_links:
                if link not in seen_links:
                    seen_links.add(link)
                    collected.append({
                        "source_group": group_name,
                        "link": link,
                    })
                    new_count += 1

            print(
                f"[{group_name}][LIST {page_num}] "
                f"+{new_count} новых ссылок, всего {len(collected)}"
            )

            if new_count == 0:
                zero_streak += 1
            else:
                zero_streak = 0

            if zero_streak >= zero_streak_limit:
                print(
                    f"[{group_name}] остановка: "
                    f"{zero_streak_limit} страниц подряд без новых ссылок"
                )
                break

            time.sleep(SLEEP_LIST)

        except Exception as e:
            print(f"[{group_name}][LIST {page_num}] ERROR -> {e}")
            break

    return collected


# ---------- STORAGE ----------

def save_links(rows: List[Dict[str, str]], filepath: str = LINKS_FILE) -> None:
    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["source_group", "link"])
        writer.writeheader()
        writer.writerows(rows)


def load_links(filepath: str = LINKS_FILE) -> List[Dict[str, str]]:
    path = Path(filepath)

    if not path.exists():
        return []

    df = pd.read_csv(filepath)
    return df.to_dict(orient="records")


# ---------- CARD PARSING ----------

def extract_price(text: str) -> Optional[int]:
    lines = [clean(x) for x in text.splitlines() if clean(x)]

    for line in lines:
        if re.fullmatch(r"\d[\d\s]{5,}", line):
            value = to_int(line)
            if value and value > 100000:
                return value

    for match in re.finditer(r"(\d[\d\s]+)\s*руб", text, re.IGNORECASE):
        value = to_int(match.group(1))
        if value and value > 100000:
            return value

    return None


def extract_coordinates_from_html(html: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Ищем московскую пару координат вида:
    55.xxxxxx, 37.xxxxxx
    """
    pattern = r'([5-6]\d\.\d{4,})\s*,\s*([3-4]\d\.\d{4,})'
    matches = re.findall(pattern, html)

    for lat, lon in matches:
        lat = float(lat)
        lon = float(lon)

        if 55 <= lat <= 56 and 36 <= lon <= 38:
            return lat, lon

    return None, None


def extract_characteristics_block(lines: List[str]) -> List[str]:
    starts = [
        i for i, line in enumerate(lines)
        if line.lower().strip() == "характеристики"
    ]

    if not starts:
        return []

    start = starts[-1]

    block = []

    stop_words = {
        "ипотека",
        "ипотека 16,9% для всех",
        "ипотека 16,9%",
        "запланируйте просмотр",
        "рассчитайте ипотеку",
        "что-то не так с объявлением?",
        "сообщите нам, и мы разберёмся в проблеме",
        "сообщить об ошибке",
        "история цены",
    }

    for line in lines[start + 1:]:
        low = line.lower().strip()

        if low in stop_words:
            break

        block.append(line)

    return block


def get_value_after_label(block: List[str], labels: List[str]) -> Optional[str]:
    labels_lower = {label.lower().strip() for label in labels}

    for i in range(len(block) - 1):
        if block[i].lower().strip() in labels_lower:
            return block[i + 1].strip()

    return None


def extract_floor_from_value(value: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    if not value:
        return None, None

    match = re.search(r"(\d+)\s+из\s+(\d+)", value, re.IGNORECASE)
    if match:
        return int(match.group(1)), int(match.group(2))

    match = re.search(r"(\d+)\s*/\s*(\d+)", value, re.IGNORECASE)
    if match:
        return int(match.group(1)), int(match.group(2))

    return None, None


def extract_rooms_from_value(value: Optional[str], text: str) -> Optional[int]:
    if value:
        match = re.search(r"(\d+)", value)
        if match:
            return int(match.group(1))

    match = re.search(r"(\d+)\s*-\s*комн", text, re.IGNORECASE)
    if match:
        return int(match.group(1))

    match = re.search(r"(\d+)к,", text, re.IGNORECASE)
    if match:
        return int(match.group(1))

    if "студия" in text.lower():
        return 0

    return None


def extract_address(lines: List[str]) -> Optional[str]:
    """
    Берём верхний адрес карточки, например:
    Москва, ул. Раменки, 11к1 (16 км до центра)
    """
    for line in lines:
        low = line.lower()

        if (
            low.startswith("москва,")
            and "тел." not in low
            and "показать" not in low
            and any(x in low for x in [
                "ул.",
                "улица",
                "проспект",
                "проезд",
                "шоссе",
                "переулок",
                "бульвар",
                "наб.",
                "набережная",
            ])
        ):
            return line

    for line in lines:
        low = line.lower()

        if (
            "москва" in low
            and "квартира" in low
            and "этаж" in low
            and "тел." not in low
            and "показать" not in low
        ):
            return line

    return None


def extract_metro(lines: List[str]) -> Tuple[Optional[str], Optional[int]]:
    """
    Ищем метро только в верхнем блоке рядом с 'На карте',
    чтобы не забрать ипотечное 'Одобрить за 5 минут'.
    """
    try:
        start = next(
            i for i, line in enumerate(lines)
            if line.lower().strip() == "на карте"
        )
    except StopIteration:
        return None, None

    stop_words = {
        "в ипотеку от",
        "рассчитать платеж",
        "хотите скидку?",
        "описание",
        "характеристики",
    }

    candidates = []

    for i in range(start + 1, min(start + 18, len(lines))):
        line = lines[i].strip()
        low = line.lower()

        if low in stop_words:
            break

        # вариант:
        # Раменки
        # 15 мин. (1 км)
        m = re.search(r"^(\d+)\s*мин", low)
        if m and i > 0:
            station = clean(lines[i - 1])
            minutes = int(m.group(1))

            bad_station = {
                "на карте",
                "москва",
                "₽",
                "/мес.",
            }

            if station.lower() not in bad_station and 2 <= len(station) <= 40:
                candidates.append((station, minutes))

        # вариант в одну строку:
        # Раменки 15 мин.
        m2 = re.search(r"^([А-Яа-яЁёA-Za-z\-\s]+)\s+(\d+)\s*мин", line)
        if m2:
            station = clean(m2.group(1))
            minutes = int(m2.group(2))

            if 2 <= len(station) <= 40:
                candidates.append((station, minutes))

    if candidates:
        return min(candidates, key=lambda x: x[1])

    return None, None


def parse_card(link: str, source_group: str) -> dict:
    text, html = asyncio.run(fetch_card_page_playwright(link))

    lines = [clean(x) for x in text.splitlines() if clean(x)]
    block = extract_characteristics_block(lines)

    price = extract_price(text)

    latitude, longitude = extract_coordinates_from_html(html)

    area_total = to_float(get_value_after_label(block, ["Общая площадь"]))
    kitchen_area = to_float(get_value_after_label(block, ["Площадь кухни"]))

    renovation = get_value_after_label(block, ["Ремонт", "Отделка"])
    build_year = to_int(get_value_after_label(block, ["Год постройки", "Год сдачи"]))
    walls = get_value_after_label(block, ["Стены", "Материал стен"])

    floor_info = get_value_after_label(block, ["Этаж/Этажность"])
    floor, floors_total = extract_floor_from_value(floor_info)

    rooms_value = get_value_after_label(block, ["Комнатность"])
    rooms = extract_rooms_from_value(rooms_value, text)

    ceiling_height = to_float(get_value_after_label(block, ["Высота потолков"]))
    floor_position = get_value_after_label(block, ["Расположение на этаже"])
    elevator = get_value_after_label(block, ["Лифт"])
    house_number = get_value_after_label(block, ["Номер дома"])
    yard = get_value_after_label(block, ["Двор"])
    parking = get_value_after_label(block, ["Парковка"])
    playground = get_value_after_label(block, ["Детская площадка"])

    address = extract_address(lines)
    metro, metro_walk_min = extract_metro(lines)

    return {
        "source_group": source_group,
        "price": price,
        "area_total": area_total,
        "rooms": rooms,
        "floor": floor,
        "floors_total": floors_total,
        "address": address,
        "latitude": latitude,
        "longitude": longitude,
        "metro": metro,
        "metro_walk_min": metro_walk_min,
        "kitchen_area": kitchen_area,
        "renovation": renovation,
        "walls": walls,
        "build_year": build_year,
        "ceiling_height": ceiling_height,
        "floor_position": floor_position,
        "elevator": elevator,
        "house_number": house_number,
        "yard": yard,
        "parking": parking,
        "playground": playground,
        "link": link,
    }


# ---------- RESULT STORAGE ----------

RESULT_COLUMNS = [
    "source_group",
    "price",
    "area_total",
    "rooms",
    "floor",
    "floors_total",
    "address",
    "latitude",
    "longitude",
    "metro",
    "metro_walk_min",
    "kitchen_area",
    "renovation",
    "walls",
    "build_year",
    "ceiling_height",
    "floor_position",
    "elevator",
    "house_number",
    "yard",
    "parking",
    "playground",
    "link",
]


def ensure_result_file(filepath: str = RESULT_FILE) -> None:
    if Path(filepath).exists():
        return

    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RESULT_COLUMNS)
        writer.writeheader()


def load_existing_links(filepath: str = RESULT_FILE) -> Set[str]:
    path = Path(filepath)

    if not path.exists():
        return set()

    try:
        df = pd.read_csv(filepath)

        if "link" not in df.columns:
            return set()

        return set(df["link"].dropna().astype(str).tolist())

    except Exception:
        return set()


def append_row(row: dict, filepath: str = RESULT_FILE) -> None:
    with open(filepath, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RESULT_COLUMNS)
        writer.writerow(row)


# ---------- PIPELINE ----------

def run_link_collection() -> List[Dict[str, str]]:
    all_rows: List[Dict[str, str]] = []
    global_seen: Set[str] = set()

    search_groups = load_search_groups()
    for group_name, base_url in search_groups.items():
        group_rows = collect_links_for_group_playwright(
            group_name=group_name,
            base_url=base_url,
            max_pages=MAX_PAGES_PER_GROUP,
            zero_streak_limit=ZERO_STREAK_LIMIT,
        )

        for row in group_rows:
            if row["link"] not in global_seen:
                global_seen.add(row["link"])
                all_rows.append(row)

    save_links(all_rows, LINKS_FILE)

    print(f"\nСсылки сохранены в {LINKS_FILE}: {len(all_rows)}")

    return all_rows


def run_card_collection(link_rows: List[Dict[str, str]]) -> None:
    ensure_result_file(RESULT_FILE)
    done_links = load_existing_links(RESULT_FILE)

    print(f"Уже собрано карточек: {len(done_links)}")

    for i, item in enumerate(link_rows, 1):
        link = item["link"]
        source_group = item["source_group"]

        if link in done_links:
            print(f"[CARD {i}] SKIP -> {link}")
            continue

        try:
            row = parse_card(link, source_group)
            append_row(row, RESULT_FILE)
            done_links.add(link)

            print(f"[CARD {i}] OK -> {source_group} -> {link}")

        except Exception as e:
            print(f"[CARD {i}] ERROR -> {source_group} -> {link} -> {e}")

        time.sleep(SLEEP_CARD)

    print(f"\nГотово. Результат сохранён в {RESULT_FILE}")


def main() -> None:
    print("Этап 1: сбор ссылок через Playwright")
    link_rows = run_link_collection()

    print("\nЭтап 2: сбор карточек через Playwright с раскрытием характеристик")
    run_card_collection(link_rows)

    try:
        df = pd.read_csv(RESULT_FILE)

        print("\nПервые строки результата:")
        print(df.head(10).to_string())

        print(f"\nВсего строк в {RESULT_FILE}: {len(df)}")

        print("\nЗаполненность полей:")
        print(df.notna().sum().to_string())

        if "source_group" in df.columns:
            print("\nРаспределение по подвыборкам:")
            print(df["source_group"].value_counts(dropna=False).to_string())

    except Exception as e:
        print(f"Не удалось прочитать итоговый CSV: {e}")


if __name__ == "__main__":
    main()