import asyncio
import csv
import re
import time
from pathlib import Path
from typing import Optional, Tuple, List, Set, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright


SEARCH_GROUPS = {
    # сюда подставь реальные URL выдач
    #"all": "https://msk.etagi.com/vtorichnoe/",
    #"studio": "https://msk.etagi.com/realty/?seller[]=owner&studio[]=true&rooms[]=true.",
     #"1k": "https://msk.etagi.com/realty/?seller[]=owner&rooms[]=1",
     #"2k": "https://msk.etagi.com/realty/?seller[]=owner&rooms[]=2",
     #"3k": "https://msk.etagi.com/realty/?seller[]=owner&rooms[]=3",
     #"4k": "https://msk.etagi.com/realty/?seller[]=owner&rooms[]=%3E4",
     #"all-akademicheskaya": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3761",
    #"all-alekseevskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3762",
    # "all-altyfvevskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3763",
    # "all-arbat": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3764",
    # "all-aeoport": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3765",
    # "all-babyshkinskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3766",
    # "all-basmannyii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3767",
    # "all-begovoi": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3768",
    #"all-beskydnikovskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3769",
    # "all-bibirevo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3770",
    # "all-burulevo_vostok": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3771",
    # "all-burulevo_zapad": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3772",
    # "all-bogorodskoe": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3773",
    # "all-brateevo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3774",
    # "all-bytirskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3775",
    # "all-vechnyaki": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3776",
    # "all-vnukovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3777",
    # "all-voikovskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3779",
    # "all-voronovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4161",
    # "all-vostochnoe_degunino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4043",
    # "all-vostochnoe_izmailovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3780",
    # "all-vostochnii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3781",
    # "all-vuhino_zulebino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3782",
    # "all-gagarinskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3783",
    # "all-golovinskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3784",
    # "all-golyanovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3785",
    #"all-danilovskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3786",
    # "all-dmitrovskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3787",
    # "all-donskoi": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3810",
    # "all-dorogomilovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3811",
    # "all-zamoskvorechie": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3812",
    # "all-zapadnoe_degunino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3772",
     #"all-zuzino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3814",
     #"all-zuablikovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3816",
    # "all-ivanovskoe": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3817",
    # "all-izmailovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3818",
    # "all-kapotnya": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3819",
    # "all-kommunarka": "https://msk.etagi.com/realty/?seller[]=owner&city_id[]=2927",
    # "all-konkovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3820",
    # "all-koptevo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3837",
    # "all-kosino_yhtomskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3872",
    # "all-kotlovka": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3881",
    # "all-krasnopahorskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4226",
    # "all-krasnoselskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3938",
    # "all-krylatskoe": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3882",
    # "all-krukovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3883",
    # "all-kuzminki": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3884",
    # "all-kyncevo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3961",
    # "all-kurkino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3939",
    # "all-levobereznyii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3963",
    # "all-lefortovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3873",
    # "all-lianozovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3874",
     #"all-lomonosovskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3875",
     #"all-losinoostrovskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3876",
    # "all-lublino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3880",
    # "all-marfino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3921",
    # "all-marina_rosha": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3922",
    # "all-mariino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3923",
    # "all-matyshkino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3924",
    # "all-metrogorodok": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3878",
    # "all-meshanskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3925",
    # "all-mitino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3926",
    # "all-mozaiskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4031",
    # "all-molzaninovskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4056",
    # "all-moskvorechie_saburovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3927",
    # "all-nagatino_sadovniki": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3928",
    # "all-nagatinskii_zaton": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3929",
    # "all-nagornyi": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3931",
    # "all-nekrasovka": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3932",
    # "all-nizegorodskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3933",
    # "all-novogirevo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3935",
    # "all-novokosino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3760",
    # "all-novo_peredelkino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3934",
    #"all-obruchevskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3964",
    # "all-orehovo_borisovo_ug": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3970",
    # "all-orehovo_borisovo_sever": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3969",
    # "all-ostankinskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3971",
    # "all-otradnoe": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3972",
    # "all-ochakovo_matveevskoe": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3973",
    # "all-perovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3940",
    # "all-pechatniki": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3974",
    # "all-pokrovskoe_strehnevo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3975",
    # "all-preobrazenskoe": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3947",
    # "all-presnenskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3976",
    # "all-grospekt_vernandskogo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3977",
    # "all-ramenki": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3978",
    # "all-rostokino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3979",
    # "all-ryazanskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3980",
    # "all-savelki": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4059",
    # "all-savelovskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3839",
    # "all-sviblovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3981",
    # "all-sever_bytovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3936",
    # "all-sever_izmailovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3879",
    # "all-sever_medvedkovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3937",
     #"all-sever_tushino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3942",
     #"all-severnyi": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3943",
    # "all-silino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3944",
    # "all-sokol": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3945",
    # "all-sokolinaya_gora": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3941",
    # "all-sokolniki": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3946",
    # "all-solncevo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3965",
    # "all-staroe_krukovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3966",
    # "all-strogino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3967",
    # "all-taganskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3968",
    # "all-tverskoi": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4038",
    # "all-tekstilshiki": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4034",
    # "all-teplyi_stan": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3821",
    # "all-timiryazevskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3838",
    # "all-troick": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4194",
    # "all-troparevo_nikylino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4052",
    # "all-filevskii_park": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4053",
    # "all-fili_davydkovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4054",
    # "all-filimonkovskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4329",
    # "all-xamovniki": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4037",
    # "all-xovrino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4040",
    # "all-xoroshevo_mnevniki": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4046",
    # "all-xoroshevskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4045",
    # "all-caricino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4032",
    # "all-ceremshki": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3815",
    # "all-certanovo_sever": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4048",
    # "all-certanovo_yug": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4051",
     #"all-certanovo_centr": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4050",
    # "all-sherbinka": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4060",
    # "all-shykino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4044",
    # "all-yug_butovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4055",
    # "all-yug_medvedkovo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4041",
    # "all-yug_tyshino": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4039",
    # "all-uzhnoportovyi": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4035",
    # "all-yakimanka": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=4036",
    # "all-yaroslavskii": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3877",
     "all-yasenevo": "https://msk.etagi.com/realty/?seller[]=owner&district_id[]=3836",

}

MAX_PAGES_PER_GROUP = 80
ZERO_STREAK_LIMIT = 4

SLEEP_LIST = 2
SLEEP_CARD = 1.5

LINKS_FILE = "links.csv"
RESULT_FILE = "result.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
}


# ---------- UTILS ----------

def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def to_int(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


def to_float(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    text = text.replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", text)
    return float(m.group(1)) if m else None


def fetch(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    r.encoding = "utf-8"
    return r.text


def build_page_url(base_url: str, page: int) -> str:
    return base_url if page == 1 else f"{base_url}?page={page}"


# ---------- PLAYWRIGHT LISTING ----------

async def fetch_links_from_page(page_url: str) -> List[str]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1440, "height": 2200})
        await page.goto(page_url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(4000)

        for selector in [
            'button:has-text("Принять")',
            'button:has-text("Согласен")',
            'button:has-text("Понятно")',
            'button:has-text("Закрыть")',
            'button:has-text("Accept")',
        ]:
            try:
                await page.locator(selector).first.click(timeout=1200)
                await page.wait_for_timeout(400)
            except Exception:
                pass

        # Небольшой автоскролл, чтобы догрузились карточки
        for _ in range(4):
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

    cleaned = []
    for href in hrefs:
        href = href.split("?")[0]
        if re.search(r"/realty/\d+/?$", href):
            if href.startswith("http"):
                cleaned.append(href)
            else:
                cleaned.append("https://msk.etagi.com" + href)

    return sorted(set(cleaned))


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

            print(f"[{group_name}][LIST {page_num}] +{new_count} новых ссылок, всего {len(collected)}")

            if new_count == 0:
                zero_streak += 1
            else:
                zero_streak = 0

            if zero_streak >= zero_streak_limit:
                print(f"[{group_name}] остановка: {zero_streak_limit} страниц подряд без новых ссылок")
                break

            time.sleep(SLEEP_LIST)

        except Exception as e:
            print(f"[{group_name}][LIST {page_num}] ERROR -> {e}")
            break

    return collected


# ---------- LINK STORAGE ----------

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


# ---------- CARD PARSING (твой стабильный вариант) ----------

def extract_price(text: str) -> Optional[int]:
    for m in re.finditer(r"(\d[\d\s]+)\s*руб", text, re.IGNORECASE):
        value = to_int(m.group(1))
        if value and value > 100000:
            return value
    return None


def extract_total_area(text: str, lines: List[str]) -> Optional[float]:
    m = re.search(r"квартира,\s*(\d+(?:[.,]\d+)?)\s*м[²2]", text, re.IGNORECASE)
    if m:
        return to_float(m.group(1))

    for i in range(len(lines) - 1):
        if lines[i].lower() == "общая площадь":
            return to_float(lines[i + 1])

    return None


def extract_rooms(text: str) -> Optional[int]:
    m = re.search(r"(\d+)\s*-\s*комн", text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    if "студия" in text.lower():
        return 0
    return None


def extract_floor(text: str) -> Tuple[Optional[int], Optional[int]]:
    m = re.search(r"(\d+)\s*/\s*(\d+)\s*этаж", text, re.IGNORECASE)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


def extract_address_and_metro(soup: BeautifulSoup, lines: List[str]) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    address = None
    metro = None
    metro_time = None

    for line in lines:
        lower = line.lower()
        if (
            "ул." in lower
            or "улица" in lower
            or "пос" in lower
            or "проезд" in lower
            or "пр-т" in lower
            or "шоссе" in lower
            or "б-р" in lower
        ):
            address = line
            break

    metro_candidates = []

    for block in soup.find_all("div"):
        text = clean(block.get_text(" ", strip=True))

        if "мин" not in text:
            continue
        if len(text) > 200:
            continue

        matches = re.findall(
            r"([А-Яа-яЁёA-Za-z\-\s]+?)\s+(\d+)\s*мин",
            text
        )

        for station, minutes in matches:
            station = clean(station)
            minutes = int(minutes)

            bad_tokens = {
                "на карте", "карта", "ипотека", "показать телефон",
                "продаётся только у нас", "своя ставка"
            }

            if station and len(station) < 60 and station.lower() not in bad_tokens:
                metro_candidates.append((station, minutes))

    if metro_candidates:
        metro, metro_time = min(metro_candidates, key=lambda x: x[1])

    return address, metro, metro_time


def extract_kitchen_and_walls(lines: List[str]) -> Tuple[Optional[float], Optional[str]]:
    kitchen = None
    walls = None

    for i in range(len(lines) - 1):
        key = lines[i].lower()

        if key == "площадь кухни":
            kitchen = to_float(lines[i + 1])

        if key == "стены":
            walls = lines[i + 1]

    return kitchen, walls


def parse_card(link: str, source_group: str) -> dict:
    html = fetch(link)
    soup = BeautifulSoup(html, "lxml")

    text = soup.get_text("\n", strip=True)
    lines = [clean(x) for x in text.split("\n") if clean(x)]

    price = extract_price(text)
    area_total = extract_total_area(text, lines)
    rooms = extract_rooms(text)
    floor, floors = extract_floor(text)

    address, metro, metro_time = extract_address_and_metro(soup, lines)
    kitchen, walls = extract_kitchen_and_walls(lines)

    return {
        "source_group": source_group,
        "price": price,
        "area_total": area_total,
        "rooms": rooms,
        "floor": floor,
        "floors_total": floors,
        "address": address,
        "metro": metro,
        "metro_walk_min": metro_time,
        "kitchen_area": kitchen,
        "walls": walls,
        "link": link,
    }


# ---------- RESULT STORAGE ----------

def ensure_result_file(filepath: str = RESULT_FILE) -> None:
    if Path(filepath).exists():
        return

    columns = [
        "source_group",
        "price",
        "area_total",
        "rooms",
        "floor",
        "floors_total",
        "address",
        "metro",
        "metro_walk_min",
        "kitchen_area",
        "walls",
        "link",
    ]

    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
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
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "source_group",
                "price",
                "area_total",
                "rooms",
                "floor",
                "floors_total",
                "address",
                "metro",
                "metro_walk_min",
                "kitchen_area",
                "walls",
                "link",
            ],
        )
        writer.writerow(row)


# ---------- PIPELINE ----------

def run_link_collection() -> List[Dict[str, str]]:
    all_rows: List[Dict[str, str]] = []
    global_seen: Set[str] = set()

    for group_name, base_url in SEARCH_GROUPS.items():
        group_rows = collect_links_for_group_playwright(group_name, base_url, MAX_PAGES_PER_GROUP)

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

    print("\nЭтап 2: сбор карточек через requests")
    run_card_collection(link_rows)

    try:
        df = pd.read_csv(RESULT_FILE)
        print("\nПервые строки результата:")
        print(df.head(10).to_string())
        print(f"\nВсего строк в result.csv: {len(df)}")
        if "source_group" in df.columns:
            print("\nРаспределение по подвыборкам:")
            print(df["source_group"].value_counts(dropna=False).to_string())
    except Exception as e:
        print(f"Не удалось прочитать итоговый CSV: {e}")


if __name__ == "__main__":
    main()