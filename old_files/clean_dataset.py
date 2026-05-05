import re
import numpy as np
import pandas as pd


INPUT_FILE = "result.csv"
OUTPUT_FILE = "data_clean.csv"


def clean_text(value):
    if pd.isna(value):
        return np.nan
    text = str(value)
    text = re.sub(r"\s+", " ", text).strip()
    return text if text else np.nan


def clean_address(value):
    text = clean_text(value)
    if pd.isna(text):
        return np.nan

    # Явный мусор
    if re.search(r"^Посмотреть \d+ фото$", text, flags=re.IGNORECASE):
        return np.nan

    # Убираем "На карте ..." если вдруг попало
    text = re.sub(r"^На карте\s+", "", text, flags=re.IGNORECASE).strip()

    # Убираем префикс вида:
    # "4к, 97.4м², 10 этаж, "
    # "3к, 81.5м², 7/12 этаж, "
    text = re.sub(
        r"^\s*\d+\s*к\s*,\s*\d+(?:[.,]\d+)?\s*м[²2]\s*,\s*\d+(?:/\d+)?\s*этаж\s*,\s*",
        "",
        text,
        flags=re.IGNORECASE,
    )

    # Иногда бывает только часть префикса
    text = re.sub(
        r"^\s*\d+\s*к\s*,\s*\d+(?:[.,]\d+)?\s*м[²2]\s*,\s*",
        "",
        text,
        flags=re.IGNORECASE,
    )

    # Убираем хвосты
    text = re.sub(r",\s*(квартира|апартаменты)\s*$", "", text, flags=re.IGNORECASE).strip()

    # Убираем служебные хвосты
    text = re.sub(r"Показать телефон.*$", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"Записаться на просмотр.*$", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"Ипотека.*$", "", text, flags=re.IGNORECASE).strip()

    # Если строка после очистки слишком короткая — это не адрес
    if len(text) < 8:
        return np.nan

    # Должен быть хотя бы один адресный маркер
    if not re.search(
        r"(ул\.|улица|просп|пр-т|шоссе|проезд|переул|пер\.|пос[её]лок|б-р|бульвар|наб\.|набережная)",
        text.lower(),
    ):
        return np.nan

    return text


def clean_metro(value):
    text = clean_text(value)
    if pd.isna(text):
        return np.nan

    # Убираем префикс "На карте"
    text = re.sub(r"^На карте\s+", "", text, flags=re.IGNORECASE).strip()

    # Убираем явный мусор
    bad_exact = {
        "находится в",
        "ипотека",
        "показать телефон",
        "карта",
        "на карте",
    }
    if text.lower() in bad_exact:
        return np.nan

    # Если в метро склеилось несколько станций, берём первую нормальную фразу
    # Примеры:
    # "Павелецкая"
    # "Верхние Лихоборы Бибирево" -> берём "Верхние Лихоборы"
    # "Театральная Чеховская" -> берём "Театральная"
    words = text.split()

    if len(words) >= 3:
        # Если первые два слова похожи на составное название станции — оставляем 2 слова
        # Верхние Лихоборы, Деловой центр, Мичуринский проспект и т.п.
        first_two = " ".join(words[:2])

        known_two_word_prefixes = [
            "Верхние Лихоборы",
            "Деловой центр",
            "Мичуринский проспект",
            "Рязанский проспект",
            "Октябрьское Поле",
            "Пятницкое шоссе",
            "Лермонтовский проспект",
            "Проспект Вернадского",
            "Нижегородская улица",  # на всякий случай
        ]

        if first_two in known_two_word_prefixes:
            text = first_two
        else:
            text = words[0]

    # Удаляем всё, что не похоже на название станции
    text = re.sub(r"[^\w\sА-Яа-яЁё\-]", "", text).strip()
    text = re.sub(r"\s+", " ", text).strip()

    if len(text) < 3 or len(text) > 40:
        return np.nan

    return text


def normalize_walls(value):
    text = clean_text(value)
    if pd.isna(text):
        return np.nan

    text_lower = text.lower()

    if "монолит" in text_lower:
        return "монолит"
    if "кирп" in text_lower:
        return "кирпич"
    if "панел" in text_lower:
        return "панель"
    if "железобетон" in text_lower:
        return "железобетон"
    if "блок" in text_lower:
        return "блок"

    return text_lower


def to_numeric(series):
    return pd.to_numeric(series, errors="coerce")


def main():
    df = pd.read_csv(INPUT_FILE)

    print("Исходный размер:", df.shape)

    # 1. Дедупликация по ссылке
    df = df.drop_duplicates(subset=["link"]).copy()
    print("После удаления дублей:", df.shape)

    # 2. Базовая очистка текстовых полей
    for col in ["source_group", "address", "metro", "walls", "link"]:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)

    # 3. Точечная очистка адреса и метро
    if "address" in df.columns:
        df["address_raw"] = df["address"]
        df["address"] = df["address"].apply(clean_address)

    if "metro" in df.columns:
        df["metro_raw"] = df["metro"]
        df["metro"] = df["metro"].apply(clean_metro)

    # 4. Числовые поля
    numeric_cols = [
        "price",
        "area_total",
        "rooms",
        "floor",
        "floors_total",
        "metro_walk_min",
        "kitchen_area",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = to_numeric(df[col])

    # 5. МЯГКИЕ фильтры качества
    # Не режем слишком агрессивно
    df = df[df["price"].notna()]
    df = df[df["price"] > 1_000_000]

    df = df[df["area_total"].notna()]
    df = df[(df["area_total"] >= 10) & (df["area_total"] <= 400)]

    if "rooms" in df.columns:
        df = df[(df["rooms"].isna()) | ((df["rooms"] >= 0) & (df["rooms"] <= 10))]

    if "kitchen_area" in df.columns:
        df = df[(df["kitchen_area"].isna()) | ((df["kitchen_area"] >= 2) & (df["kitchen_area"] <= 100))]

    if "floor" in df.columns:
        df = df[(df["floor"].isna()) | ((df["floor"] >= 1) & (df["floor"] <= 100))]

    if "floors_total" in df.columns:
        df = df[(df["floors_total"].isna()) | ((df["floors_total"] >= 1) & (df["floors_total"] <= 100))]

    # floor <= floors_total
    df = df[
        (df["floor"].isna())
        | (df["floors_total"].isna())
        | (df["floor"] <= df["floors_total"])
    ]

    if "metro_walk_min" in df.columns:
        df = df[
            (df["metro_walk_min"].isna())
            | ((df["metro_walk_min"] >= 1) & (df["metro_walk_min"] <= 180))
        ]

    # 6. Производная фича
    df["price_per_m2"] = df["price"] / df["area_total"]

    # Очень мягкий фильтр по цене за метр
    df = df[
        (df["price_per_m2"] >= 30_000)
        & (df["price_per_m2"] <= 3_000_000)
    ]

    # 7. Нормализация стен
    if "walls" in df.columns:
        df["walls_clean"] = df["walls"].apply(normalize_walls)

    # 8. Порядок колонок
    preferred_order = [
        "source_group",
        "price",
        "area_total",
        "kitchen_area",
        "price_per_m2",
        "rooms",
        "floor",
        "floors_total",
        "address",
        "metro",
        "metro_walk_min",
        "walls",
        "walls_clean",
        "link",
        "address_raw",
        "metro_raw",
    ]

    existing_cols = [col for col in preferred_order if col in df.columns]
    other_cols = [col for col in df.columns if col not in existing_cols]
    df = df[existing_cols + other_cols]

    # 9. Сохраняем
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print("После очистки:", df.shape)
    print("\nПропуски:")
    print(df.isna().sum())
    print("\nПервые строки:")
    print(df.head(10).to_string())


if __name__ == "__main__":
    main()
