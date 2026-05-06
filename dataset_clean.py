import pandas as pd
import numpy as np

# =========================
# 1. Загрузка данных
# =========================

url = "https://raw.githubusercontent.com/moretskayalv/parser_realty_vkr/main/outputs/result_final_geo_poi.csv"

df = pd.read_csv(url)

print("Исходный размер:", df.shape)


# =========================
# 2. Нормализация названий колонок
# =========================

df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
)


# =========================
# 3. Приведение числовых колонок
# =========================

numeric_cols = [
    "price",
    "area_total",
    "area_living",
    "area_kitchen",
    "floor",
    "floors_total",
    "build_year",
    "ceiling_height",
    "latitude",
    "longitude",
    "distance_to_center_km",
    "distance_to_mkad_km",
    "nearest_school_km",
    "nearest_kindergarten_km",
    "nearest_clinic_km",
    "nearest_mall_km",
    "metro_distance_km",
    "metro_walk_min"
]

for col in numeric_cols:
    if col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .str.replace(" ", "", regex=False)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")


# =========================
# 4. Удаление критичных пропусков
# =========================

critical_cols = [
    "price",
    "area_total",
    "latitude",
    "longitude"
]

critical_cols = [col for col in critical_cols if col in df.columns]

df = df.dropna(subset=critical_cols)

print("После удаления критичных пропусков:", df.shape)


# =========================
# 5. Удаление дубликатов
# =========================

duplicate_subset = [
    "address",
    "price",
    "area_total",
    "floor",
    "latitude",
    "longitude"
]

duplicate_subset = [col for col in duplicate_subset if col in df.columns]

df = df.drop_duplicates(subset=duplicate_subset)

print("После удаления дубликатов:", df.shape)


# =========================
# 6. Фильтрация выбросов
# =========================

if "price" in df.columns:
    df = df[
        (df["price"] >= 3_000_000) &
        (df["price"] <= 300_000_000)
    ]

if "area_total" in df.columns:
    df = df[
        (df["area_total"] >= 10) &
        (df["area_total"] <= 300)
    ]

if "floor" in df.columns:
    df = df[
        (df["floor"] >= 1) &
        (df["floor"] <= 100)
    ]

if "floors_total" in df.columns:
    df = df[
        (df["floors_total"] >= 1) &
        (df["floors_total"] <= 100)
    ]

if {"floor", "floors_total"}.issubset(df.columns):
    df = df[df["floor"] <= df["floors_total"]]

if "ceiling_height" in df.columns:
    df.loc[
        (df["ceiling_height"] < 2.3) |
        (df["ceiling_height"] > 6),
        "ceiling_height"
    ] = np.nan

if "build_year" in df.columns:
    df.loc[
        (df["build_year"] < 1800) |
        (df["build_year"] > 2026),
        "build_year"
    ] = np.nan

if "distance_to_center_km" in df.columns:
    df = df[df["distance_to_center_km"] <= 60]

if "distance_to_mkad_km" in df.columns:
    df = df[df["distance_to_mkad_km"] <= 80]

print("После фильтрации выбросов:", df.shape)


# =========================
# 7. Заполнение пропусков
# =========================

median_fill_cols = [
    "build_year",
    "ceiling_height",
    "area_living",
    "area_kitchen",
    "metro_distance_km",
    "metro_walk_min",
    "nearest_school_km",
    "nearest_kindergarten_km",
    "nearest_clinic_km",
    "nearest_mall_km"
]

for col in median_fill_cols:
    if col in df.columns:
        df[col] = df[col].fillna(df[col].median())


categorical_cols = [
    "address",
    "metro_station",
    "walls",
    "parking",
    "yard",
    "elevator",
    "renovation",
    "building_type"
]

for col in categorical_cols:
    if col in df.columns:
        df[col] = (
            df[col]
            .fillna("unknown")
            .astype(str)
            .str.strip()
            .str.lower()
        )


# =========================
# 8. Feature Engineering
# =========================

if {"price", "area_total"}.issubset(df.columns):
    df["price_per_m2"] = df["price"] / df["area_total"]

if {"floor", "floors_total"}.issubset(df.columns):
    df["floor_ratio"] = df["floor"] / df["floors_total"]

if "build_year" in df.columns:
    df["house_age"] = 2026 - df["build_year"]

if "metro_walk_min" in df.columns:
    df["has_metro_near"] = (df["metro_walk_min"] <= 15).astype(int)


# =========================
# 9. Финальная проверка
# =========================

print("\nФинальный размер:", df.shape)
print("\nПропуски:")
print(df.isna().sum().sort_values(ascending=False).head(20))


# =========================
# 10. Сохранение
# =========================

df.to_csv("dataset_clean_before_encoding.csv", index=False)

print("\nФайл сохранён: dataset_clean_before_encoding.csv")