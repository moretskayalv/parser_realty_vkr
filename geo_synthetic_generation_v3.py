"""
geo_synthetic_generation_v3.py

Улучшенная генерация синтетических объектов недвижимости, v3.

Что исправлено относительно первой версии:
1. Гео-зависимые признаки НЕ шумятся и НЕ наследуются от родителей.
   После генерации координат они полностью пересчитываются по координатам:
   - ближайшее метро, расстояние до метро, пешее время;
   - расстояние до центра Москвы;
   - расстояние до МКАД;
   - расстояния до POI: школа, детский сад, клиника, ТЦ.
2. Вместо чистой интерполяции используется смесь методов:
   - bootstrap + адаптивный шум;
   - mixup/interpolation внутри похожих объектов;
   - tail-preserving sampling для дорогих/дешёвых объектов по price_per_m2 и price.
3. Генерация идёт внутри геокластера, но второй родитель выбирается из более похожей группы:
   cluster + rooms + renovation -> cluster + rooms -> cluster.
4. Цена считается согласованно: price = price_per_m2 * area_total.
5. Используются мягкие ограничения, чтобы не убивать хвосты распределений.

Перед запуском рядом со скриптом должны лежать:
- dataset_with_geo_generation_clusters.csv
- mkad_osm.geojson
- moscow_poi.csv

Опционально интернет нужен для загрузки станций метро из GitHub.
"""

from __future__ import annotations

import os
import time
import warnings
from dataclasses import dataclass
from math import atan2, cos, radians, sin, sqrt
from pathlib import Path
from typing import Iterable

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from sklearn.model_selection import train_test_split
from sklearn.neighbors import BallTree


# =========================
# 1. Настройки
# =========================

@dataclass(frozen=True)
class Config:
    input_file: str = "dataset_with_geo_generation_clusters.csv"

    synthetic_raw_file: str = "synthetic_objects_need_geo_recalculation_v3.csv"
    synthetic_enriched_file: str = "synthetic_objects_geo_enriched_v3.csv"
    train_original_file: str = "train_original_before_synthetic_v3.csv"
    train_augmented_file: str = "train_augmented_geo_enriched_v3.csv"
    test_file: str = "test_original_not_augmented_v3.csv"
    generation_report_file: str = "geo_cluster_generation_report_v3.csv"

    mkad_file: str = "mkad_osm.geojson"
    poi_file: str = "moscow_poi.csv"
    metro_stations_local_file: str = "metro_stations.csv"
    metro_stations_url: str = "https://raw.githubusercontent.com/nalgeon/metro/main/data/station.ru.csv"

    cluster_col: str = "geo_generation_cluster"
    lat_col: str = "latitude"
    lon_col: str = "longitude"
    target_col: str = "price"
    price_per_m2_col: str = "price_per_m2"

    random_state: int = 42
    test_size: float = 0.2
    synthetic_multiplier: int = 2
    min_cluster_size: int = 5

    # Координаты Москвы, используются только для пересчёта distance_to_center_km.
    moscow_center_lat: float = 55.7522
    moscow_center_lon: float = 37.6156

    # Для координат: вокруг родителя/между родителями.
    coord_jitter_min_meters: float = 15.0
    coord_jitter_max_meters: float = 85.0

    # Генерация: доли методов.
    bootstrap_share: float = 0.65
    mixup_share: float = 0.20
    tail_share: float = 0.15

    # Reverse geocoding лучше не делать для синтетики: долго и создаёт псевдо-реальные адреса.
    do_reverse_geocoding: bool = False
    reverse_geocoding_sleep: float = 1.0

    # Если True, отсутствие MKAD/POI/metro файла не падает, а оставляет признаки NaN.
    # Для финального датасета лучше False, чтобы не пропустить ошибку входных файлов.
    allow_missing_geo_sources: bool = False


CFG = Config()


# =========================
# 2. Общие функции
# =========================

def meters_to_degrees_lat(meters: float) -> float:
    return meters / 111_000.0


def meters_to_degrees_lon(meters: float, latitude: float) -> float:
    # Долгота в градусах зависит от широты.
    return meters / (111_000.0 * max(cos(radians(latitude)), 0.1))


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0

    lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return radius_km * c


def safe_log_values(values: pd.Series | np.ndarray, lower: float = 1.0) -> np.ndarray:
    return np.log(np.asarray(values, dtype=float).clip(min=lower))


def robust_std(series: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if len(values) < 2:
        return 0.0
    iqr = values.quantile(0.75) - values.quantile(0.25)
    std = values.std()
    robust = iqr / 1.349 if iqr > 0 else std
    if pd.isna(robust):
        return 0.0
    return float(max(robust, 0.0))


def weighted_choice_method(rng: np.random.Generator, cfg: Config) -> str:
    methods = np.array(["bootstrap", "mixup", "tail"])
    probs = np.array([cfg.bootstrap_share, cfg.mixup_share, cfg.tail_share], dtype=float)
    probs = probs / probs.sum()
    return str(rng.choice(methods, p=probs))


def fill_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if {"rooms", "kitchen_area"}.issubset(df.columns):
        df["kitchen_area"] = (
            df.groupby("rooms")["kitchen_area"]
            .transform(lambda x: x.fillna(x.median()))
        )

    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if df[col].isna().any():
            median_value = df[col].median()
            df[col] = df[col].fillna(median_value)

    object_cols = df.select_dtypes(exclude=[np.number]).columns
    for col in object_cols:
        if df[col].isna().any():
            mode = df[col].mode(dropna=True)
            fill_value = mode.iloc[0] if len(mode) else "unknown"
            df[col] = df[col].fillna(fill_value)

    return df


# =========================
# 3. Ограничения недвижимости
# =========================

def clip_real_estate_constraints(df: pd.DataFrame, current_year: int = 2026) -> pd.DataFrame:
    df = df.copy()

    if "area_total" in df.columns:
        df["area_total"] = pd.to_numeric(df["area_total"], errors="coerce").clip(10, 300)

    if "rooms" in df.columns:
        # В исходных данных rooms=0 может означать студию.
        # Не превращаем студии в однушки, иначе искажается распределение rooms.
        df["rooms"] = pd.to_numeric(df["rooms"], errors="coerce").round().clip(0, 10).astype("Int64")

    if "kitchen_area" in df.columns:
        df["kitchen_area"] = pd.to_numeric(df["kitchen_area"], errors="coerce")
        if "rooms" in df.columns:
            studio_mask = df["rooms"].astype(float) == 0
            df.loc[studio_mask, "kitchen_area"] = df.loc[studio_mask, "kitchen_area"].clip(0, None)
            df.loc[~studio_mask, "kitchen_area"] = df.loc[~studio_mask, "kitchen_area"].clip(3, None)
        else:
            df["kitchen_area"] = df["kitchen_area"].clip(3, None)

        if "area_total" in df.columns:
            df["kitchen_area"] = np.minimum(df["kitchen_area"], df["area_total"] * 0.6)


    if "floor" in df.columns:
        df["floor"] = pd.to_numeric(df["floor"], errors="coerce").round().clip(1, 100).astype("Int64")

    if "floors_total" in df.columns:
        df["floors_total"] = pd.to_numeric(df["floors_total"], errors="coerce").round().clip(1, 100).astype("Int64")

    if {"floor", "floors_total"}.issubset(df.columns):
        df["floor"] = np.minimum(df["floor"].astype(float), df["floors_total"].astype(float)).round().astype("Int64")
        df["floor_ratio"] = df["floor"].astype(float) / df["floors_total"].astype(float)

    if "ceiling_height" in df.columns:
        df["ceiling_height"] = pd.to_numeric(df["ceiling_height"], errors="coerce").clip(2.3, 6.0)

    if "build_year" in df.columns:
        df["build_year"] = pd.to_numeric(df["build_year"], errors="coerce").round().clip(1800, current_year).astype("Int64")
        df["house_age"] = current_year - df["build_year"].astype(float)

    if {"area_total", "price_per_m2", "price"}.issubset(df.columns):
        df["price_per_m2"] = pd.to_numeric(df["price_per_m2"], errors="coerce").clip(1, None)
        df["price"] = df["price_per_m2"] * df["area_total"]

    return df


# =========================
# 4. Гео-зависимые признаки: не шумим, а обнуляем перед пересчётом
# =========================

def geo_dependent_columns(df: pd.DataFrame) -> list[str]:
    keywords = [
        "distance_to_",
        "nearest_",
        "metro_distance",
        "metro_walk",
        "metro_source",
        "has_metro_near",
    ]
    explicit_cols = [
        "metro",
        "address",
        "address_source",
        "house_number",
    ]

    result = []
    for col in df.columns:
        lowered = col.lower()
        if any(key in lowered for key in keywords) or col in explicit_cols:
            result.append(col)

    return sorted(set(result))


def reset_geo_dependent_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in geo_dependent_columns(df):
        df[col] = np.nan

    if "link" in df.columns:
        df["link"] = "synthetic"

    return df


# =========================
# 5. Генерация синтетики
# =========================

def get_candidate_pool(
    cluster_df: pd.DataFrame,
    parent: pd.Series,
    cfg: Config,
) -> pd.DataFrame:
    """Ищем похожих родителей с fallback: cluster+rooms+renovation -> cluster+rooms -> cluster."""
    candidates = cluster_df

    if "rooms" in cluster_df.columns:
        same_rooms = candidates[candidates["rooms"] == parent.get("rooms")]
        if len(same_rooms) >= cfg.min_cluster_size:
            candidates = same_rooms

            if "renovation" in cluster_df.columns:
                same_renovation = candidates[candidates["renovation"] == parent.get("renovation")]
                if len(same_renovation) >= cfg.min_cluster_size:
                    candidates = same_renovation

    return candidates.reset_index(drop=True)


def generate_coordinate_near_parent(
    row: pd.Series,
    cluster_df: pd.DataFrame,
    rng: np.random.Generator,
    cfg: Config,
) -> tuple[float, float]:
    jitter_m = rng.uniform(cfg.coord_jitter_min_meters, cfg.coord_jitter_max_meters)

    lat = float(row[cfg.lat_col]) + rng.normal(0, meters_to_degrees_lat(jitter_m))
    lon = float(row[cfg.lon_col]) + rng.normal(0, meters_to_degrees_lon(jitter_m, float(row[cfg.lat_col])))

    lat_margin = meters_to_degrees_lat(cfg.coord_jitter_max_meters * 3)
    lon_margin = meters_to_degrees_lon(cfg.coord_jitter_max_meters * 3, float(row[cfg.lat_col]))

    lat = float(np.clip(lat, cluster_df[cfg.lat_col].min() - lat_margin, cluster_df[cfg.lat_col].max() + lat_margin))
    lon = float(np.clip(lon, cluster_df[cfg.lon_col].min() - lon_margin, cluster_df[cfg.lon_col].max() + lon_margin))

    return lat, lon


def generate_coordinate_mixup(
    parent_a: pd.Series,
    parent_b: pd.Series,
    cluster_df: pd.DataFrame,
    lam: float,
    rng: np.random.Generator,
    cfg: Config,
) -> tuple[float, float]:
    lat = lam * float(parent_a[cfg.lat_col]) + (1 - lam) * float(parent_b[cfg.lat_col])
    lon = lam * float(parent_a[cfg.lon_col]) + (1 - lam) * float(parent_b[cfg.lon_col])

    base_lat = float(parent_a[cfg.lat_col])
    jitter_m = rng.uniform(cfg.coord_jitter_min_meters, cfg.coord_jitter_max_meters)
    lat += rng.normal(0, meters_to_degrees_lat(jitter_m))
    lon += rng.normal(0, meters_to_degrees_lon(jitter_m, base_lat))

    lat_margin = meters_to_degrees_lat(cfg.coord_jitter_max_meters * 3)
    lon_margin = meters_to_degrees_lon(cfg.coord_jitter_max_meters * 3, base_lat)

    lat = float(np.clip(lat, cluster_df[cfg.lat_col].min() - lat_margin, cluster_df[cfg.lat_col].max() + lat_margin))
    lon = float(np.clip(lon, cluster_df[cfg.lon_col].min() - lon_margin, cluster_df[cfg.lon_col].max() + lon_margin))

    return lat, lon


def perturb_continuous(
    value: float,
    col: str,
    pool: pd.DataFrame,
    rng: np.random.Generator,
    method: str,
) -> float:
    std = robust_std(pool[col])
    if std <= 0 or pd.isna(value):
        return float(value)

    if method == "tail":
        scale = rng.uniform(0.06, 0.14)
    elif method == "mixup":
        scale = rng.uniform(0.05, 0.12)
    else:
        scale = rng.uniform(0.07, 0.20)

    return float(value + rng.normal(0, std * scale))


def perturb_integer(
    value: float,
    col: str,
    pool: pd.DataFrame,
    rng: np.random.Generator,
    method: str,
) -> float:
    if pd.isna(value):
        return value

    # rooms лучше не шумить часто, иначе меняется тип объекта.
    if col == "rooms":
        return value

    std = robust_std(pool[col])
    if std <= 0:
        return value

    scale = 0.10 if method == "bootstrap" else 0.06
    return round(float(value + rng.normal(0, std * scale)))


def sample_empirical_numeric(
    col: str,
    pool: pd.DataFrame,
    rng: np.random.Generator,
    p_noise: float = 0.10,
    noise_std: float = 0.02,
    decimals: int = 2,
) -> float:
    """Для типовых numeric-признаков вроде ceiling_height: чаще берём реальное значение из похожей группы."""
    values = pd.to_numeric(pool[col], errors="coerce").dropna().values
    if len(values) == 0:
        return np.nan

    value = float(rng.choice(values))
    if rng.random() < p_noise:
        value += float(rng.normal(0, noise_std))

    return round(value, decimals)


def sample_build_year(pool: pd.DataFrame, rng: np.random.Generator, current_year: int = 2026) -> int | pd._libs.missing.NAType:
    """Год постройки лучше брать из эмпирического распределения, а не шумить как обычное число."""
    values = (
        pd.to_numeric(pool["build_year"], errors="coerce")
        .dropna()
        .round()
        .astype(int)
        .values
    )
    if len(values) == 0:
        return pd.NA

    if rng.random() < 0.85:
        return int(rng.choice(values))

    base = int(rng.choice(values))
    return int(np.clip(base + rng.integers(-2, 3), 1800, current_year))


def apply_empirical_discrete_features(row: pd.Series, pool: pd.DataFrame, rng: np.random.Generator) -> pd.Series:
    """Точечные правки для признаков, у которых распределение состоит из типовых значений."""
    if "ceiling_height" in row.index and "ceiling_height" in pool.columns:
        row["ceiling_height"] = sample_empirical_numeric(
            "ceiling_height",
            pool,
            rng,
            p_noise=0.10,
            noise_std=0.02,
            decimals=2,
        )

    if "build_year" in row.index and "build_year" in pool.columns:
        row["build_year"] = sample_build_year(pool, rng)

    return row


def get_tail_df(cluster_df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """Хвосты сохраняем не только по price_per_m2, но и по полной цене."""
    masks = []

    if cfg.price_per_m2_col in cluster_df.columns:
        low_ppm2 = cluster_df[cfg.price_per_m2_col].quantile(0.15)
        high_ppm2 = cluster_df[cfg.price_per_m2_col].quantile(0.85)
        masks.append(
            (cluster_df[cfg.price_per_m2_col] <= low_ppm2)
            | (cluster_df[cfg.price_per_m2_col] >= high_ppm2)
        )

    if cfg.target_col in cluster_df.columns:
        low_price = cluster_df[cfg.target_col].quantile(0.15)
        high_price = cluster_df[cfg.target_col].quantile(0.85)
        masks.append(
            (cluster_df[cfg.target_col] <= low_price)
            | (cluster_df[cfg.target_col] >= high_price)
        )

    if not masks:
        return cluster_df.copy()

    mask = masks[0]
    for current_mask in masks[1:]:
        mask = mask | current_mask

    return cluster_df[mask].copy()


def generate_price_per_m2(
    base_ppm2: float,
    pool: pd.DataFrame,
    train_df: pd.DataFrame,
    rng: np.random.Generator,
    cfg: Config,
    method: str,
) -> float:
    if cfg.price_per_m2_col not in pool.columns:
        return base_ppm2

    base_ppm2 = max(float(base_ppm2), 1.0)
    log_pool = safe_log_values(pool[cfg.price_per_m2_col], lower=1.0)
    log_std = float(np.nanstd(log_pool))

    if log_std <= 0:
        return base_ppm2

    if method == "tail":
        scale = rng.uniform(0.08, 0.18)
    elif method == "mixup":
        scale = rng.uniform(0.06, 0.14)
    else:
        scale = rng.uniform(0.08, 0.20)

    new_ppm2 = float(np.exp(np.log(base_ppm2) + rng.normal(0, log_std * scale)))

    # Мягкое глобальное ограничение: сохраняем почти все хвосты, но убираем совсем неадекватные выбросы.
    q_low = train_df[cfg.price_per_m2_col].quantile(0.005)
    q_high = train_df[cfg.price_per_m2_col].quantile(0.995)

    return float(np.clip(new_ppm2, q_low, q_high))


def make_bootstrap_object(
    parent: pd.Series,
    cluster_df: pd.DataFrame,
    train_df: pd.DataFrame,
    rng: np.random.Generator,
    cfg: Config,
    continuous_cols: list[str],
    integer_cols: list[str],
) -> pd.Series:
    pool = get_candidate_pool(cluster_df, parent, cfg)
    row = parent.copy()

    row[cfg.lat_col], row[cfg.lon_col] = generate_coordinate_near_parent(row, cluster_df, rng, cfg)

    for col in continuous_cols:
        row[col] = perturb_continuous(float(row[col]), col, pool, rng, method="bootstrap")

    for col in integer_cols:
        row[col] = perturb_integer(row[col], col, pool, rng, method="bootstrap")

    row = apply_empirical_discrete_features(row, pool, rng)

    if cfg.price_per_m2_col in row.index:
        row[cfg.price_per_m2_col] = generate_price_per_m2(
            row[cfg.price_per_m2_col], pool, train_df, rng, cfg, method="bootstrap"
        )

    return row


def make_mixup_object(
    parent_a: pd.Series,
    cluster_df: pd.DataFrame,
    train_df: pd.DataFrame,
    rng: np.random.Generator,
    cfg: Config,
    continuous_cols: list[str],
    integer_cols: list[str],
    categorical_cols: list[str],
) -> pd.Series:
    pool = get_candidate_pool(cluster_df, parent_a, cfg)
    parent_b = pool.iloc[rng.integers(0, len(pool))]

    # Beta(0.5, 0.5) чаще держит объект ближе к одному из родителей, а не тянет к середине.
    lam = float(rng.beta(0.5, 0.5))
    row = parent_a.copy()

    row[cfg.lat_col], row[cfg.lon_col] = generate_coordinate_mixup(parent_a, parent_b, cluster_df, lam, rng, cfg)

    for col in continuous_cols:
        mixed = lam * float(parent_a[col]) + (1 - lam) * float(parent_b[col])
        row[col] = perturb_continuous(mixed, col, pool, rng, method="mixup")

    for col in integer_cols:
        # Дискретные признаки лучше выбирать от одного родителя, а не усреднять.
        row[col] = parent_a[col] if rng.random() < lam else parent_b[col]

    for col in categorical_cols:
        row[col] = parent_a[col] if rng.random() < lam else parent_b[col]

    row = apply_empirical_discrete_features(row, pool, rng)

    if cfg.price_per_m2_col in row.index:
        mixed_ppm2 = lam * float(parent_a[cfg.price_per_m2_col]) + (1 - lam) * float(parent_b[cfg.price_per_m2_col])
        row[cfg.price_per_m2_col] = generate_price_per_m2(
            mixed_ppm2, pool, train_df, rng, cfg, method="mixup"
        )

    return row


def make_tail_object(
    cluster_df: pd.DataFrame,
    train_df: pd.DataFrame,
    rng: np.random.Generator,
    cfg: Config,
    continuous_cols: list[str],
    integer_cols: list[str],
) -> pd.Series:
    tail_df = get_tail_df(cluster_df, cfg)

    if len(tail_df) < 2:
        parent = cluster_df.iloc[rng.integers(0, len(cluster_df))]
        return make_bootstrap_object(parent, cluster_df, train_df, rng, cfg, continuous_cols, integer_cols)

    parent = tail_df.iloc[rng.integers(0, len(tail_df))]
    pool = tail_df.reset_index(drop=True)
    row = parent.copy()

    row[cfg.lat_col], row[cfg.lon_col] = generate_coordinate_near_parent(row, cluster_df, rng, cfg)

    for col in continuous_cols:
        row[col] = perturb_continuous(float(row[col]), col, pool, rng, method="tail")

    for col in integer_cols:
        row[col] = perturb_integer(row[col], col, pool, rng, method="tail")

    row = apply_empirical_discrete_features(row, pool, rng)

    if cfg.price_per_m2_col in row.index:
        row[cfg.price_per_m2_col] = generate_price_per_m2(
            row[cfg.price_per_m2_col], pool, train_df, rng, cfg, method="tail"
        )

    return row


def generate_synthetic_by_geo_cluster_v3(train_df: pd.DataFrame, cfg: Config = CFG) -> pd.DataFrame:
    rng = np.random.default_rng(cfg.random_state)
    synthetic_rows: list[pd.Series] = []

    continuous_cols = [
        # ceiling_height не шумим как continuous: он генерируется эмпирически ниже.
        col for col in ["area_total", "kitchen_area"]
        if col in train_df.columns
    ]
    integer_cols = [
        # build_year не шумим как обычный integer: он генерируется эмпирически ниже.
        col for col in ["rooms", "floor", "floors_total"]
        if col in train_df.columns
    ]
    categorical_cols = [
        col for col in [
            "source_group", "renovation", "walls", "floor_position",
            "elevator", "yard", "parking", "playground"
        ]
        if col in train_df.columns
    ]

    required_cols = [cfg.cluster_col, cfg.lat_col, cfg.lon_col]
    missing_required = [col for col in required_cols if col not in train_df.columns]
    if missing_required:
        raise ValueError(f"Не хватает обязательных колонок: {missing_required}")

    for cluster_id in sorted(train_df[cfg.cluster_col].dropna().unique()):
        cluster_df = train_df[train_df[cfg.cluster_col] == cluster_id].copy().reset_index(drop=True)
        n = len(cluster_df)
        if n < cfg.min_cluster_size:
            continue

        n_synthetic = n * cfg.synthetic_multiplier

        for _ in range(n_synthetic):
            method = weighted_choice_method(rng, cfg)
            parent = cluster_df.iloc[rng.integers(0, n)]

            if method == "mixup":
                row = make_mixup_object(
                    parent, cluster_df, train_df, rng, cfg,
                    continuous_cols, integer_cols, categorical_cols
                )
            elif method == "tail":
                row = make_tail_object(
                    cluster_df, train_df, rng, cfg,
                    continuous_cols, integer_cols
                )
            else:
                row = make_bootstrap_object(
                    parent, cluster_df, train_df, rng, cfg,
                    continuous_cols, integer_cols
                )

            synthetic_rows.append(row)

    if not synthetic_rows:
        raise ValueError("Не удалось сгенерировать синтетику: все кластеры меньше MIN_CLUSTER_SIZE.")

    synthetic_df = pd.DataFrame(synthetic_rows).reset_index(drop=True)
    synthetic_df = clip_real_estate_constraints(synthetic_df)

    if {cfg.price_per_m2_col, cfg.target_col, "area_total"}.issubset(synthetic_df.columns):
        synthetic_df[cfg.price_per_m2_col] = synthetic_df[cfg.target_col] / synthetic_df["area_total"]

    # Ключевое условие: старые geo-признаки не используются и не шумятся.
    synthetic_df = reset_geo_dependent_columns(synthetic_df)

    synthetic_df["is_synthetic"] = 1
    synthetic_df["generation_method"] = "geo_cluster_bootstrap_mixup_tail_v3"
    synthetic_df["parent_geo_cluster"] = synthetic_df[cfg.cluster_col]

    return synthetic_df


# =========================
# 6. Пересчёт гео-признаков
# =========================

def load_metro_stations(cfg: Config = CFG) -> pd.DataFrame:
    local_path = Path(cfg.metro_stations_local_file)

    if local_path.exists():
        stations = pd.read_csv(local_path)
    else:
        try:
            stations = pd.read_csv(cfg.metro_stations_url)
        except Exception as error:
            if cfg.allow_missing_geo_sources:
                warnings.warn(f"Не удалось загрузить метро: {error}")
                return pd.DataFrame(columns=["metro", "latitude", "longitude"])
            raise

    if "city_id" in stations.columns:
        stations = stations[stations["city_id"] == 1]

    name_col = next((col for col in stations.columns if col in ["name", "station_name", "metro"]), None)
    lat_col = next((col for col in stations.columns if col in ["lat", "latitude"]), None)
    lon_col = next((col for col in stations.columns if col in ["lon", "lng", "longitude"]), None)

    if name_col is None or lat_col is None or lon_col is None:
        raise ValueError("Не удалось определить колонки метро: name/lat/lon")

    stations = stations[[name_col, lat_col, lon_col]].copy()
    stations.columns = ["metro", "latitude", "longitude"]
    stations = stations.dropna(subset=["latitude", "longitude"])

    return stations


def add_nearest_metro(df: pd.DataFrame, stations: pd.DataFrame, cfg: Config = CFG) -> pd.DataFrame:
    df = df.copy()

    if stations.empty:
        df["metro"] = np.nan
        df["metro_distance_km"] = np.nan
        df["metro_walk_min"] = np.nan
        df["metro_source"] = "missing_metro_source"
        df["has_metro_near"] = np.nan
        return df

    station_coords = np.radians(stations[["latitude", "longitude"]].values)
    object_coords = np.radians(df[[cfg.lat_col, cfg.lon_col]].values)

    tree = BallTree(station_coords, metric="haversine")
    distances, indices = tree.query(object_coords, k=1)

    distances_km = distances[:, 0] * 6371.0
    nearest_indices = indices[:, 0]

    df["metro"] = stations.iloc[nearest_indices]["metro"].values
    df["metro_distance_km"] = np.round(distances_km, 2)
    df["metro_walk_min"] = np.round(distances_km * 12).astype(int)
    df["metro_source"] = "geo_recomputed"
    df["has_metro_near"] = (df["metro_walk_min"] <= 15).astype(int)

    return df


def add_distance_to_center(df: pd.DataFrame, cfg: Config = CFG) -> pd.DataFrame:
    df = df.copy()
    df["distance_to_center_km"] = [
        round(haversine_km(lat, lon, cfg.moscow_center_lat, cfg.moscow_center_lon), 2)
        for lat, lon in zip(df[cfg.lat_col], df[cfg.lon_col])
    ]
    return df


def add_distance_to_mkad(df: pd.DataFrame, cfg: Config = CFG) -> pd.DataFrame:
    df = df.copy()
    mkad_path = Path(cfg.mkad_file)

    if not mkad_path.exists():
        if cfg.allow_missing_geo_sources:
            warnings.warn(f"Не найден файл МКАД: {cfg.mkad_file}")
            df["distance_to_mkad_km"] = np.nan
            return df
        raise FileNotFoundError(f"Не найден файл МКАД: {cfg.mkad_file}")

    mkad = gpd.read_file(mkad_path)
    if mkad.empty:
        raise ValueError("Файл МКАД пустой")

    mkad_proj = mkad.to_crs(epsg=32637)
    objects_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[cfg.lon_col], df[cfg.lat_col]),
        crs="EPSG:4326",
    )
    objects_proj = objects_gdf.to_crs(epsg=32637)

    mkad_union = mkad_proj.geometry.union_all() if hasattr(mkad_proj.geometry, "union_all") else mkad_proj.geometry.unary_union
    df["distance_to_mkad_km"] = (objects_proj.geometry.distance(mkad_union) / 1000).round(2).values

    return df


def load_poi(cfg: Config = CFG) -> pd.DataFrame:
    poi_path = Path(cfg.poi_file)
    if not poi_path.exists():
        if cfg.allow_missing_geo_sources:
            warnings.warn(f"Не найден файл POI: {cfg.poi_file}")
            return pd.DataFrame(columns=["poi_type", "latitude", "longitude"])
        raise FileNotFoundError(f"Не найден файл POI: {cfg.poi_file}")

    poi_df = pd.read_csv(poi_path)
    required_cols = ["poi_type", "latitude", "longitude"]
    missing_cols = [col for col in required_cols if col not in poi_df.columns]
    if missing_cols:
        raise ValueError(f"В POI-файле не хватает колонок: {missing_cols}")

    return poi_df.dropna(subset=required_cols).copy()


def add_nearest_poi_distances(df: pd.DataFrame, poi_df: pd.DataFrame, cfg: Config = CFG) -> pd.DataFrame:
    df = df.copy()
    poi_mapping = {
        "school": "distance_to_school_km",
        "kindergarten": "distance_to_kindergarten_km",
        "clinic": "distance_to_clinic_km",
        "mall": "distance_to_mall_km",
    }

    object_coords = np.radians(df[[cfg.lat_col, cfg.lon_col]].values)

    for poi_type, output_col in poi_mapping.items():
        subset = poi_df[poi_df["poi_type"] == poi_type].copy()
        if subset.empty:
            df[output_col] = np.nan
            continue

        poi_coords = np.radians(subset[["latitude", "longitude"]].values)
        tree = BallTree(poi_coords, metric="haversine")
        distances, _ = tree.query(object_coords, k=1)
        df[output_col] = np.round(distances[:, 0] * 6371.0, 2)

    return df


def reverse_geocode(lat: float, lon: float) -> str | None:
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {"lat": lat, "lon": lon, "format": "json", "accept-language": "ru"}
    headers = {"User-Agent": "real-estate-diploma-project"}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=15)
        if response.status_code == 200:
            return response.json().get("display_name")
        warnings.warn(f"Ошибка геокодера: {response.status_code}")
    except Exception as error:
        warnings.warn(f"Ошибка reverse geocoding: {error}")

    return None


def add_address(df: pd.DataFrame, cfg: Config = CFG) -> pd.DataFrame:
    df = df.copy()

    if "address" not in df.columns:
        df["address"] = np.nan
    if "address_source" not in df.columns:
        df["address_source"] = np.nan

    if not cfg.do_reverse_geocoding:
        # Не ставим специальный маркер вроде "geo" / "synthetic" / "not_used_for_synthetic":
        # такая колонка становится leakage-признаком в real-vs-synthetic classifier.
        df["address"] = np.nan
        df["address_source"] = np.nan
        return df

    for index, row in df.iterrows():
        df.at[index, "address"] = reverse_geocode(row[cfg.lat_col], row[cfg.lon_col])
        df.at[index, "address_source"] = "geo"
        time.sleep(cfg.reverse_geocoding_sleep)

        if index % 50 == 0:
            print(f"Reverse geocoding: {index}")

    return df


def recompute_geo_features(df: pd.DataFrame, cfg: Config = CFG) -> pd.DataFrame:
    """Полный пересчёт geo-зависимых признаков по latitude/longitude."""
    df = df.copy()
    df = df.dropna(subset=[cfg.lat_col, cfg.lon_col]).copy()

    # Ещё раз очищаем, чтобы случайно не остались значения от родительских реальных объектов.
    df = reset_geo_dependent_columns(df)

    metro_stations = load_metro_stations(cfg)
    poi_df = load_poi(cfg)

    df = add_nearest_metro(df, metro_stations, cfg)
    df = add_distance_to_center(df, cfg)
    df = add_distance_to_mkad(df, cfg)
    df = add_nearest_poi_distances(df, poi_df, cfg)
    df = add_address(df, cfg)

    return df


# =========================
# 7. Контроль качества после генерации
# =========================

def business_rule_report(df: pd.DataFrame, cfg: Config = CFG) -> dict[str, float]:
    report = {}

    if {"floor", "floors_total"}.issubset(df.columns):
        report["floor_lte_floors_total_share"] = float((df["floor"].astype(float) <= df["floors_total"].astype(float)).mean())

    if {"kitchen_area", "area_total"}.issubset(df.columns):
        report["kitchen_lte_area_share"] = float((df["kitchen_area"].astype(float) <= df["area_total"].astype(float)).mean())

    if {cfg.target_col, cfg.price_per_m2_col, "area_total"}.issubset(df.columns):
        expected_price = df[cfg.price_per_m2_col].astype(float) * df["area_total"].astype(float)
        relative_error = (df[cfg.target_col].astype(float) - expected_price).abs() / df[cfg.target_col].astype(float).clip(lower=1)
        report["price_formula_median_relative_error"] = float(relative_error.median())
        report["price_formula_p95_relative_error"] = float(relative_error.quantile(0.95))

    for col in ["metro_distance_km", "distance_to_center_km", "distance_to_mkad_km"]:
        if col in df.columns:
            report[f"{col}_missing_share"] = float(df[col].isna().mean())

    return report


def make_generation_report(
    train_original: pd.DataFrame,
    synthetic_raw: pd.DataFrame,
    synthetic_enriched: pd.DataFrame,
    cfg: Config = CFG,
) -> pd.DataFrame:
    cluster_report = (
        train_original.groupby(cfg.cluster_col)
        .size()
        .rename("original_rows")
        .reset_index()
    )

    synth_report = (
        synthetic_enriched.groupby(cfg.cluster_col)
        .size()
        .rename("synthetic_rows")
        .reset_index()
    )

    report = cluster_report.merge(synth_report, on=cfg.cluster_col, how="left")
    report["synthetic_rows"] = report["synthetic_rows"].fillna(0).astype(int)
    report["synthetic_to_original_ratio"] = report["synthetic_rows"] / report["original_rows"]

    # Сколько geo-признаков реально пересчитано.
    geo_cols = [
        "metro_distance_km",
        "distance_to_center_km",
        "distance_to_mkad_km",
        "distance_to_school_km",
        "distance_to_kindergarten_km",
        "distance_to_clinic_km",
        "distance_to_mall_km",
    ]
    existing_geo_cols = [col for col in geo_cols if col in synthetic_enriched.columns]
    for col in existing_geo_cols:
        report[f"{col}_non_missing_share_global"] = 1.0 - synthetic_enriched[col].isna().mean()

    return report


# =========================
# 8. Запуск пайплайна
# =========================

def main(cfg: Config = CFG) -> None:
    df = pd.read_csv(cfg.input_file)
    df = fill_missing_values(df)

    train_df, test_df = train_test_split(
        df,
        test_size=cfg.test_size,
        random_state=cfg.random_state,
    )

    train_df = train_df.copy().reset_index(drop=True)
    test_df = test_df.copy().reset_index(drop=True)

    synthetic_raw = generate_synthetic_by_geo_cluster_v3(train_df, cfg)
    synthetic_raw.to_csv(cfg.synthetic_raw_file, index=False, encoding="utf-8-sig")

    synthetic_enriched = recompute_geo_features(synthetic_raw, cfg)
    synthetic_enriched.to_csv(cfg.synthetic_enriched_file, index=False, encoding="utf-8-sig")

    train_original_marked = train_df.copy()
    train_original_marked["is_synthetic"] = 0
    train_original_marked["generation_method"] = "original"
    train_original_marked["parent_geo_cluster"] = train_original_marked[cfg.cluster_col]
    train_original_marked.to_csv(cfg.train_original_file, index=False, encoding="utf-8-sig")

    train_augmented = pd.concat([train_original_marked, synthetic_enriched], ignore_index=True)
    train_augmented.to_csv(cfg.train_augmented_file, index=False, encoding="utf-8-sig")

    test_df.to_csv(cfg.test_file, index=False, encoding="utf-8-sig")

    generation_report = make_generation_report(train_original_marked, synthetic_raw, synthetic_enriched, cfg)
    generation_report.to_csv(cfg.generation_report_file, index=False, encoding="utf-8-sig")

    print("Готово.")
    print(f"Original dataset: {df.shape}")
    print(f"Train original: {train_original_marked.shape}")
    print(f"Synthetic raw: {synthetic_raw.shape}")
    print(f"Synthetic enriched: {synthetic_enriched.shape}")
    print(f"Train augmented: {train_augmented.shape}")
    print(f"Test untouched: {test_df.shape}")

    print("\nBusiness-rule report for synthetic enriched:")
    for key, value in business_rule_report(synthetic_enriched, cfg).items():
        print(f"- {key}: {value:.6f}")

    print("\nСохранены файлы:")
    for path in [
        cfg.synthetic_raw_file,
        cfg.synthetic_enriched_file,
        cfg.train_original_file,
        cfg.train_augmented_file,
        cfg.test_file,
        cfg.generation_report_file,
    ]:
        print(f"- {path}")


if __name__ == "__main__":
    main(CFG)
