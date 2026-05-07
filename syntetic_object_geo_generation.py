import os
import time
import requests
import numpy as np
import pandas as pd
import geopandas as gpd

from math import radians, sin, cos, sqrt, atan2
from shapely.geometry import Point
from sklearn.neighbors import BallTree


# =========================
# Настройки
# =========================

INPUT_FILE = "synthetic_objects_need_geo_recalculation.csv"
OUTPUT_FILE = "synthetic_objects_geo_enriched.csv"

MKAD_FILE = "mkad_osm.geojson"
POI_FILE = "moscow_poi.csv"

MOSCOW_CENTER_LAT = 55.7522
MOSCOW_CENTER_LON = 37.6156

STATIONS_URL = (
    "https://raw.githubusercontent.com/nalgeon/metro/main/data/station.ru.csv"
)

DO_REVERSE_GEOCODING = True
REVERSE_GEOCODING_SLEEP = 1


# =========================
# Базовые функции
# =========================

def is_empty(value):
    return pd.isna(value) or str(value).strip() == ""


def haversine(lat1, lon1, lat2, lon2):
    r = 6371.0

    lat1 = radians(float(lat1))
    lon1 = radians(float(lon1))
    lat2 = radians(float(lat2))
    lon2 = radians(float(lon2))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        sin(dlat / 2) ** 2
        + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    )

    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return r * c


def reverse_geocode(lat, lon):
    url = "https://nominatim.openstreetmap.org/reverse"

    params = {
        "lat": lat,
        "lon": lon,
        "format": "json",
        "accept-language": "ru",
    }

    headers = {
        "User-Agent": "real-estate-diploma-project"
    }

    try:
        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=15
        )

        if response.status_code == 200:
            data = response.json()
            return data.get("display_name")

        print(f"Ошибка геокодера: {response.status_code}")

    except Exception as error:
        print(f"Ошибка reverse geocoding: {error}")

    return None


# =========================
# Метро
# =========================

def load_metro_stations():
    stations = pd.read_csv(STATIONS_URL)

    if "city_id" in stations.columns:
        stations = stations[stations["city_id"] == 1]

    name_col = None
    lat_col = None
    lon_col = None

    for col in stations.columns:
        if col in ["name", "station_name"]:
            name_col = col
        if col in ["lat", "latitude"]:
            lat_col = col
        if col in ["lon", "lng", "longitude"]:
            lon_col = col

    if name_col is None or lat_col is None or lon_col is None:
        raise ValueError("Не удалось определить колонки метро")

    stations = stations[[name_col, lat_col, lon_col]].copy()
    stations.columns = ["metro", "latitude", "longitude"]
    stations = stations.dropna(subset=["latitude", "longitude"])

    return stations


def add_nearest_metro(df, stations):
    station_coords = np.radians(
        stations[["latitude", "longitude"]].values
    )

    object_coords = np.radians(
        df[["latitude", "longitude"]].values
    )

    tree = BallTree(
        station_coords,
        metric="haversine"
    )

    distances, indices = tree.query(
        object_coords,
        k=1
    )

    distances_km = distances[:, 0] * 6371.0
    nearest_indices = indices[:, 0]

    df["metro"] = stations.iloc[nearest_indices]["metro"].values
    df["metro_distance_km"] = np.round(distances_km, 2)
    df["metro_walk_min"] = np.round(distances_km * 12).astype(int)
    df["metro_source"] = "geo_recomputed"

    df["has_metro_near"] = (
        df["metro_walk_min"] <= 15
    ).astype(int)

    return df


# =========================
# Центр Москвы
# =========================

def add_distance_to_center(df):
    df["distance_to_center_km"] = df.apply(
        lambda row: round(
            haversine(
                row["latitude"],
                row["longitude"],
                MOSCOW_CENTER_LAT,
                MOSCOW_CENTER_LON
            ),
            2
        ),
        axis=1
    )

    return df


# =========================
# МКАД
# =========================

def add_distance_to_mkad(df):
    if not os.path.exists(MKAD_FILE):
        raise FileNotFoundError(
            f"Не найден файл МКАД: {MKAD_FILE}"
        )

    mkad = gpd.read_file(MKAD_FILE)

    if mkad.empty:
        raise ValueError("Файл МКАД пустой")

    mkad_proj = mkad.to_crs(epsg=32637)

    objects_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(
            df["longitude"],
            df["latitude"]
        ),
        crs="EPSG:4326"
    )

    objects_proj = objects_gdf.to_crs(epsg=32637)

    mkad_union = mkad_proj.geometry.unary_union

    df["distance_to_mkad_km"] = (
        objects_proj.geometry.distance(mkad_union) / 1000
    ).round(2).values

    return df


# =========================
# POI
# =========================

def load_poi():
    if not os.path.exists(POI_FILE):
        raise FileNotFoundError(
            f"Не найден файл POI: {POI_FILE}"
        )

    poi_df = pd.read_csv(POI_FILE)

    required_cols = [
        "poi_type",
        "latitude",
        "longitude"
    ]

    missing_cols = [
        col for col in required_cols
        if col not in poi_df.columns
    ]

    if missing_cols:
        raise ValueError(
            f"В POI-файле не хватает колонок: {missing_cols}"
        )

    poi_df = poi_df.dropna(
        subset=["poi_type", "latitude", "longitude"]
    )

    return poi_df


def add_nearest_poi_distances(df, poi_df):
    poi_mapping = {
        "school": "distance_to_school_km",
        "kindergarten": "distance_to_kindergarten_km",
        "clinic": "distance_to_clinic_km",
        "mall": "distance_to_mall_km",
    }

    object_coords = np.radians(
        df[["latitude", "longitude"]].values
    )

    for poi_type, output_col in poi_mapping.items():
        subset = poi_df[
            poi_df["poi_type"] == poi_type
        ].copy()

        if subset.empty:
            df[output_col] = np.nan
            continue

        poi_coords = np.radians(
            subset[["latitude", "longitude"]].values
        )

        tree = BallTree(
            poi_coords,
            metric="haversine"
        )

        distances, _ = tree.query(
            object_coords,
            k=1
        )

        distances_km = distances[:, 0] * 6371.0

        df[output_col] = np.round(distances_km, 2)

    return df


# =========================
# Адрес
# =========================

def add_address(df):
    if "address" not in df.columns:
        df["address"] = np.nan

    df["address_source"] = "missing"

    if not DO_REVERSE_GEOCODING:
        return df

    for index, row in df.iterrows():
        if is_empty(row.get("address")):
            address = reverse_geocode(
                row["latitude"],
                row["longitude"]
            )

            df.at[index, "address"] = address
            df.at[index, "address_source"] = "geo"

            time.sleep(REVERSE_GEOCODING_SLEEP)
        else:
            df.at[index, "address_source"] = "raw"

        if index % 50 == 0:
            print(f"Reverse geocoding: {index}")

    return df


# =========================
# Запуск
# =========================

df = pd.read_csv(INPUT_FILE)

df = df.dropna(
    subset=["latitude", "longitude"]
).copy()

print("Исходный synthetic dataset:", df.shape)

metro_stations = load_metro_stations()
poi_df = load_poi()

df = add_nearest_metro(df, metro_stations)
df = add_distance_to_center(df)
df = add_distance_to_mkad(df)
df = add_nearest_poi_distances(df, poi_df)
df = add_address(df)

df.to_csv(
    OUTPUT_FILE,
    index=False,
    encoding="utf-8-sig"
)

print("Готово!")
print("Сохранён файл:", OUTPUT_FILE)
print("Размер:", df.shape)