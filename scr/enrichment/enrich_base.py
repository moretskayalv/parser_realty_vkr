import time
from math import radians, sin, cos, sqrt, atan2

import pandas as pd
import requests
import geopandas as gpd
from shapely.geometry import Point


INPUT_FILE = "result.csv"
OUTPUT_FILE = "result_final.csv"
MKAD_FILE = "mkad.geojson"

MOSCOW_CENTER_LAT = 55.7522
MOSCOW_CENTER_LON = 37.6156

STATIONS_URL = "https://raw.githubusercontent.com/nalgeon/metro/main/data/station.ru.csv"


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
        response = requests.get(url, params=params, headers=headers, timeout=15)

        if response.status_code == 200:
            data = response.json()
            return data.get("display_name")

        print(f"Ошибка геокодера: {response.status_code}")

    except Exception as error:
        print(f"Ошибка reverse geocoding: {error}")

    return None


def load_metro_stations():
    stations = pd.read_csv(STATIONS_URL)

    print("Колонки файла метро:", stations.columns.tolist())

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
        raise ValueError("Не удалось определить колонки метро: name / lat / lon")

    stations = stations[[name_col, lat_col, lon_col]].copy()
    stations.columns = ["metro", "lat", "lon"]

    stations = stations.dropna(subset=["lat", "lon"])

    return stations.to_dict("records")


def find_nearest_metro(lat, lon, stations):
    nearest_metro = None
    min_distance_km = float("inf")

    for station in stations:
        distance_km = haversine(
            lat,
            lon,
            station["lat"],
            station["lon"]
        )

        if distance_km < min_distance_km:
            min_distance_km = distance_km
            nearest_metro = station["metro"]

    metro_walk_min = round(min_distance_km * 12)

    return nearest_metro, metro_walk_min, round(min_distance_km, 2)


def distance_to_center(lat, lon):
    return round(
        haversine(
            lat,
            lon,
            MOSCOW_CENTER_LAT,
            MOSCOW_CENTER_LON
        ),
        2
    )


def load_mkad_geometry():
    mkad = gpd.read_file(MKAD_FILE)

    if mkad.empty:
        raise ValueError("Файл mkad.geojson пустой или не прочитался")

    mkad = mkad.to_crs(epsg=3857)
    return mkad.geometry.iloc[0]


def distance_to_mkad(lat, lon, mkad_geometry):
    point = gpd.GeoSeries(
        [Point(float(lon), float(lat))],
        crs="EPSG:4326"
    ).to_crs(epsg=3857)

    distance_m = point.distance(mkad_geometry).iloc[0]

    return round(distance_m / 1000, 2)


def main():
    df = pd.read_csv(INPUT_FILE)

    metro_stations = load_metro_stations()
    mkad_geometry = load_mkad_geometry()

    result_rows = []

    for index, row in df.iterrows():
        row_data = row.to_dict()

        lat = row_data.get("latitude")
        lon = row_data.get("longitude")

        has_geo = not is_empty(lat) and not is_empty(lon)

        if has_geo:
            lat = float(lat)
            lon = float(lon)

            # Адрес: если пустой — ищем по координатам
            if is_empty(row_data.get("address")):
                row_data["address"] = reverse_geocode(lat, lon)
                row_data["address_source"] = "geo"
                time.sleep(1)
            else:
                row_data["address_source"] = "raw"

            # Метро: пересчитываем для всех строк
            metro, walk_min, metro_distance_km = find_nearest_metro(
                lat,
                lon,
                metro_stations
            )

            row_data["metro"] = metro
            row_data["metro_walk_min"] = walk_min
            row_data["metro_distance_km"] = metro_distance_km
            row_data["metro_source"] = "geo_recomputed"

            # Расстояния
            row_data["distance_to_center_km"] = distance_to_center(lat, lon)
            row_data["distance_to_mkad_km"] = distance_to_mkad(
                lat,
                lon,
                mkad_geometry
            )

        else:
            row_data["address_source"] = (
                "raw" if not is_empty(row_data.get("address")) else "missing"
            )
            row_data["metro_source"] = (
                "raw" if not is_empty(row_data.get("metro")) else "missing"
            )

            row_data["metro_distance_km"] = None
            row_data["distance_to_center_km"] = None
            row_data["distance_to_mkad_km"] = None

        result_rows.append(row_data)

        if index % 50 == 0:
            print(f"Обработано строк: {index}")

    result_df = pd.DataFrame(result_rows)

    result_df.to_csv(
        OUTPUT_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    print("Готово!")
    print(f"Исходных строк: {len(df)}")
    print(f"Сохранено строк: {len(result_df)}")
    print(f"Файл: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
