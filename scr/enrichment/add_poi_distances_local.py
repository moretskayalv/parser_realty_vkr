import os
import requests
import pandas as pd
from math import radians, sin, cos, sqrt, atan2


INPUT_FILE = "result_final_mkad_fixed.csv"
OUTPUT_FILE = "result_final_geo_poi.csv"
POI_FILE = "moscow_poi.csv"

OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]


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

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return r * c


def download_poi():
    query = """
    [out:json][timeout:240];
    area["name"="Москва"]["boundary"="administrative"]->.searchArea;
    (
      node["amenity"="school"](area.searchArea);
      way["amenity"="school"](area.searchArea);
      relation["amenity"="school"](area.searchArea);

      node["amenity"="kindergarten"](area.searchArea);
      way["amenity"="kindergarten"](area.searchArea);
      relation["amenity"="kindergarten"](area.searchArea);

      node["amenity"="clinic"](area.searchArea);
      way["amenity"="clinic"](area.searchArea);
      relation["amenity"="clinic"](area.searchArea);

      node["amenity"="hospital"](area.searchArea);
      way["amenity"="hospital"](area.searchArea);
      relation["amenity"="hospital"](area.searchArea);

      node["healthcare"="clinic"](area.searchArea);
      way["healthcare"="clinic"](area.searchArea);
      relation["healthcare"="clinic"](area.searchArea);

      node["healthcare"="hospital"](area.searchArea);
      way["healthcare"="hospital"](area.searchArea);
      relation["healthcare"="hospital"](area.searchArea);

      node["shop"="mall"](area.searchArea);
      way["shop"="mall"](area.searchArea);
      relation["shop"="mall"](area.searchArea);

      node["building"="retail"](area.searchArea);
      way["building"="retail"](area.searchArea);
      relation["building"="retail"](area.searchArea);
    );
    out center tags;
    """

    headers = {
        "User-Agent": "real-estate-diploma-project/1.0",
        "Content-Type": "text/plain; charset=utf-8",
        "Accept": "application/json",
    }

    last_error = None

    for url in OVERPASS_URLS:
        try:
            print(f"Скачиваю POI через: {url}")

            response = requests.post(
                url,
                data=query.encode("utf-8"),
                headers=headers,
                timeout=300,
            )

            print("Status:", response.status_code)

            if response.status_code != 200:
                print(response.text[:300])
                last_error = response.text[:300]
                continue

            data = response.json()
            elements = data.get("elements", [])

            rows = []

            for element in elements:
                tags = element.get("tags", {})

                lat = element.get("lat")
                lon = element.get("lon")

                if lat is None or lon is None:
                    center = element.get("center", {})
                    lat = center.get("lat")
                    lon = center.get("lon")

                if lat is None or lon is None:
                    continue

                poi_type = None

                if tags.get("amenity") == "school":
                    poi_type = "school"
                elif tags.get("amenity") == "kindergarten":
                    poi_type = "kindergarten"
                elif (
                    tags.get("amenity") in ["clinic", "hospital"]
                    or tags.get("healthcare") in ["clinic", "hospital"]
                ):
                    poi_type = "clinic"
                elif (
                    tags.get("shop") == "mall"
                    or tags.get("building") == "retail"
                ):
                    poi_type = "mall"

                if poi_type is None:
                    continue

                rows.append({
                    "poi_type": poi_type,
                    "name": tags.get("name"),
                    "latitude": lat,
                    "longitude": lon,
                })

            poi_df = pd.DataFrame(rows)

            if poi_df.empty:
                raise ValueError("POI скачались, но таблица пустая")

            poi_df = poi_df.drop_duplicates(
                subset=["poi_type", "latitude", "longitude"]
            )

            poi_df.to_csv(POI_FILE, index=False, encoding="utf-8-sig")

            print(f"POI сохранены: {POI_FILE}")
            print(poi_df["poi_type"].value_counts())

            return poi_df

        except Exception as error:
            print(f"Ошибка на {url}: {error}")
            last_error = error

    raise RuntimeError(f"Не удалось скачать POI. Последняя ошибка: {last_error}")


def load_or_download_poi():
    if os.path.exists(POI_FILE):
        print(f"Использую локальный файл: {POI_FILE}")
        poi_df = pd.read_csv(POI_FILE)

        if not poi_df.empty:
            print(poi_df["poi_type"].value_counts())
            return poi_df

    return download_poi()


def nearest_distance(lat, lon, poi_df, poi_type):
    subset = poi_df[poi_df["poi_type"] == poi_type]

    if subset.empty:
        return None

    min_distance = None

    for _, poi in subset.iterrows():
        distance = haversine(lat, lon, poi["latitude"], poi["longitude"])

        if min_distance is None or distance < min_distance:
            min_distance = distance

    return round(min_distance, 2)


def main():
    df = pd.read_csv(INPUT_FILE)
    poi_df = load_or_download_poi()

    df["distance_to_school_km"] = None
    df["distance_to_kindergarten_km"] = None
    df["distance_to_clinic_km"] = None
    df["distance_to_mall_km"] = None

    for index, row in df.iterrows():
        lat = row.get("latitude")
        lon = row.get("longitude")

        if not is_empty(lat) and not is_empty(lon):
            lat = float(lat)
            lon = float(lon)

            df.at[index, "distance_to_school_km"] = nearest_distance(
                lat, lon, poi_df, "school"
            )
            df.at[index, "distance_to_kindergarten_km"] = nearest_distance(
                lat, lon, poi_df, "kindergarten"
            )
            df.at[index, "distance_to_clinic_km"] = nearest_distance(
                lat, lon, poi_df, "clinic"
            )
            df.at[index, "distance_to_mall_km"] = nearest_distance(
                lat, lon, poi_df, "mall"
            )

        if index % 100 == 0:
            print(f"Обработано строк: {index}")

    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print("Готово")
    print(f"Файл сохранён: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()