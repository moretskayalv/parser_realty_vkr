import pandas as pd
import requests
import geopandas as gpd
from shapely.geometry import LineString, Point


INPUT_FILE = "result_final.csv"
OUTPUT_FILE = "result_final_mkad_fixed.csv"
MKAD_FILE = "mkad_osm.geojson"

OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]


def download_mkad_from_osm():
    query = """
    [out:json][timeout:180];
    relation(2094222);
    (._;>;);
    out body;
    """

    headers = {
        "User-Agent": "real-estate-diploma-project/1.0",
        "Content-Type": "text/plain; charset=utf-8",
        "Accept": "application/json",
    }

    last_error = None

    for url in OVERPASS_URLS:
        try:
            print(f"Пробую скачать МКАД через: {url}")

            response = requests.post(
                url,
                data=query.encode("utf-8"),
                headers=headers,
                timeout=240,
            )

            print("Status:", response.status_code)

            if response.status_code != 200:
                print(response.text[:300])
                last_error = response.text[:300]
                continue

            data = response.json()

            nodes = {}
            ways = []

            for element in data.get("elements", []):
                if element.get("type") == "node":
                    nodes[element["id"]] = (
                        element["lon"],
                        element["lat"],
                    )

            for element in data.get("elements", []):
                if element.get("type") == "way":
                    coords = []

                    for node_id in element.get("nodes", []):
                        if node_id in nodes:
                            coords.append(nodes[node_id])

                    if len(coords) >= 2:
                        ways.append(LineString(coords))

            if not ways:
                raise ValueError("МКАД скачался, но линии не найдены")

            gdf = gpd.GeoDataFrame(
                {"name": ["MKAD"] * len(ways)},
                geometry=ways,
                crs="EPSG:4326",
            )

            gdf.to_file(MKAD_FILE, driver="GeoJSON")

            print(f"МКАД сохранён в файл: {MKAD_FILE}")
            print(f"Сегментов МКАД: {len(gdf)}")

            return gdf

        except Exception as error:
            print(f"Ошибка на {url}: {error}")
            last_error = error

    raise RuntimeError(f"Не удалось скачать МКАД. Последняя ошибка: {last_error}")


def load_mkad():
    try:
        print(f"Пробую прочитать локальный файл: {MKAD_FILE}")
        mkad = gpd.read_file(MKAD_FILE)

        if mkad.empty:
            raise ValueError("Локальный файл МКАД пустой")

        print("Локальный МКАД найден")
        return mkad

    except Exception:
        print("Локальный МКАД не найден, скачиваю из OSM...")
        return download_mkad_from_osm()


def distance_to_mkad_real(lat, lon, mkad_gdf):
    if pd.isna(lat) or pd.isna(lon):
        return None

    point = gpd.GeoSeries(
        [Point(float(lon), float(lat))],
        crs="EPSG:4326",
    ).to_crs(epsg=32637)

    mkad_proj = mkad_gdf.to_crs(epsg=32637)

    distance_m = mkad_proj.distance(point.iloc[0]).min()

    return round(distance_m / 1000, 2)


def main():
    df = pd.read_csv(INPUT_FILE)
    mkad = load_mkad()

    df["distance_to_mkad_km"] = df.apply(
        lambda row: distance_to_mkad_real(
            row["latitude"],
            row["longitude"],
            mkad,
        ),
        axis=1,
    )

    df.to_csv(
        OUTPUT_FILE,
        index=False,
        encoding="utf-8-sig",
    )

    print("Готово")
    print(f"Файл сохранён: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()