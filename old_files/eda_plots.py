import os
import pandas as pd
import matplotlib.pyplot as plt

INPUT_FILE = "data_clean.csv"
PLOTS_DIR = "plots"


def main():
    os.makedirs(PLOTS_DIR, exist_ok=True)

    df = pd.read_csv(INPUT_FILE)

    # Базовая подстраховка
    numeric_cols = [
        "price", "area_total", "kitchen_area",
        "price_per_m2", "rooms", "metro_walk_min"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 1. Распределение цены
    plt.figure(figsize=(8, 5))
    df["price"].dropna().hist(bins=40)
    plt.xlabel("Цена, руб.")
    plt.ylabel("Количество объявлений")
    plt.title("Распределение цен квартир")
    plt.tight_layout()
    plt.savefig(f"{PLOTS_DIR}/price_distribution.png", dpi=200)
    plt.close()

    # 2. Распределение общей площади
    plt.figure(figsize=(8, 5))
    df["area_total"].dropna().hist(bins=40)
    plt.xlabel("Общая площадь, м²")
    plt.ylabel("Количество объявлений")
    plt.title("Распределение общей площади")
    plt.tight_layout()
    plt.savefig(f"{PLOTS_DIR}/area_distribution.png", dpi=200)
    plt.close()

    # 3. Распределение цены за м²
    if "price_per_m2" in df.columns:
        plt.figure(figsize=(8, 5))
        df["price_per_m2"].dropna().hist(bins=40)
        plt.xlabel("Цена за м², руб.")
        plt.ylabel("Количество объявлений")
        plt.title("Распределение цены за квадратный метр")
        plt.tight_layout()
        plt.savefig(f"{PLOTS_DIR}/price_per_m2_distribution.png", dpi=200)
        plt.close()

    # 4. Количество объявлений по комнатности
    if "rooms" in df.columns:
        room_counts = df["rooms"].dropna().value_counts().sort_index()

        plt.figure(figsize=(8, 5))
        plt.bar(room_counts.index.astype(str), room_counts.values)
        plt.xlabel("Количество комнат")
        plt.ylabel("Количество объявлений")
        plt.title("Распределение объявлений по комнатности")
        plt.tight_layout()
        plt.savefig(f"{PLOTS_DIR}/rooms_distribution.png", dpi=200)
        plt.close()

    # 5. Средняя цена за м² по комнатности
    if "rooms" in df.columns and "price_per_m2" in df.columns:
        grouped = (
            df.groupby("rooms", dropna=True)["price_per_m2"]
            .mean()
            .sort_index()
        )

        plt.figure(figsize=(8, 5))
        plt.bar(grouped.index.astype(str), grouped.values)
        plt.xlabel("Количество комнат")
        plt.ylabel("Средняя цена за м², руб.")
        plt.title("Средняя цена за м² по комнатности")
        plt.tight_layout()
        plt.savefig(f"{PLOTS_DIR}/price_per_m2_by_rooms.png", dpi=200)
        plt.close()

    # 6. Топ-15 станций метро по числу объявлений
    if "metro" in df.columns:
        top_metro = df["metro"].dropna().value_counts().head(15)

        plt.figure(figsize=(10, 6))
        plt.barh(top_metro.index[::-1], top_metro.values[::-1])
        plt.xlabel("Количество объявлений")
        plt.ylabel("Станция метро")
        plt.title("Топ-15 станций метро по числу объявлений")
        plt.tight_layout()
        plt.savefig(f"{PLOTS_DIR}/top_metro.png", dpi=200)
        plt.close()

    # 7. Связь цены и площади
    plt.figure(figsize=(8, 5))
    sample = df[["area_total", "price"]].dropna().sample(
        min(1000, len(df[["area_total", "price"]].dropna())),
        random_state=42
    )
    plt.scatter(sample["area_total"], sample["price"], alpha=0.5)
    plt.xlabel("Общая площадь, м²")
    plt.ylabel("Цена, руб.")
    plt.title("Связь цены и площади")
    plt.tight_layout()
    plt.savefig(f"{PLOTS_DIR}/price_vs_area.png", dpi=200)
    plt.close()

    print("Готово. Графики сохранены в папку:", PLOTS_DIR)


if __name__ == "__main__":
    main()
