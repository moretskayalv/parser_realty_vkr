import pandas as pd
import numpy as np

from pathlib import Path
from sklearn.model_selection import train_test_split


# =========================
# 1. Настройки
# =========================

INPUT_FILE = "dataset_with_spatial_districts.csv"

CLUSTER_COL = "district_cluster"
LAT_COL = "latitude"
LON_COL = "longitude"
TARGET_COL = "price"

RANDOM_STATE = 42
TEST_SIZE = 0.2

COPIES_PER_CLUSTER = 2
COORD_NOISE_METERS = 80
DISTANCE_NOISE = 0.03
CONTINUOUS_NOISE = 0.01


# =========================
# 2. Загрузка данных
# =========================

df = pd.read_csv(INPUT_FILE)

print("Исходный размер:", df.shape)


# =========================
# 3. Train / test split
# =========================

train_df, test_df = train_test_split(
    df,
    test_size=TEST_SIZE,
    random_state=RANDOM_STATE
)

train_df = train_df.copy()
test_df = test_df.copy()


# =========================
# 4. Определяем группы колонок
# =========================

distance_cols = [
    col for col in train_df.columns
    if any(key in col.lower() for key in [
        "distance",
        "nearest",
        "metro",
        "mkad",
        "center",
        "school",
        "kindergarten",
        "clinic",
        "mall"
    ])
    and pd.api.types.is_numeric_dtype(train_df[col])
]

safe_continuous_cols = [
    col for col in train_df.select_dtypes(include=[np.number]).columns
    if col not in [
        LAT_COL,
        LON_COL,
        CLUSTER_COL,
        TARGET_COL
    ]
    and "price" not in col.lower()
    and col not in distance_cols
]

integer_like_cols = [
    col for col in safe_continuous_cols
    if any(key in col.lower() for key in [
        "room",
        "floor",
        "year"
    ])
]

safe_continuous_cols = [
    col for col in safe_continuous_cols
    if col not in integer_like_cols
]

print("\nDistance columns:")
print(distance_cols)

print("\nContinuous columns:")
print(safe_continuous_cols)


# =========================
# 5. Функция аугментации
# =========================

def generate_cluster_synthetic_data(
    train_data,
    cluster_col,
    lat_col,
    lon_col,
    copies_per_cluster=2,
    coord_noise_meters=80,
    distance_noise=0.03,
    continuous_noise=0.01,
    random_state=42
):
    rng = np.random.default_rng(random_state)
    meter_to_degree = 1 / 111_000

    augmented_parts = []

    original = train_data.copy()
    original["is_synthetic"] = 0
    original["augmentation_copy"] = 0

    augmented_parts.append(original)

    for cluster_id in sorted(train_data[cluster_col].dropna().unique()):

        cluster_df = train_data[
            train_data[cluster_col] == cluster_id
        ]

        if len(cluster_df) < 5:
            continue

        for copy_id in range(1, copies_per_cluster + 1):

            synthetic = cluster_df.copy()

            # ---- 1. Микро-сдвиг координат ----
            synthetic[lat_col] = synthetic[lat_col] + rng.normal(
                loc=0,
                scale=coord_noise_meters * meter_to_degree,
                size=len(synthetic)
            )

            synthetic[lon_col] = synthetic[lon_col] + rng.normal(
                loc=0,
                scale=coord_noise_meters * meter_to_degree,
                size=len(synthetic)
            )

            # ---- 2. Шум для расстояний ----
            for col in distance_cols:
                synthetic[col] = synthetic[col] * rng.normal(
                    loc=1.0,
                    scale=distance_noise,
                    size=len(synthetic)
                )

                synthetic[col] = synthetic[col].clip(lower=0)

            # ---- 3. Шум для непрерывных числовых признаков ----
            for col in safe_continuous_cols:

                std = cluster_df[col].std()

                if pd.notna(std) and std > 0:
                    synthetic[col] = synthetic[col] + rng.normal(
                        loc=0,
                        scale=std * continuous_noise,
                        size=len(synthetic)
                    )

            # ---- 4. Целевую переменную НЕ трогаем ----
            synthetic["is_synthetic"] = 1
            synthetic["augmentation_copy"] = copy_id

            augmented_parts.append(synthetic)

    return pd.concat(augmented_parts, ignore_index=True)


# =========================
# 6. Генерация синтетического train
# =========================

train_augmented = generate_cluster_synthetic_data(
    train_data=train_df,
    cluster_col=CLUSTER_COL,
    lat_col=LAT_COL,
    lon_col=LON_COL,
    copies_per_cluster=COPIES_PER_CLUSTER,
    coord_noise_meters=COORD_NOISE_METERS,
    distance_noise=DISTANCE_NOISE,
    continuous_noise=CONTINUOUS_NOISE,
    random_state=RANDOM_STATE
)


# =========================
# 7. Отчёт по кластерам
# =========================

cluster_report = (
    train_augmented
    .groupby(CLUSTER_COL)
    .agg(
        total_rows=(CLUSTER_COL, "size"),
        synthetic_rows=("is_synthetic", "sum")
    )
    .reset_index()
)

cluster_report["original_rows"] = (
    cluster_report["total_rows"] -
    cluster_report["synthetic_rows"]
)


# =========================
# 8. Сохранение файлов
# =========================

train_augmented.to_csv(
    "train_cluster_augmented.csv",
    index=False
)

train_df.to_csv(
    "train_original.csv",
    index=False
)

test_df.to_csv(
    "test_original_not_augmented.csv",
    index=False
)

cluster_report.to_csv(
    "cluster_augmentation_report.csv",
    index=False
)


# =========================
# 9. Итог
# =========================

print("\nГотово!")

print("Train до аугментации:", train_df.shape)
print("Test без аугментации:", test_df.shape)
print("Train после аугментации:", train_augmented.shape)

print(
    "Синтетических строк:",
    int(train_augmented["is_synthetic"].sum())
)

print("\nСохранены файлы:")
print("train_cluster_augmented.csv")
print("train_original.csv")
print("test_original_not_augmented.csv")
print("cluster_augmentation_report.csv")
