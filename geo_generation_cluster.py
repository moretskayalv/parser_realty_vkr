import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split


# =========================
# 1. Настройки
# =========================

INPUT_FILE = "dataset_with_geo_generation_clusters.csv"

CLUSTER_COL = "geo_generation_cluster"
LAT_COL = "latitude"
LON_COL = "longitude"

TARGET_COL = "price"
PRICE_PER_M2_COL = "price_per_m2"

RANDOM_STATE = 42
TEST_SIZE = 0.2

SYNTHETIC_MULTIPLIER = 2
COORD_JITTER_METERS = 35
MIN_CLUSTER_SIZE = 5


# =========================
# 2. Вспомогательные функции
# =========================

def meters_to_degrees(meters):
    return meters / 111_000


def clip_real_estate_constraints(df):
    if "area_total" in df.columns:
        df["area_total"] = df["area_total"].clip(10, 300)

    if "kitchen_area" in df.columns:
        df["kitchen_area"] = df["kitchen_area"].clip(3, None)

        if "area_total" in df.columns:
            df["kitchen_area"] = np.minimum(
                df["kitchen_area"],
                df["area_total"] * 0.6
            )

    if "rooms" in df.columns:
        df["rooms"] = df["rooms"].round().clip(1, 10)

    if "floor" in df.columns:
        df["floor"] = df["floor"].round().clip(1, 100)

    if "floors_total" in df.columns:
        df["floors_total"] = df["floors_total"].round().clip(1, 100)

    if {"floor", "floors_total"}.issubset(df.columns):
        df["floor"] = np.minimum(df["floor"], df["floors_total"])
        df["floor_ratio"] = df["floor"] / df["floors_total"]

    if "ceiling_height" in df.columns:
        df["ceiling_height"] = df["ceiling_height"].clip(2.3, 6.0)

    if "build_year" in df.columns:
        df["build_year"] = df["build_year"].round().clip(1800, 2026)
        df["house_age"] = 2026 - df["build_year"]

    return df


def nullify_geo_dependent_columns(df):
    geo_keywords = [
        "distance_to_",
        "nearest_",
        "metro_distance",
        "metro_walk",
    ]

    for col in df.columns:
        if any(key in col.lower() for key in geo_keywords):
            df[col] = np.nan

    for col in [
        "metro",
        "metro_source",
        "address",
        "address_source",
        "house_number",
        "has_metro_near"
    ]:
        if col in df.columns:
            df[col] = np.nan

    if "link" in df.columns:
        df["link"] = "synthetic"

    return df


# =========================
# 3. Генерация синтетики
# =========================

def generate_synthetic_by_geo_cluster(train_df):
    rng = np.random.default_rng(RANDOM_STATE)
    jitter_degree = meters_to_degrees(COORD_JITTER_METERS)

    synthetic_parts = []

    continuous_cols = [
        col for col in [
            "area_total",
            "kitchen_area",
            "ceiling_height"
        ]
        if col in train_df.columns
    ]

    integer_cols = [
        col for col in [
            "rooms",
            "floor",
            "floors_total",
            "build_year"
        ]
        if col in train_df.columns
    ]

    categorical_cols = [
        col for col in [
            "source_group",
            "renovation",
            "walls",
            "floor_position",
            "elevator",
            "yard",
            "parking",
            "playground"
        ]
        if col in train_df.columns
    ]

    for cluster_id in sorted(train_df[CLUSTER_COL].dropna().unique()):
        cluster_df = (
            train_df[train_df[CLUSTER_COL] == cluster_id]
            .copy()
            .reset_index(drop=True)
        )

        n = len(cluster_df)

        if n < MIN_CLUSTER_SIZE:
            continue

        n_synthetic = n * SYNTHETIC_MULTIPLIER

        idx_a = rng.integers(0, n, size=n_synthetic)
        idx_b = rng.integers(0, n, size=n_synthetic)

        parent_a = cluster_df.iloc[idx_a].reset_index(drop=True)
        parent_b = cluster_df.iloc[idx_b].reset_index(drop=True)

        lam = rng.beta(2, 2, size=n_synthetic)

        synth = parent_a.copy()

        # координаты внутри кластера
        synth[LAT_COL] = (
            lam * parent_a[LAT_COL].values
            + (1 - lam) * parent_b[LAT_COL].values
        )

        synth[LON_COL] = (
            lam * parent_a[LON_COL].values
            + (1 - lam) * parent_b[LON_COL].values
        )

        synth[LAT_COL] += rng.normal(
            0,
            jitter_degree,
            size=n_synthetic
        )

        synth[LON_COL] += rng.normal(
            0,
            jitter_degree,
            size=n_synthetic
        )

        # не даём точкам выйти далеко за границы своего кластера
        synth[LAT_COL] = synth[LAT_COL].clip(
            cluster_df[LAT_COL].min() - 2 * jitter_degree,
            cluster_df[LAT_COL].max() + 2 * jitter_degree
        )

        synth[LON_COL] = synth[LON_COL].clip(
            cluster_df[LON_COL].min() - 2 * jitter_degree,
            cluster_df[LON_COL].max() + 2 * jitter_degree
        )

        # непрерывные признаки
        for col in continuous_cols:
            std = cluster_df[col].std()

            noise = 0
            if pd.notna(std) and std > 0:
                noise = rng.normal(
                    0,
                    std * 0.03,
                    size=n_synthetic
                )

            synth[col] = (
                lam * parent_a[col].values
                + (1 - lam) * parent_b[col].values
                + noise
            )

        # дискретные признаки
        for col in integer_cols:
            synth[col] = (
                lam * parent_a[col].values
                + (1 - lam) * parent_b[col].values
            ).round()

        # категориальные признаки берём от одного из родителей
        choose_a = rng.random(n_synthetic) < lam

        for col in categorical_cols:
            synth[col] = np.where(
                choose_a,
                parent_a[col].values,
                parent_b[col].values
            )

        # цена согласованно через price_per_m2
        if {
            PRICE_PER_M2_COL,
            TARGET_COL,
            "area_total"
        }.issubset(synth.columns):

            ppm2_std = cluster_df[PRICE_PER_M2_COL].std()

            ppm2_noise = 0
            if pd.notna(ppm2_std) and ppm2_std > 0:
                ppm2_noise = rng.normal(
                    0,
                    ppm2_std * 0.03,
                    size=n_synthetic
                )

            synth[PRICE_PER_M2_COL] = (
                lam * parent_a[PRICE_PER_M2_COL].values
                + (1 - lam) * parent_b[PRICE_PER_M2_COL].values
                + ppm2_noise
            )

            synth[PRICE_PER_M2_COL] = synth[PRICE_PER_M2_COL].clip(
                cluster_df[PRICE_PER_M2_COL].quantile(0.01),
                cluster_df[PRICE_PER_M2_COL].quantile(0.99)
            )

            synth[TARGET_COL] = (
                synth[PRICE_PER_M2_COL] *
                synth["area_total"]
            )

        synth = clip_real_estate_constraints(synth)

        if {
            PRICE_PER_M2_COL,
            TARGET_COL,
            "area_total"
        }.issubset(synth.columns):
            synth[PRICE_PER_M2_COL] = (
                synth[TARGET_COL] / synth["area_total"]
            )

        # геозависимые признаки обнуляем — потом пересчитаем
        synth = nullify_geo_dependent_columns(synth)

        synth["is_synthetic"] = 1
        synth["generation_method"] = "geo_cluster_interpolation"
        synth["parent_geo_cluster"] = cluster_id

        synthetic_parts.append(synth)

    return pd.concat(synthetic_parts, ignore_index=True)


# =========================
# 4. Запуск
# =========================

df = pd.read_csv(INPUT_FILE)

df["kitchen_area"] = (
    df.groupby("rooms")["kitchen_area"]
    .transform(lambda x: x.fillna(x.median()))
)

df["kitchen_area"] = df["kitchen_area"].fillna(
    df["kitchen_area"].median()
)

train_df, test_df = train_test_split(
    df,
    test_size=TEST_SIZE,
    random_state=RANDOM_STATE
)

train_df = train_df.copy().reset_index(drop=True)
test_df = test_df.copy().reset_index(drop=True)

synthetic_df = generate_synthetic_by_geo_cluster(train_df)

train_original_marked = train_df.copy()
train_original_marked["is_synthetic"] = 0
train_original_marked["generation_method"] = "original"
train_original_marked["parent_geo_cluster"] = train_original_marked[CLUSTER_COL]

train_augmented = pd.concat(
    [train_original_marked, synthetic_df],
    ignore_index=True
)


# =========================
# 5. Сохранение
# =========================

synthetic_df.to_csv(
    "synthetic_objects_need_geo_recalculation.csv",
    index=False
)

train_augmented.to_csv(
    "train_augmented_need_geo_recalculation.csv",
    index=False
)

train_original_marked.to_csv(
    "train_original_before_synthetic.csv",
    index=False
)

test_df.to_csv(
    "test_original_not_augmented.csv",
    index=False
)

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

cluster_report.to_csv(
    "geo_cluster_generation_report.csv",
    index=False
)


# =========================
# 6. Итог
# =========================

print("Готово.")
print("Original dataset:", df.shape)
print("Train original:", train_original_marked.shape)
print("Synthetic only:", synthetic_df.shape)
print("Train augmented:", train_augmented.shape)
print("Test untouched:", test_df.shape)

print("\nСохранены файлы:")
print("synthetic_objects_need_geo_recalculation.csv")
print("train_augmented_need_geo_recalculation.csv")
print("train_original_before_synthetic.csv")
print("test_original_not_augmented.csv")
print("geo_cluster_generation_report.csv")