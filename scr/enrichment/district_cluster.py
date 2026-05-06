import pandas as pd

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler


# =========================
# 1. Загрузка
# =========================

df = pd.read_csv(
    "dataset_clean_before_encoding.csv"
)


# =========================
# 2. Признаки для spatial clustering
# =========================

cluster_features = [
    "latitude",
    "longitude",
    "distance_to_center_km",
    "distance_to_mkad_km",
    "metro_distance_km"
]

cluster_features = [
    col for col in cluster_features
    if col in df.columns
]


# =========================
# 3. Standardization
# =========================

scaler = StandardScaler()

X = scaler.fit_transform(
    df[cluster_features]
)


# =========================
# 4. KMeans
# =========================

n_clusters = 25

kmeans = KMeans(
    n_clusters=n_clusters,
    random_state=42,
    n_init=10
)

df["district_cluster"] = (
    kmeans.fit_predict(X)
)


# =========================
# 5. Проверка
# =========================

print(
    df["district_cluster"]
    .value_counts()
    .sort_index()
)


# =========================
# 6. Сохранение
# =========================

df.to_csv(
    "dataset_with_spatial_districts.csv",
    index=False
)

print(
    "Файл сохранён:"
    " dataset_with_spatial_districts.csv"
)
