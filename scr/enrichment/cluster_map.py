import pandas as pd
import matplotlib.pyplot as plt
import folium

from sklearn.cluster import MiniBatchKMeans
from sklearn.preprocessing import StandardScaler


# =========================
# 1. Загрузка данных
# =========================

df = pd.read_csv("dataset_clean_before_encoding.csv")

lat_col = "latitude"
lon_col = "longitude"
cluster_col = "geo_generation_cluster"

df = df.dropna(subset=[lat_col, lon_col]).copy()

print("Dataset shape:", df.shape)


# =========================
# 2. Кластеризация только по координатам
# =========================

X = df[[lat_col, lon_col]]

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

n_clusters = 25

kmeans = MiniBatchKMeans(
    n_clusters=n_clusters,
    random_state=42,
    batch_size=256,
    n_init=20
)

df[cluster_col] = kmeans.fit_predict(X_scaled)


# =========================
# 3. Сводка по кластерам
# =========================

cluster_summary = (
    df.groupby(cluster_col)
    .agg(
        count=(cluster_col, "size"),
        mean_latitude=(lat_col, "mean"),
        mean_longitude=(lon_col, "mean"),
        median_price=("price", "median"),
        median_price_per_m2=("price_per_m2", "median")
    )
    .reset_index()
)

cluster_summary.to_csv(
    "geo_generation_cluster_summary.csv",
    index=False
)

print(cluster_summary)


# =========================
# 4. PNG карта
# =========================

plt.figure(figsize=(14, 12))

scatter = plt.scatter(
    df[lon_col],
    df[lat_col],
    c=df[cluster_col],
    s=28,
    alpha=0.82,
    cmap="tab20",
    edgecolors="black",
    linewidths=0.2
)

plt.colorbar(scatter, label=cluster_col)

for _, row in cluster_summary.iterrows():
    plt.text(
        row["mean_longitude"],
        row["mean_latitude"],
        str(int(row[cluster_col])),
        fontsize=10,
        weight="bold",
        ha="center",
        va="center",
        bbox=dict(
            facecolor="white",
            alpha=0.75,
            edgecolor="black",
            boxstyle="round,pad=0.25"
        )
    )

plt.title(
    "Spatially Connected Clusters for Synthetic Generation",
    fontsize=18,
    weight="bold"
)

plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.grid(alpha=0.25)

plt.tight_layout()

plt.savefig(
    "geo_generation_clusters.png",
    dpi=400,
    bbox_inches="tight"
)

plt.show()


# =========================
# 5. Интерактивная HTML-карта Москвы
# =========================

map_center = [
    df[lat_col].mean(),
    df[lon_col].mean()
]

m = folium.Map(
    location=map_center,
    zoom_start=10,
    tiles="CartoDB positron"
)

colors = [
    "red", "blue", "green", "purple", "orange",
    "darkred", "lightred", "beige", "darkblue", "darkgreen",
    "cadetblue", "darkpurple", "pink", "lightblue",
    "lightgreen", "gray", "black", "lightgray"
]

for _, row in df.iterrows():
    cluster_id = int(row[cluster_col])
    color = colors[cluster_id % len(colors)]

    popup_text = f"""
    <b>Geo generation cluster:</b> {cluster_id}<br>
    <b>Price:</b> {row.get("price", "NA")}<br>
    <b>Price per m²:</b> {row.get("price_per_m2", "NA")}<br>
    <b>Metro:</b> {row.get("metro", "NA")}<br>
    <b>Address:</b> {row.get("address", "NA")}
    """

    folium.CircleMarker(
        location=[row[lat_col], row[lon_col]],
        radius=3,
        color=color,
        fill=True,
        fill_color=color,
        fill_opacity=0.65,
        popup=folium.Popup(popup_text, max_width=300)
    ).add_to(m)


# =========================
# 6. Центры кластеров
# =========================

for _, row in cluster_summary.iterrows():
    cluster_id = int(row[cluster_col])

    popup_text = f"""
    <b>Cluster center:</b> {cluster_id}<br>
    <b>Objects:</b> {row["count"]}<br>
    <b>Median price:</b> {round(row["median_price"], 0)}<br>
    <b>Median price per m²:</b> {round(row["median_price_per_m2"], 0)}
    """

    folium.Marker(
        location=[
            row["mean_latitude"],
            row["mean_longitude"]
        ],
        popup=folium.Popup(popup_text, max_width=350),
        icon=folium.Icon(
            color="black",
            icon="info-sign"
        )
    ).add_to(m)


m.save("geo_generation_clusters_interactive.html")


# =========================
# 7. Сохранение датасета
# =========================

df.to_csv(
    "dataset_with_geo_generation_clusters.csv",
    index=False
)

print("\nСохранены файлы:")
print("dataset_with_geo_generation_clusters.csv")
print("geo_generation_cluster_summary.csv")
print("geo_generation_clusters.png")
print("geo_generation_clusters_interactive.html")
