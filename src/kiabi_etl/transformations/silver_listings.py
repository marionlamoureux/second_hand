# Silver: cleaned and validated Kiabi listings
# Derive external_id from URL when empty so gold gets one row per listing (Leboncoin: .../3155730669).
# is_never_worn: True when description/title suggest item was never worn (neuf, jamais porté, etc.).
# latitude/longitude: derived from location string by matching French city/region names (same logic as app map).
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Regex: any of these (case-insensitive) in title or description → never worn
NEVER_WORN_PATTERN = (
    r"(?i)(\bneuf\b|\bneuve\b|jamais porté|jamais porte|never worn|neuf avec étiquette|"
    r"jamais utilisé|jamais utilise|sans port\b|avec étiquette)"
)

# Approximate (lat, lon) for French cities/regions — match location string (case-insensitive) to derive coordinates
FRANCE_COORDS = [
    ("île-de-france", 48.8497, 2.6370),
    ("hauts-de-france", 50.4801, 2.7937),
    ("auvergne-rhône-alpes", 45.4471, 4.3857),
    ("nouvelle-aquitaine", 44.7002, -0.2995),
    ("pays de la loire", 47.7633, -0.3299),
    ("saint-étienne", 45.4397, 4.3872),
    ("provence", 43.9352, 5.2210),
    ("grand est", 48.6998, 6.1878),
    ("occitanie", 43.8927, 3.2828),
    ("normandie", 49.1193, 0.5503),
    ("bretagne", 48.2020, -2.9326),
    ("paris", 48.8566, 2.3522),
    ("lyon", 45.7640, 4.8357),
    ("marseille", 43.2965, 5.3698),
    ("lille", 50.6292, 3.0573),
    ("toulouse", 43.6047, 1.4442),
    ("bordeaux", 44.8378, -0.5792),
    ("nantes", 47.2184, -1.5536),
    ("strasbourg", 48.5734, 7.7521),
    ("montpellier", 43.6108, 3.8767),
    ("rennes", 48.1173, -1.6778),
    ("reims", 49.2583, 4.0317),
    ("nice", 43.7102, 7.2620),
    ("angers", 47.4784, -0.5632),
    ("brest", 48.3905, -4.4860),
    ("le mans", 48.0061, 0.1996),
    ("grenoble", 45.1885, 5.7245),
    ("amiens", 49.8940, 2.2958),
    ("toulon", 43.1242, 5.9280),
    ("dijon", 47.3220, 5.0415),
    ("clermont", 45.7772, 3.0870),
    ("caen", 49.1829, -0.3707),
]


def _lat_lon_from_location(loc_col):
    """Build Spark expressions for latitude/longitude from location string (first match in FRANCE_COORDS)."""
    loc_lower = F.lower(F.trim(F.coalesce(loc_col, F.lit(""))))
    lat_col = F.lit(None).cast("double")
    lon_col = F.lit(None).cast("double")
    for name, lat, lon in FRANCE_COORDS:
        lat_col = F.when(loc_lower.contains(name), F.lit(lat)).otherwise(lat_col)
        lon_col = F.when(loc_lower.contains(name), F.lit(lon)).otherwise(lon_col)
    return lat_col, lon_col

LISTING_SOURCES = ("leboncoin", "vinted", "ebay", "label_emmaus")

@dp.table(name="silver_listings", comment="Cleaned Kiabi listings with normalized types")
def silver_listings():
    # Only listing rows (exclude competition rows that have brand/marketplace instead of source)
    base = (
        spark.readStream.table("bronze_listings")
        .filter(F.col("source").isin(*LISTING_SOURCES))
    )
    # When external_id is null or blank, derive from URL so we don't collapse all rows into one in gold
    external_id_raw = F.trim(F.coalesce(F.col("external_id"), F.lit("")))
    url_col = F.coalesce(F.col("url"), F.lit(""))
    derived_id = F.when(
        external_id_raw != "",
        external_id_raw,
    ).when(
        (F.col("source") == "leboncoin") & (url_col != ""),
        # Last numeric path segment: .../ad/vetements/3155730669 or .../3155730669/
        F.regexp_extract(url_col, r".*/([0-9]+)/?(?:\?|$)", 1),
    ).when(
        F.col("source") == "vinted",
        F.regexp_extract(F.col("url"), r"/items/([0-9]+)-", 1),
    ).when(
        F.col("source") == "ebay",
        F.regexp_extract(F.col("url"), r"/itm/([0-9]+)", 1),
    ).when(
        F.col("source") == "label_emmaus",
        F.regexp_extract(F.col("url"), r"-([0-9]+)/?$", 1),
    ).otherwise(external_id_raw)
    lat_col, lon_col = _lat_lon_from_location(F.col("location"))
    base = (
        base
        .withColumn("external_id", derived_id)
        .filter(F.col("external_id").isNotNull())
        .filter(F.trim(F.col("external_id")) != "")
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("title", F.trim(F.coalesce(F.col("title"), F.lit(""))))
        .withColumn("description", F.trim(F.coalesce(F.col("description"), F.lit(""))))
        .withColumn(
            "is_never_worn",
            F.when(
                F.lower(F.concat(F.coalesce(F.col("title"), F.lit("")), F.lit(" "), F.coalesce(F.col("description"), F.lit("")))).rlike(NEVER_WORN_PATTERN),
                True,
            ).otherwise(False),
        )
        .withColumn("primary_image_url", F.coalesce(F.col("primary_image_url"), F.lit("")).cast("string"))
        .withColumn("location", F.trim(F.coalesce(F.col("location"), F.lit("")).cast("string")))
    )
    return (
        base
        .withColumn("latitude", lat_col)
        .withColumn("longitude", lon_col)
        .withColumn("_processed_at", F.current_timestamp())
    )
