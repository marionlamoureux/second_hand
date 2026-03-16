# Silver: cleaned and validated Kiabi listings — MATERIALIZED VIEW (batch read from bronze).
# Changed from STREAMING TABLE to avoid DLT streaming checkpoint initialization deadlock
# that caused silver to hang indefinitely in STARTING state after bronze completed.
# For ~21k rows/run, batch recomputation is fast enough.
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

# City coordinates — ordered so longer/more-specific names are tried first.
# Covers top ~150 French cities by population plus common Kiabi-store cities.
# Accent-insensitive matching is handled in the UDF via unicodedata.
FRANCE_CITY_COORDS = [
    # Regions (fallback if no city matched)
    ("ile-de-france", 48.8497, 2.6370),
    ("hauts-de-france", 50.4801, 2.7937),
    ("auvergne-rhone-alpes", 45.4471, 4.3857),
    ("nouvelle-aquitaine", 44.7002, -0.2995),
    ("pays de la loire", 47.7633, -0.3299),
    ("grand est", 48.6998, 6.1878),
    ("occitanie", 43.8927, 3.2828),
    ("normandie", 49.1193, 0.5503),
    ("bretagne", 48.2020, -2.9326),
    ("provence", 43.9352, 5.2210),
    # Île-de-France suburbs (before "paris" to avoid wrong match)
    ("boulogne-billancourt", 48.8353, 2.2400),
    ("saint-maur-des-fosses", 48.7975, 2.4794),
    ("rueil-malmaison", 48.8762, 2.1886),
    ("asnieres-sur-seine", 48.9123, 2.2837),
    ("argenteuil", 48.9472, 2.2463),
    ("montreuil", 48.8636, 2.4482),
    ("versailles", 48.8014, 2.1301),
    ("nanterre", 48.8926, 2.2069),
    ("courbevoie", 48.8973, 2.2518),
    ("colombes", 48.9231, 2.2549),
    ("vitry-sur-seine", 48.7875, 2.4015),
    ("creteil", 48.7773, 2.4559),
    ("champigny-sur-marne", 48.8174, 2.5153),
    ("levallois-perret", 48.8942, 2.2877),
    ("clichy", 48.9043, 2.3053),
    ("ivry-sur-seine", 48.8136, 2.3836),
    ("aulnay-sous-bois", 48.9290, 2.4951),
    ("saint-denis", 48.9362, 2.3574),
    ("evry", 48.6323, 2.4277),
    ("cergy", 49.0360, 2.0640),
    ("massy", 48.7267, 2.2711),
    ("meaux", 48.9597, 2.8783),
    ("melun", 48.5399, 2.6575),
    ("corbeil-essonnes", 48.6148, 2.4784),
    ("bobigny", 48.9094, 2.4399),
    ("vincennes", 48.8475, 2.4392),
    ("fontenay-sous-bois", 48.8528, 2.4765),
    ("saint-germain-en-laye", 48.8984, 2.0945),
    ("poissy", 48.9295, 2.0466),
    ("sartrouville", 48.9381, 2.1588),
    ("paris", 48.8566, 2.3522),
    # Hauts-de-France
    ("villeneuve-d-ascq", 50.6149, 3.1385),
    ("roubaix", 50.6942, 3.1746),
    ("tourcoing", 50.7235, 3.1612),
    ("dunkerque", 51.0343, 2.3773),
    ("valenciennes", 50.3579, 3.5237),
    ("calais", 50.9513, 1.8587),
    ("lens", 50.4330, 2.8290),
    ("douai", 50.3698, 3.0800),
    ("boulogne-sur-mer", 50.7267, 1.6135),
    ("arras", 50.2922, 2.7781),
    ("bethune", 50.5330, 2.6410),
    ("maubeuge", 50.2769, 3.9737),
    ("amiens", 49.8940, 2.2958),
    ("compiegne", 49.4181, 2.8233),
    ("laon", 49.5645, 3.6245),
    ("soissons", 49.3815, 3.3235),
    ("beauvais", 49.4294, 2.0810),
    ("lille", 50.6292, 3.0573),
    # Grand Est
    ("strasbourg", 48.5734, 7.7521),
    ("mulhouse", 47.7508, 7.3359),
    ("metz", 49.1193, 6.1757),
    ("nancy", 48.6921, 6.1844),
    ("reims", 49.2583, 4.0317),
    ("troyes", 48.2974, 4.0744),
    ("thionville", 49.3600, 6.1680),
    ("colmar", 48.0799, 7.3589),
    ("epinal", 48.1667, 6.4500),
    ("charleville-mezieres", 49.7734, 4.7215),
    ("chalons-en-champagne", 48.9574, 4.3652),
    ("saint-dizier", 48.6361, 4.9482),
    ("haguenau", 48.8160, 7.7913),
    ("sarreguemines", 49.1112, 7.0680),
    ("forbach", 49.1893, 6.8993),
    # Auvergne-Rhône-Alpes
    ("villeurbanne", 45.7719, 4.8862),
    ("venissieux", 45.6960, 4.8875),
    ("saint-priest", 45.6953, 4.9387),
    ("vaulx-en-velin", 45.7760, 4.9198),
    ("caluire-et-cuire", 45.7967, 4.8530),
    ("annecy", 45.8992, 6.1294),
    ("chambery", 45.5646, 5.9178),
    ("clermont-ferrand", 45.7772, 3.0870),
    ("saint-etienne", 45.4397, 4.3872),
    ("roanne", 46.0360, 4.0700),
    ("valence", 44.9333, 4.8917),
    ("bourg-en-bresse", 46.2050, 5.2250),
    ("grenoble", 45.1885, 5.7245),
    ("thonon-les-bains", 46.3693, 6.4766),
    ("annemasse", 46.1940, 6.2351),
    ("albertville", 45.6766, 6.3909),
    ("montelimar", 44.5574, 4.7524),
    ("romans-sur-isere", 45.0479, 5.0555),
    ("aurillac", 44.9248, 2.4411),
    ("riom", 45.8947, 3.1131),
    ("moulins", 46.5641, 3.3341),
    ("vichy", 46.1278, 3.4264),
    ("issoire", 45.5437, 3.2487),
    ("lyon", 45.7640, 4.8357),
    # Nouvelle-Aquitaine
    ("merignac", 44.8333, -0.6432),
    ("pessac", 44.8067, -0.6315),
    ("bordeaux", 44.8378, -0.5792),
    ("bayonne", 43.4929, -1.4748),
    ("angouleme", 45.6500, 0.1562),
    ("poitiers", 46.5802, 0.3404),
    ("la rochelle", 46.1591, -1.1520),
    ("niort", 46.3248, -0.4651),
    ("limoges", 45.8350, 1.2632),
    ("perigueux", 45.1840, 0.7130),
    ("agen", 44.2009, 0.6203),
    ("pau", 43.2951, -0.3708),
    ("biarritz", 43.4832, -1.5586),
    ("brive-la-gaillarde", 45.1565, 1.5323),
    ("rochefort", 45.9418, -0.9587),
    ("saintes", 45.7456, -0.6328),
    ("mont-de-marsan", 43.8929, -0.4994),
    ("dax", 43.7101, -1.0480),
    ("arcachon", 44.6586, -1.1683),
    ("chateauroux", 46.8121, 1.6906),
    ("gueret", 46.1680, 1.8695),
    ("tulle", 45.2673, 1.7724),
    # Pays de la Loire
    ("saint-nazaire", 47.2736, -2.2137),
    ("le mans", 48.0061, 0.1996),
    ("laval", 48.0702, -0.7718),
    ("angers", 47.4784, -0.5632),
    ("cholet", 47.0585, -0.8794),
    ("la roche-sur-yon", 46.6705, -1.4264),
    ("nantes", 47.2184, -1.5536),
    ("saumur", 47.2597, -0.0765),
    # Bretagne
    ("quimper", 47.9963, -4.0976),
    ("brest", 48.3905, -4.4860),
    ("lorient", 47.7492, -3.3659),
    ("vannes", 47.6559, -2.7600),
    ("saint-brieuc", 48.5137, -2.7603),
    ("rennes", 48.1173, -1.6778),
    ("lannion", 48.7322, -3.4575),
    ("saint-malo", 48.6493, -2.0257),
    # Normandie
    ("le havre", 49.4944, 0.1079),
    ("rouen", 49.4432, 1.0993),
    ("caen", 49.1829, -0.3707),
    ("cherbourg", 49.6337, -1.6169),
    ("evreux", 49.0208, 1.1515),
    ("alencon", 48.4286, 0.0911),
    ("dieppe", 49.9263, 1.0830),
    # Occitanie
    ("montpellier", 43.6108, 3.8767),
    ("toulouse", 43.6047, 1.4442),
    ("nimes", 43.8367, 4.3601),
    ("beziers", 43.3442, 3.2160),
    ("perpignan", 42.6987, 2.8956),
    ("albi", 43.9267, 2.1475),
    ("carcassonne", 43.2120, 2.3500),
    ("tarbes", 43.2327, 0.0760),
    ("castres", 43.6065, 2.2462),
    ("narbonne", 43.1839, 3.0042),
    ("sete", 43.4040, 3.6899),
    ("montauban", 44.0158, 1.3551),
    ("rodez", 44.3511, 2.5752),
    ("auch", 43.6478, 0.5850),
    ("cahors", 44.4508, 1.4417),
    ("foix", 42.9653, 1.6060),
    # PACA
    ("aix-en-provence", 43.5297, 5.4474),
    ("marseille", 43.2965, 5.3698),
    ("nice", 43.7102, 7.2620),
    ("toulon", 43.1242, 5.9280),
    ("antibes", 43.5808, 7.1280),
    ("cannes", 43.5513, 7.0128),
    ("grasse", 43.6583, 6.9222),
    ("frejus", 43.4332, 6.7378),
    ("draguignan", 43.5350, 6.4668),
    ("la seyne-sur-mer", 43.1002, 5.8829),
    ("hyeres", 43.1198, 6.1282),
    ("avignon", 43.9493, 4.8055),
    ("gap", 44.5596, 6.0793),
    ("arles", 43.6764, 4.6277),
    ("martigues", 43.4049, 5.0529),
    ("salon-de-provence", 43.6395, 5.0984),
    ("aubagne", 43.2948, 5.5660),
    ("menton", 43.7764, 7.4994),
    ("saint-raphael", 43.4253, 6.7679),
    # Centre-Val de Loire
    ("orleans", 47.9029, 1.9093),
    ("tours", 47.3939, 0.6880),
    ("bourges", 47.0806, 2.3969),
    ("blois", 47.5864, 1.4191),
    ("chartres", 48.4486, 1.4878),
    ("vendome", 47.7917, 1.0647),
    # Bourgogne-Franche-Comté
    ("besancon", 47.2431, 6.0227),
    ("dijon", 47.3220, 5.0415),
    ("chalon-sur-saone", 46.7812, 4.8526),
    ("belfort", 47.6292, 6.8638),
    ("montbeliard", 47.5100, 6.8041),
    ("macon", 46.3077, 4.8315),
    ("auxerre", 47.7986, 3.5676),
    ("nevers", 46.9896, 3.1575),
    # Corsica
    ("ajaccio", 41.9190, 8.7386),
    ("bastia", 42.6976, 9.4506),
    # DOM-TOM
    ("cayenne", 4.9224, -52.3326),
    ("fort-de-france", 14.6415, -61.0242),
    ("pointe-a-pitre", 16.2417, -61.5330),
]


def _build_postal_lkp():
    """Read the official French postal code file and return a dict of code_postal → (lat, lon).
    Multiple communes can share the same postal code — we take the centroid (avg lat/lon).
    Path is derived from the pipeline's landing_path config.
    """
    landing_base = spark.conf.get("landing_path")
    csv_path = f"{landing_base}/enrichment_files/base-officielle-codes-postaux.csv"
    return {
        row["code_postal"]: (row["avg_lat"], row["avg_lon"])
        for row in (
            spark.read.csv(csv_path, header=True, inferSchema=False)
            .select(
                F.col("code_postal"),
                F.col("latitude").cast("double"),
                F.col("longitude").cast("double"),
            )
            .filter(F.col("code_postal").isNotNull() & F.col("latitude").isNotNull())
            .groupBy("code_postal")
            .agg(
                F.avg("latitude").alias("avg_lat"),
                F.avg("longitude").alias("avg_lon"),
            )
            .collect()
        )
    }


def _lat_lon_from_location(loc_col, postal_lkp):
    """Derive lat/lon from location string using a UDF.
    Strategy (in order):
      1. Extract 5-digit postal code → precise centroid from the official postal code file.
         LeBonCoin always formats location as "City (59100)", covering the vast majority.
      2. Accent-normalised city name match against FRANCE_CITY_COORDS (Vinted / eBay fallback).
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StructType, StructField, DoubleType

    _postal = postal_lkp
    _cities = FRANCE_CITY_COORDS

    @udf(returnType=StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
    ]))
    def _coords_udf(loc):
        if not loc:
            return None
        import re
        import unicodedata

        # 1. Postal code → precise coords (covers LeBonCoin "City (59100)" format)
        m = re.search(r"\b(\d{5})\b", loc)
        if m:
            coords = _postal.get(m.group(1))
            if coords:
                return coords

        # 2. City name match (Vinted / eBay — no postal code)
        def _norm(s):
            return unicodedata.normalize("NFD", s.lower()).encode("ascii", "ignore").decode("ascii")

        loc_norm = _norm(loc)
        for name, lat, lon in _cities:
            if name in loc_norm:
                return (lat, lon)

        return None

    coords = _coords_udf(F.coalesce(loc_col, F.lit("")))
    return coords.getField("lat"), coords.getField("lon")

LISTING_SOURCES = ("leboncoin", "vinted", "ebay", "label_emmaus", "vestiaire")

@dp.materialized_view(name="silver_listings", comment="Cleaned Kiabi listings with normalized types")
def silver_listings():
    # Only listing rows (exclude competition rows that have brand/marketplace instead of source)
    base = (
        spark.read.table("bronze_listings")
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
    postal_lkp = _build_postal_lkp()
    lat_col, lon_col = _lat_lon_from_location(F.col("location"), postal_lkp)
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
