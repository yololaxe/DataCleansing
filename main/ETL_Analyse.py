from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, count, row_number
from pyspark.sql.window import Window
import os

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

PYTHON_PATH = "C:\\Program Files\\Python311\\python.exe"
if not os.path.exists(PYTHON_PATH):
    raise FileNotFoundError(f"Python introuvable à l'emplacement spécifié : {PYTHON_PATH}")

# Initialisation de Spark
spark = SparkSession.builder \
    .appName("OpenFoodFacts Data Analysis") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Charger les données nettoyées
input_cleaned_path = "output/cleaned_data_single.csv"
data_cleaned = spark.read.csv(input_cleaned_path, header=True, inferSchema=True)

# Inspection initiale
print("=== Exemple de données nettoyées ===")
data_cleaned.show(10, truncate=False)

# Produits les plus caloriques par catégorie
data_cleaned = data_cleaned.withColumn(
    "calories", 4 * (col("sugars_100g") + col("proteins_100g")) + 9 * col("fat_100g"))
window_spec = Window.partitionBy("categories").orderBy(col("calories").desc())
most_caloric = data_cleaned.filter(col("calories").isNotNull()) \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .select("categories", "product_name", "calories")
print("=== Produits les plus caloriques par catégorie ===")
most_caloric.show(10, truncate=False)

# Tendances de production par marque
trends_by_brand = data_cleaned.filter(col("brands").isNotNull()) \
    .groupBy("brands") \
    .agg(count("*").alias("product_count")) \
    .orderBy(col("product_count").desc())
print("=== Tendances de production par marque ===")
trends_by_brand.show(10, truncate=False)

# Statistiques descriptives par catégorie
category_stats = data_cleaned.groupBy("categories").agg(
    avg("sodium_100g").alias("avg_sodium"),
    avg("sugars_100g").alias("avg_sugars"),
    avg("fat_100g").alias("avg_fat"),
    avg("proteins_100g").alias("avg_proteins"),
    count("*").alias("product_count")
).filter(col("product_count") >= 50) \
 .orderBy(col("avg_sodium").desc())
print("=== Statistiques descriptives par catégorie ===")
category_stats.show(10, truncate=False)

# Arrêter Spark
spark.stop()