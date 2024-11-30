from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import StructType, StructField, DoubleType
import os

# Importer findspark pour initialiser Spark
import findspark
findspark.init()

# ============================
# Configuration de Spark et Hadoop
# ============================

# Définir les chemins Hadoop et Python
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

PYTHON_PATH = "C:\\Program Files\\Python311\\python.exe"
if not os.path.exists(PYTHON_PATH):
    raise FileNotFoundError(f"Python introuvable à l'emplacement spécifié : {PYTHON_PATH}")

# ============================
# Initialisation de Spark
# ============================
spark = SparkSession.builder \
    .appName("OpenFoodFacts Data Cleansing") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.pyspark.python", PYTHON_PATH) \
    .config("spark.pyspark.driver.python", PYTHON_PATH) \
    .getOrCreate()

# Charger les données
input_path = "CSV/openfoodfacts.csv"
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Le fichier {input_path} est introuvable.")
data = spark.read.csv(input_path, header=True, inferSchema=True, sep="\t")

# Inspection initiale
print("=== Schéma initial des données ===")
data.printSchema()
print("=== Exemple de données brutes ===")
data.show(10, truncate=False)

# Nettoyage des données
important_columns = ["product_name", "categories", "packaging", "brands",
                     "nutriscore_score", "nutriscore_grade", "sodium_100g",
                     "sugars_100g", "fat_100g", "proteins_100g"]
filtered_data = data.select([c for c in important_columns if c in data.columns])

# Supprimer les valeurs manquantes
filtered_data = filtered_data.dropna(subset=["product_name", "categories"])
print(f"=== Nombre de lignes après suppression des valeurs manquantes : {filtered_data.count()} ===")

# Supprimer les doublons
filtered_data = filtered_data.dropDuplicates(subset=["product_name", "categories"])
print(f"=== Nombre de lignes après suppression des doublons : {filtered_data.count()} ===")

# Compter les occurrences par catégorie
category_counts = filtered_data.groupBy("categories").count().orderBy(col("count").desc())
print("=== Nombre d'articles par catégorie ===")
category_counts.show(10, truncate=False)

# Sauvegarder les données nettoyées
output_path = "output/cleaned_data_single.csv"
filtered_data.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
print(f"=== Les données nettoyées ont été sauvegardées dans {output_path} ===")

# Arrêter Spark
spark.stop()