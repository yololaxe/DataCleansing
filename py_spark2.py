from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, udf, count, from_json
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, DoubleType
import os

import findspark
findspark.init()

# Configuration de l'environnement Hadoop et Spark
os.environ["HADOOP_HOME"] = "C:\hadoop"
os.environ["PATH"] += os.pathsep + "C:\hadoop\bin"

PYTHON_PATH = "C:\Program Files\Python311\python.exe"
if not os.path.exists(PYTHON_PATH):
    raise FileNotFoundError(f"Python introuvable à l'emplacement spécifié : {PYTHON_PATH}")

# Initialiser une session Spark avec des configurations adaptées
spark = SparkSession.builder \
    .appName("OpenFoodFacts Data Cleansing") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.pyspark.python", PYTHON_PATH) \
    .config("spark.pyspark.driver.python", PYTHON_PATH) \
    .config("spark.local.dir", "E:\EPSI\DataCleansing\spark-temp") \
    .config("spark.sql.parquet.enableVectorizedReader", "false")\
    .config("spark.hadoop.io.nativeio", "false")\
    .getOrCreate()



# Réduire les logs
spark.sparkContext.setLogLevel("ERROR")

# Charger les données depuis un fichier CSV
input_path = "CSV/openfoodfacts.csv" 
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Le fichier {input_path} est introuvable.")



data = spark.read.csv(input_path, header=True, inferSchema=True, sep="\t")
# data = data.limit(1000)

# Afficher toutes les colonnes et leur type
print("Liste des colonnes et types :")
data.printSchema()
# data.columns 

print("Colonnes disponibles dans le dataset :")
print(data.columns)


category_counts = data.groupBy("categories").agg(count("*").alias("count")).orderBy("count", ascending=False)

# Afficher les 10 catégories les plus fréquentes

category_counts.show(10, truncate=False)
#Les importantes
important_columns = [
    "product_name",
    "categories",
    "packaging",
    "brands",
    "nutriscore_score",
    "nutriscore_grade",
    "sodium_100g",
    "sugars_100g",
    "fat_100g",
    "proteins_100g"
]

nutrients_schema = StructType([
    StructField("sodium", DoubleType(), True),
    StructField("sugars", DoubleType(), True),
    StructField("fat", DoubleType(), True),
    StructField("proteins", DoubleType(), True)
])

# Filtrer pour ne garder que les colonnes pertinentes
filtered_data = data.select(*important_columns)

# Afficher un échantillon des données filtrées
print("Échantillon des colonnes sélectionnées :")
filtered_data.show(10, truncate=False)

#Afficher le nombre de ligne
print(f"Nombre de lignes après suppression des doublons : {filtered_data.count()}")

# Étape 2 : Supprimer les lignes avec des valeurs manquantes
filtered_data = filtered_data.dropna(subset=["product_name", "categories"])
print(f"Nombre de lignes après suppression des valeurs manquantes : {filtered_data.count()}")

# Étape 3 : Supprimer les doublons
filtered_data = filtered_data.dropDuplicates(subset=["product_name", "categories"])
print(f"Nombre de lignes après suppression des doublons : {filtered_data.count()}")

# Étape 4 : Analyse des catégories
print("Analyse des catégories :")
filtered_data = filtered_data.groupBy("categories").agg(count("*").alias("count")).orderBy("count", ascending=False)
filtered_data.show(10, truncate=False)



# # Étape 5 : Analyse supplémentaire (exemples)
# # Produits avec un score nutritionnel faible (score <= 3)
# low_score_products = deduplicated_data.filter(col("nutriscore_score") <= 3)
# print("Produits avec un score nutritionnel faible :")
# low_score_products.show(10, truncate=False)

# # Produits riches en sodium (sodium_100g > 1.0)
# high_sodium_products = deduplicated_data.filter(col("sodium_100g") > 1.0)
# print("Produits riches en sodium :")
# high_sodium_products.show(10, truncate=False)

###################Test sur les nutriments pour nnormaliser 
if "nutriments" in filtered_data.columns:
    filtered_data = filtered_data.withColumn("parsed_nutrients", from_json(col("nutriments"), nutrients_schema))
    filtered_data = filtered_data.select(
        "*",
        col("parsed_nutrients.sodium").alias("sodium"),
        col("parsed_nutrients.sugars").alias("sugars"),
        col("parsed_nutrients.fat").alias("fat"),
        col("parsed_nutrients.proteins").alias("proteins")
    )

# Conversion des unités
filtered_data = filtered_data.withColumn("sodium_100g", col("sodium_100g") / 1000)
filtered_data = filtered_data.withColumnRenamed("sodium_100g", "sodium_g_per_100g")

# Suppression des colonnes inutiles
filtered_data = filtered_data.drop("categories_tags", "nutriments")

# Afficher un aperçu des données normalisées
filtered_data.show(10, truncate=False)

# Étape 6 : Sauvegarder les données nettoyées
filtered_data.write.csv("output/cleaned_data.csv", header=True, mode="overwrite")
print("Les données nettoyées ont été sauvegardées avec succès.")

# Arrêter la session Spark
spark.stop()












# print("Échantillon des données :")
# data.show(10, truncate=False)

# key_columns = ["product_name", "categories", "nutriments", "packaging"]
# filtered_data = data.select([col(c) for c in key_columns if c in data.columns])

# # Afficher un échantillon des colonnes sélectionnées
# filtered_data.show(10, truncate=False)

# # Supprimer les lignes avec des valeurs manquantes dans des colonnes clés
# important_columns = ["product_name", "categories", "nutriments", "packaging"]

# data_cleaned = data.dropna(subset=important_columns)

# # Afficher le nombre de lignes après suppression
# print(f"Nombre de lignes après suppression des valeurs manquantes : {data_cleaned.count()}")


# data_cleaned = data_cleaned.filter((col("sodium") >= 0) & (col("sodium") <= 1000))

# # Vérifiez les données après suppression des valeurs aberrantes
# data_cleaned.show(10, truncate=False)

# data_cleaned = data_cleaned.dropDuplicates(subset=["product_name", "code"])

# # Afficher le nombre de lignes après suppression des doublons
# print(f"Nombre de lignes après suppression des doublons : {data_cleaned.count()}")

# data_cleaned.write.parquet("output/cleaned_data.parquet", mode="overwrite")

# # Nettoyage des noms des colonnes (enlever espaces et caractères spéciaux)
# data = data.toDF(*[c.strip().replace(" ", "_").replace(".", "_") for c in data.columns])

# # Fonction pour nettoyer et limiter la longueur des catégories
# def sanitize_category(category):
#     if category:
#         return ''.join(e for e in category if e.isalnum() or e in ['-', '_', ' '])[:100]  # Limite à 100 caractères
#     return "Unknown"

# sanitize_category_udf = udf(sanitize_category, StringType())

# # Ajouter une colonne "year" à partir de "created_datetime" si elle existe
# if "created_datetime" in data.columns:
#     data = data.withColumn("year", year(col("created_datetime")))

# # Vérification des colonnes importantes avant le nettoyage
# important_columns = ["product_name", "categories", "brands"]
# missing_columns = [col for col in important_columns if col not in data.columns]
# if missing_columns:
#     raise ValueError(f"Les colonnes suivantes sont manquantes dans le dataset : {', '.join(missing_columns)}")

# # Suppression des lignes avec des valeurs manquantes et des doublons
# cleaned_data = data.dropna(subset=important_columns).dropDuplicates(subset=["code"])

# # Appliquer le nettoyage sur les catégories
# cleaned_data = cleaned_data.withColumn("categories", sanitize_category_udf(col("categories")))

# # Sauvegarder les données en format Parquet avec compression Snappy
# output_path = "output/cleaned_data_parquet"
# os.makedirs(output_path, exist_ok=True)

# cleaned_data.write \
#     .partitionBy("categories", "year") \
#     .parquet(output_path, mode="overwrite", compression="snappy")

# print(f"Les données nettoyées ont été sauvegardées avec succès dans {output_path}.")

# # Arrêter la session Spark
# spark.stop()

