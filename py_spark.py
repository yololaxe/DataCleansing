from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
import json

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("OpenFoodFacts Data Cleansing") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.local.dir", "G:/proj/DataCleansing/spark-temp") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/openfoodfacts.products") \
    .getOrCreate()


# Charger les données depuis un fichier CSV
data = spark.read.csv("CSV\openfoodfacts.csv", header=True, inferSchema=True, sep="\t")

data = data.limit(10000)  # Charger uniquement 1000 lignes pour les tests

raw_json = data.toJSON().collect()

with open("data.json", "w", encoding="utf-8") as json_file:
    json.dump(raw_json, json_file, indent=4, ensure_ascii=False)

# Remplacer les espaces et caractères spéciaux dans les noms de colonnes
# Nettoyer les noms de colonnes
data = data.toDF(*[c.strip().replace(" ", "_").replace(".", "_") for c in data.columns])

# Vérifiez les colonnes disponibles
print("Colonnes disponibles :", data.columns)

# Suppression des lignes avec des valeurs manquantes dans certaines colonnes

important_columns = ["product_name", "categories", "brands"]
# Supprimer les lignes où une ou plusieurs colonnes importantes sont nulles
cleaned_data = data.dropna(subset=important_columns)

# Suppression des doublons basés sur 'code'
cleaned_data = cleaned_data.dropDuplicates(subset=["code"])

filtered_data = cleaned_data.filter(
    (col("product_name").isNotNull()) &               # Vérifie que le nom n'est pas NULL
    (trim(col("product_name")) != "") &              # Vérifie que le nom n'est pas vide
    (~col("states").like("%discontinued%"))          # Exclut les produits marqués comme "discontinued"
)

simplified_data = filtered_data.select(
    "code", "product_name", "brands", "categories", "countries"
).limit(150)


simplified_data.show(truncate=False)

# Afficher le résultat
# filtered_data.show()

# Afficher un échantillon
# cleaned_data.show(10)

json_data = simplified_data.toJSON().collect()

with open("simplified_data.json", "w", encoding="utf-8") as json_file:
    json.dump(json_data, json_file, indent=4, ensure_ascii=False)

# Résumé final
print("=== Résumé final ===")
print(f"Nombre de lignes initiales : {data.count()}")
print(f"Nombre de lignes après nettoyage : {cleaned_data.count()}")
print(f"Nombre de lignes après filtrage : {filtered_data.count()}")
print(f"Nombre de lignes exportées dans 'simplified_data.json' : {simplified_data.count()}")

# Arrêter la session Spark
spark.stop()