from pyspark.sql import SparkSession

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("OpenFoodFacts Data Cleansing") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/openfoodfacts.products") \
    .getOrCreate()

# Charger les données depuis un fichier CSV
data = spark.read.csv("CSV\openfoodfacts.csv", header=True, inferSchema=True)

# Afficher un échantillon
data.show(5)

# Afficher le schéma
data.printSchema()

# Statistiques descriptives
data.describe().show()

# Nombre de doublons
print(f"Nombre de doublons : {data.count() - data.distinct().count()}")

# Proportion de valeurs nulles par colonne
from pyspark.sql.functions import col, sum as spark_sum

null_counts = data.select([
    spark_sum(col(c).isNull().cast("int")).alias(c) for c in data.columns
])
null_counts.show()
