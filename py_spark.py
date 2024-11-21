from pyspark.sql import SparkSession

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("OpenFoodFacts Data Cleansing") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.local.dir", "E:\EPSI\DataCleansing\spark-temp") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/openfoodfacts.products") \
    .getOrCreate()


# Charger les données depuis un fichier CSV
data = spark.read.csv("CSV\openfoodfacts.csv", header=True, inferSchema=True, sep="\t")

data = data.limit(1000)  # Charger uniquement 1000 lignes pour les tests

# Remplacer les espaces et caractères spéciaux dans les noms de colonnes
# Nettoyer les noms de colonnes
data = data.toDF(*[c.strip().replace(" ", "_").replace(".", "_") for c in data.columns])

# Vérifiez les colonnes disponibles
print("Colonnes disponibles :", data.columns)

# Suppression des lignes avec des valeurs manquantes dans certaines colonnes
cleaned_data = data.dropna(subset=["product_name", "categories"])

# Suppression des doublons basés sur 'code'
cleaned_data = cleaned_data.dropDuplicates(subset=["code"])

#cleaned_data.write.csv("CSV\cleaned_data.csv", header=True)

# Afficher un échantillon
cleaned_data.show(10)