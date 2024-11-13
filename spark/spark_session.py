from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Inisialisasi Spark session
spark = SparkSession.builder \
    .appName("IncrementalTransactionClustering") \
    .getOrCreate()

# Mendefinisikan skema data
schema = StructType([
    StructField("transID", StringType(), True),
    StructField("payCardID", StringType(), True),
    StructField("payCardBank", StringType(), True),
    StructField("payCardName", StringType(), True),
    StructField("payCardSex", StringType(), True),
    StructField("payCardBirthDate", StringType(), True),
    StructField("corridorID", StringType(), True),
    StructField("corridorName", StringType(), True),
    StructField("direction", StringType(), True),
    StructField("tapInStops", StringType(), True),
    StructField("tapInStopsName", StringType(), True),
    StructField("tapInStopsLat", FloatType(), True),
    StructField("tapInStopsLon", FloatType(), True),
    StructField("stopStartSeq", StringType(), True),
    StructField("tapInTime", StringType(), True),
    StructField("tapOutStops", StringType(), True),
    StructField("tapOutStopsName", StringType(), True),
    StructField("tapOutStopsLat", FloatType(), True),
    StructField("tapOutStopsLon", FloatType(), True),
    StructField("stopEndSeq", StringType(), True),
    StructField("tapOutTime", StringType(), True),
    StructField("payAmount", FloatType(), True)
])

# Membaca data batch
batch_files = ["batch_1.csv", "batch_2.csv", "batch_3.csv"]

# Variabel untuk menyimpan data gabungan kumulatif
combined_data = None

# Loop untuk membuat tiga model dengan data kumulatif bertahap
for i, batch_file in enumerate(batch_files, 1):
    print(f"\n=== Model {i}: Menggunakan {i} batch ===")
    
    # Membaca batch berikutnya
    batch_data = spark.read.csv(batch_file, header=True, schema=schema)
    combined_data = batch_data if combined_data is None else combined_data.union(batch_data)
    
    # Menyusun fitur
    featureCols = ["payAmount", "tapInStopsLat", "tapInStopsLon", "tapOutStopsLat", "tapOutStopsLon"]
    assembler = VectorAssembler(inputCols=featureCols, outputCol="features", handleInvalid="skip")
    assembled_df = assembler.transform(combined_data)
    
    # Skala fitur
    standardScaler = StandardScaler(inputCol="features", outputCol="features_scaled")
    scaled_df = standardScaler.fit(assembled_df).transform(assembled_df)
    
    # Mencari k terbaik
    best_k = 2
    best_silhouette = -1
    for k in range(2, 10):
        kmeans = KMeans(featuresCol='features_scaled', k=k, seed=23)
        kmeans_model = kmeans.fit(scaled_df)
        kmeans_predictions = kmeans_model.transform(scaled_df)
        
        evaluator = ClusteringEvaluator(featuresCol='features_scaled', metricName='silhouette', distanceMeasure='squaredEuclidean')
        silhouette_score = evaluator.evaluate(kmeans_predictions)
        print(f"Silhouette Score for k={k}: {silhouette_score}")
        
        if silhouette_score > best_silhouette:
            best_k = k
            best_silhouette = silhouette_score
            best_model = kmeans_model
            best_predictions = kmeans_predictions
    
    print(f"Best Silhouette Score for Model {i} with k={best_k}: {best_silhouette}")
    
    # Menampilkan hasil clustering terbaik
    best_predictions.select("transID", "payCardBank", "payAmount", "prediction").show(10)
    
    # Menyimpan hasil clustering terbaik untuk model ini
    output_path = f"clustering_results_model_{i}_best_k_{best_k}"
    best_predictions.select("transID", "payCardBank", "payAmount", "prediction") \
        .coalesce(1) \
        .write.csv(output_path, header=True, mode="overwrite")
    print(f"Hasil clustering terbaik untuk Model {i} disimpan di {output_path}")

# Stop Spark session
spark.stop()
