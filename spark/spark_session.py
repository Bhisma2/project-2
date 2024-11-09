from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder \
    .appName("KafkaBatchProcessing") \
    .getOrCreate()

batch_data = spark.read.json("../kafka/limited_batch_data.json")
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("TransactionClusteringApplication") \
    .getOrCreate()

# Definisikan schema untuk file CSV
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
    StructField("tapInStopsLat", StringType(), True),
    StructField("tapInStopsLon", StringType(), True),
    StructField("stopStartSeq", StringType(), True),
    StructField("tapInTime", StringType(), True),
    StructField("tapOutStops", StringType(), True),
    StructField("tapOutStopsName", StringType(), True),
    StructField("tapOutStopsLat", StringType(), True),
    StructField("tapOutStopsLon", StringType(), True),
    StructField("stopEndSeq", StringType(), True),
    StructField("tapOutTime", StringType(), True),
    StructField("payAmount", FloatType(), True)
])

# Membaca batch data dari file CSV dengan schema
batch_data = spark.read.csv("limited_batch_data.csv", header=True, schema=schema)

# Tampilkan beberapa baris data untuk verifikasi
batch_data.show(5)

# Pilih kolom numerik yang relevan untuk clustering
featureCols = ["payAmount"]
assembler = VectorAssembler(inputCols=featureCols, outputCol="features", handleInvalid="skip")
assembled_df = assembler.transform(batch_data)

# Standardisasi fitur
standardScaler = StandardScaler(inputCol="features", outputCol="features_scaled")
scaled_df = standardScaler.fit(assembled_df).transform(assembled_df)

# Inisialisasi dan latih model K-Means
k = 2  # Jumlah cluster, bisa disesuaikan
kmeans = KMeans(featuresCol='features_scaled', k=k, seed=23)
kmeans_model = kmeans.fit(scaled_df)

# Prediksi cluster menggunakan K-Means
kmeans_predictions = kmeans_model.transform(scaled_df)

# Evaluasi model K-Means menggunakan Silhouette Score
evaluator = ClusteringEvaluator(featuresCol='features_scaled', metricName='silhouette', distanceMeasure='squaredEuclidean')
silhouette_score = evaluator.evaluate(kmeans_predictions)
print(f"Silhouette Score for K-Means: {silhouette_score}")

# Tampilkan hasil clustering
kmeans_predictions.select("transID", "payAmount", "prediction").show(10)

# Hentikan Spark session setelah selesai
spark.stop()

batch_data.show(5)

features = ["payAmount"]
assembler = VectorAssembler(inputCols=features, outputCol="features")

data = assembler.transform(batch_data).select("features", "payAmount")

train_data, test_data = data.randomSplit([0.8, 0.2])

lr = LinearRegression(labelCol="payAmount", featuresCol="features")
lr_model = lr.fit(train_data)

predictions = lr_model.transform(test_data)
predictions.show(5)

rmse = lr_model.summary.rootMeanSquaredError
print(f"Root Mean Squared Error (RMSE): {rmse}")

spark.stop()
