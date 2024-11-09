from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

spark = SparkSession.builder \
    .appName("TransactionClusteringApplication") \
    .getOrCreate()

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

batch_files = ["batch_1.csv", "batch_2.csv", "batch_3.csv"]

for i, batch_file in enumerate(batch_files, 1):
    print(f"\nMemproses {batch_file}...")

    batch_data = spark.read.csv(batch_file, header=True, schema=schema)

    featureCols = ["payAmount"]
    assembler = VectorAssembler(inputCols=featureCols, outputCol="features", handleInvalid="skip")
    assembled_df = assembler.transform(batch_data)

    standardScaler = StandardScaler(inputCol="features", outputCol="features_scaled")
    scaled_df = standardScaler.fit(assembled_df).transform(assembled_df)

    k = 2
    kmeans = KMeans(featuresCol='features_scaled', k=k, seed=23)
    kmeans_model = kmeans.fit(scaled_df)

    kmeans_predictions = kmeans_model.transform(scaled_df)

    evaluator = ClusteringEvaluator(featuresCol='features_scaled', metricName='silhouette', distanceMeasure='squaredEuclidean')
    silhouette_score = evaluator.evaluate(kmeans_predictions)
    print(f"Silhouette Score for {batch_file}: {silhouette_score}")

    kmeans_predictions.select("transID", "payAmount", "prediction").show(10)

    output_path = f"clustering_results_batch_{i}"
    kmeans_predictions.select("transID", "payAmount", "prediction") \
        .coalesce(1) \
        .write.csv(output_path, header=True, mode="overwrite")
    print(f"Hasil clustering disimpan di {output_path}")

spark.stop()



import shutil
from google.colab import files
import os

main_output_folder = "clustering_results_alls"
os.makedirs(main_output_folder, exist_ok=True)

batch_folders = ["clustering_results_batch_1", "clustering_results_batch_2", "clustering_results_batch_3"]
for batch_folder in batch_folders:
    shutil.move(batch_folder, main_output_folder)

shutil.make_archive("clustering_results", "zip", main_output_folder)

files.download("clustering_results.zip")
