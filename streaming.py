from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == "__main__":

    # Path to the pre-trained model
    path_to_model = r''

    # Create a local StreamingContext 
    # STEP 1 : creating spark session object

    spark = SparkSession.builder.appName("Kafka Pyspark Streamin Learning").master("local[*]").getOrCreate()

    # Schema for the incoming data
    schema = StructType([StructField("message", StringType())])

    # STEP 2 : reading a data stream from a kafka topic

    # Read the data from kafka
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "twitter").option("startingOffsets", "latest").option("header", "true").load().selectExpr("CAST(value AS STRING) as message")

    # Load the pre-trained model
    nlp_pipeline_model = PipelineModel.load(path_to_model)
    # Make predictions
    prediction = nlp_pipeline_model.transform(df)
    # Select the columns of interest
    prediction = prediction.select(prediction.message, prediction.prediction)

    # Print prediction in console
    prediction.writeStream.format("console").outputMode("update").start().awaitTermination()