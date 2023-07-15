from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, element_at,col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType

data_schema = StructType([
    StructField("data", ArrayType(StructType([
        StructField("clouds", IntegerType()),
        StructField("dew_point", DoubleType()),
        StructField("dt", IntegerType()),
        StructField("feels_like", DoubleType()),
        StructField("humidity", IntegerType()),
        StructField("pressure", IntegerType()),
        StructField("sunrise", IntegerType()),
        StructField("sunset", IntegerType()),
        StructField("temp", DoubleType()),
        StructField("uvi", DoubleType()),
        StructField("visibility", IntegerType()),
        StructField("weather", ArrayType(StructType([
            StructField("description", StringType()),
            StructField("icon", StringType()),
            StructField("id", IntegerType()),
            StructField("main", StringType())
        ]))),
        StructField("wind_deg", IntegerType()),
        StructField("wind_gust", DoubleType()),
        StructField("wind_speed", DoubleType())
    ]))),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("timezone", StringType()),
    StructField("timezone_offset", IntegerType())
])

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "openweathermap"

spark = SparkSession.builder.appName("read_test_stream").getOrCreate()

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()
    
df = df.selectExpr("CAST(value AS STRING)")

# Apply the schema to the JSON data
df = df.withColumn("data", from_json("value", data_schema))

# Explode the "data" array column
df = df.withColumn("exploded_data", explode("data.data"))

# Select the desired columns from the exploded data
df = df.select("exploded_data.clouds", "exploded_data.dew_point", "exploded_data.dt", "exploded_data.feels_like",
               "exploded_data.humidity", "exploded_data.pressure", "exploded_data.sunrise", "exploded_data.sunset",
               "exploded_data.temp", "exploded_data.uvi", "exploded_data.visibility",
               element_at("exploded_data.weather.description", 1).alias("description"), 
               element_at("exploded_data.weather.icon", 1).alias("icon"),
               element_at("exploded_data.weather.id", 1).alias("id"),
               element_at("exploded_data.weather.main", 1).alias("main"),
               "data.lat", "data.lon", "data.timezone", "data.timezone_offset")

# TODO faire le preprocessing ici
df = df.withColumn("timestamp", col("dt").cast(TimestampType()))

#df = df.withColumn("temperature_celsius", col("temp") / 10)

#df = df.withColumn("humidity_percentage", col("humidity") / 100)

# Drop the original columns after preprocessing
#df = df.drop("dt", "temp", "humidity")
df = df.drop("dt", "clouds")

# TODO faire une prédiction ici

# TODO récuperer la prédiction et écrire sur la base de données MongoDB

df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
