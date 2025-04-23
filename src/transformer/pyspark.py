import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
import logging
import os
import sys
import configparser
from pyspark.sql.utils import AnalysisException, ParseException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("spark_streaming.log")
    ]
)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from config.ini or environment variables."""
    config = configparser.ConfigParser()
    config_file = "config.ini"

    if os.path.exists(config_file):
        config.read(config_file)
        logger.info(f"Loaded configuration from {config_file}")
    else:
        logger.warning(f"Config file {config_file} not found, using environment variables")
        config['DEFAULT'] = {
            'spark_master': os.environ.get('SPARK_MASTER', 'spark://localhost:7077'),
            'kafka_bootstrap_servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'),
            'kafka_topic': os.environ.get('KAFKA_TOPIC', 'cryptocurrency'),
            'checkpoint_location': os.environ.get('CHECKPOINT_LOCATION', './checkpoint'),
            'output_path': os.environ.get('OUTPUT_PATH', './output')
        }

    return {
        'spark_master': config['DEFAULT']['spark_master'],
        'kafka_bootstrap_servers': config['DEFAULT']['kafka_bootstrap_servers'],
        'kafka_topic': config['DEFAULT']['kafka_topic'],
        'checkpoint_location': config['DEFAULT']['checkpoint_location'],
        'output_path': config['DEFAULT']['output_path']
    }

def init_session(master="spark://localhost:7077"):
    """Initialize Spark session with Kafka dependency."""
    try:
        session = (SparkSession.builder
                   .master(master)
                   .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
                   .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                   .appName("CryptoTradeStreaming")
                   .getOrCreate())
        logger.info("Spark session initialized successfully")
        return session
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {str(e)}")
        raise

# Define schema for the JSON messages
schema = StructType([
    StructField("type", StringType(), nullable=True),
    StructField("data", ArrayType(
        StructType([
            StructField("c", StringType(), nullable=True),
            StructField("p", DoubleType(), nullable=True),
            StructField("s", StringType(), nullable=True),
            StructField("t", LongType(), nullable=True),
            StructField("v", DoubleType(), nullable=True)
        ])
    ), nullable=True)
])

def process_streaming_data(spark, config):
    """Process streaming data from Kafka."""
    try:
        # Read streaming data from Kafka
        kafka_df = (spark
                    .readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", config['kafka_bootstrap_servers'])
                    .option("subscribe", config['kafka_topic'])
                    .option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "false")
                    .load())
        logger.info(f"Connected to Kafka topic: {config['kafka_topic']}")

        # Cast key and value as strings
        kafka_df = kafka_df.select(
            col("key").cast("string"),
            col("value").cast("string")
        )

        # Parse JSON messages
        parsed_df = kafka_df.withColumn("parsed_value", from_json(col("value"), schema))

        # Explode the 'data' array to flatten the structure
        exploded_df = parsed_df.select(
            col("parsed_value.type").alias("message_type"),
            explode(col("parsed_value.data")).alias("trade")
        ).select(
            col("message_type"),
            col("trade.c").alias("condition"),
            col("trade.p").alias("price"),
            col("trade.s").alias("symbol"),
            col("trade.t").alias("timestamp"),
            col("trade.v").alias("volume")
        )

        # Write to console
        console_query = (exploded_df
                         .writeStream
                         .outputMode("append")
                         .format("console")
                         .option("truncate", "false")
                         .start())
        logger.info("Started console streaming query")

        # # Write to Parquet files
        # file_query = (exploded_df
        #               .writeStream
        #               .outputMode("append")
        #               .format("parquet")
        #               .option("path", config['output_path'])
        #               .option("checkpointLocation", config['checkpoint_location'])
        #               .partitionBy("message_type")
        #               .start())
        # logger.info(f"Started Parquet file streaming query at {config['output_path']}")

        # Aggregate metrics (e.g., count of records per batch)
        metrics_df = exploded_df.groupBy().agg(count("*").alias("record_count"))
        metrics_query = (metrics_df
                         .writeStream
                         .outputMode("complete")
                         .format("console")
                         .start())
        logger.info("Started metrics streaming query")

        return [console_query, metrics_query]

    except (AnalysisException, ParseException) as e:
        logger.error(f"Error processing streaming data: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

def main():
    """Main function to run the streaming job."""
    try:
        # Load configuration
        config = load_config()

        # Initialize Spark session
        spark = init_session(config['spark_master'])

        # Process streaming data
        queries = process_streaming_data(spark, config)

        # Wait for any query to terminate
        for query in queries:
            query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Received shutdown signal, stopping streaming queries")
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    queries = []  # To store active queries for cleanup
    main()