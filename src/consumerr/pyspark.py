import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from dotenv import load_dotenv
import logging
import os
import sys
import time
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
    """Load Spark and Cassandra configuration from .env file."""
    # Load .env file
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)
        logger.info("Loaded configuration from .env file")
    else:
        logger.warning(".env file not found, relying on environment variables")

    # Define required configuration keys
    config_keys = {
        'spark_master': os.environ.get('SPARK_MASTER', 'spark://localhost:7077'),
        'kafka_bootstrap_servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'),
        'kafka_topic': os.environ.get('KAFKA_TOPIC', 'cryptocurrency'),
        'checkpoint_location': os.environ.get('CHECKPOINT_LOCATION', './checkpoint'),
        'output_path': os.environ.get('OUTPUT_PATH', './output'),
        'cassandra_contact_points': os.environ.get('CASSANDRA_CONTACT_POINTS', 'localhost'),
        'cassandra_port': os.environ.get('CASSANDRA_PORT', '9042'),
        'cassandra_keyspace': os.environ.get('CASSANDRA_KEYSPACE', 'crypto_trades'),
        'cassandra_replication_factor': os.environ.get('CASSANDRA_REPLICATION_FACTOR', '3')
    }

    # Validate required keys
    missing_keys = [key for key, value in config_keys.items() if not value]
    if missing_keys:
        logger.error(f"Missing required environment variables: {', '.join(missing_keys)}")
        raise ValueError(f"Missing required environment variables: {', '.join(missing_keys)}")

    # Convert types and process contact points
    return {
        'spark_master': config_keys['spark_master'],
        'kafka_bootstrap_servers': config_keys['kafka_bootstrap_servers'],
        'kafka_topic': config_keys['kafka_topic'],
        'checkpoint_location': config_keys['checkpoint_location'],
        'output_path': config_keys['output_path'],
        'cassandra_contact_points': config_keys['cassandra_contact_points'].split(','),
        'cassandra_port': int(config_keys['cassandra_port']),
        'cassandra_keyspace': config_keys['cassandra_keyspace'],
        'cassandra_replication_factor': int(config_keys['cassandra_replication_factor'])
    }

def init_session(master="spark://localhost:7077", cassandra_config=None):
    """Initialize Spark session with Kafka and Cassandra dependencies."""
    try:
        session = (SparkSession.builder
                   .master(master)
                   .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
                   .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                   .config("spark.cassandra.connection.host", ",".join(cassandra_config['cassandra_contact_points']))
                   .config("spark.cassandra.connection.port", cassandra_config['cassandra_port'])
                   .appName("CryptoTradeStreaming")
                   .getOrCreate())
        logger.info("Spark session initialized successfully")
        return session
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {str(e)}")
        raise

def create_cassandra_schema(config, max_retries=3, retry_delay=5):
    """Create keyspace and table in Cassandra with retry logic."""
    for attempt in range(max_retries):
        try:
            # Connect to Cassandra cluster
            cluster = Cluster(
                contact_points=config['cassandra_contact_points'],
                port=config['cassandra_port']
            )
            session = cluster.connect()
            logger.info(f"Connected to Cassandra cluster at {','.join(config['cassandra_contact_points'])}:{config['cassandra_port']}")

            # Create keyspace if it doesn't exist
            keyspace_cql = f"""
            CREATE KEYSPACE IF NOT EXISTS {config['cassandra_keyspace']}
            WITH replication = {{
                'class': 'SimpleStrategy',
                'replication_factor': {config['cassandra_replication_factor']}
            }}
            """
            session.execute(SimpleStatement(keyspace_cql))
            logger.info(f"Keyspace '{config['cassandra_keyspace']}' created or already exists")

            # Connect to the keyspace
            session.set_keyspace(config['cassandra_keyspace'])

            # Create table if it doesn't exist
            table_cql = """
            CREATE TABLE IF NOT EXISTS trades (
                symbol text,
                timestamp bigint,
                message_type text,
                condition text,
                price double,
                volume double,
                PRIMARY KEY (symbol, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)
            """
            session.execute(SimpleStatement(table_cql))
            logger.info("Table 'trades' created or already exists")

            return True

        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{max_retries} failed to create Cassandra schema: {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise
        finally:
            # Clean up
            if 'session' in locals():
                session.cluster.shutdown()
                logger.info("Cassandra session closed")

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
    """Process streaming data from Kafka and write to Cassandra."""
    try:
        # Read streaming data from Kafka
        kafka_df = (spark
                    .readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", config['kafka_bootstrap_servers'])
                    .option("subscribe", config['kafka_topic'])
                    .option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "false")
                    .option("kafka.session.timeout.ms", "120000")  # Increase timeout to 120s
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
                         .option("checkpointLocation", config['checkpoint_location'] + "/console")
                         .start())
        logger.info("Started console streaming query")

        # Write to Cassandra
        cassandra_query = (exploded_df
                           .writeStream
                           .format("org.apache.spark.sql.cassandra")
                           .option("keyspace", config['cassandra_keyspace'])
                           .option("table", "trades")
                           .option("checkpointLocation", config['checkpoint_location'] + "/cassandra")
                           .outputMode("append")
                           .start())
        logger.info("Started Cassandra streaming query")

        return [console_query, cassandra_query]

    except (AnalysisException, ParseException) as e:
        logger.error(f"Error processing streaming data: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

def main():
    """Main function to run the streaming job."""
    spark = None
    queries = []
    try:
        # Load configuration
        config = load_config()

        # Create Cassandra schema
        logger.info("Creating Cassandra schema")
        create_cassandra_schema(config)
        logger.info("Cassandra schema setup completed")

        # Initialize Spark session
        spark = init_session(config['spark_master'], config)

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
    finally:
        # Clean up
        for query in queries:
            if query.isActive:
                query.stop()
                logger.info("Stopped streaming query")
        if spark:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()