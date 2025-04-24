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

    # Define default configuration values
    default_config = {
        'spark_master': os.environ.get('SPARK_MASTER', 'spark://spark-master:7077'),
        'kafka_bootstrap_servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        'kafka_topic': os.environ.get('KAFKA_TOPIC', 'cryptocurrency'),
        'checkpoint_location': os.environ.get('CHECKPOINT_LOCATION', './checkpoint'),
        'output_path': os.environ.get('OUTPUT_PATH', './output'),
        'cassandra_contact_points': os.environ.get('CASSANDRA_CONTACT_POINTS', 'cassandra'),
        'cassandra_port': os.environ.get('CASSANDRA_PORT', '9042'),
        'cassandra_keyspace': os.environ.get('CASSANDRA_KEYSPACE', 'crypto'),
        'cassandra_replication_factor': os.environ.get('CASSANDRA_REPLICATION_FACTOR', '1')
    }

    # Convert types and process contact points
    return {
        'spark_master': default_config['spark_master'],
        'kafka_bootstrap_servers': default_config['kafka_bootstrap_servers'],
        'kafka_topic': default_config['kafka_topic'],
        'checkpoint_location': default_config['checkpoint_location'],
        'output_path': default_config['output_path'],
        'cassandra_contact_points': default_config['cassandra_contact_points'].split(','),
        'cassandra_port': int(default_config['cassandra_port']),
        'cassandra_keyspace': default_config['cassandra_keyspace'],
        'cassandra_replication_factor': int(default_config['cassandra_replication_factor'])
    }

def init_session(master, cassandra_config=None):
    """Initialize Spark session with Kafka and Cassandra dependencies."""
    try:
        # Reduced memory and resource settings for compatibility with workers
        session = (SparkSession.builder
                   .master(master)
                   .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
                   .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                   .config("spark.cassandra.connection.host", ",".join(cassandra_config['cassandra_contact_points']))
                   .config("spark.cassandra.connection.port", cassandra_config['cassandra_port'])
                   # Reduced resource settings
                   .config("spark.sql.shuffle.partitions", "4")  # Reduced from 8
                   .config("spark.memory.fraction", "0.7")
                   .config("spark.memory.storageFraction", "0.3")
                   .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                   .config("spark.executor.memory", "800m")  # Reduced from 2g
                   .config("spark.executor.cores", "1")  # Explicitly set to 1 core
                   .config("spark.driver.memory", "800m")  # Reduced from 2g
                   .config("spark.streaming.backpressure.enabled", "true")
                   .config("spark.streaming.kafka.consumer.cache.enabled", "true")
                   .config("spark.cassandra.connection.keep_alive_ms", "60000")
                   .config("spark.cassandra.output.batch.size.rows", "50")  # Reduced batch size
                   .config("spark.cassandra.connection.timeout_ms", "10000")
                   .appName("CryptoTradeStreaming")
                   .getOrCreate())
        logger.info("Spark session initialized successfully")
        return session
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {str(e)}")
        raise

def create_cassandra_schema(config, max_retries=5, retry_delay=5):
    """Create keyspace and table in Cassandra with retry logic."""
    for attempt in range(max_retries):
        try:
            # Connect to Cassandra cluster
            cluster = Cluster(
                contact_points=config['cassandra_contact_points'],
                port=config['cassandra_port'],
                connect_timeout=30,
                control_connection_timeout=30
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
            logger.info(f"Keyspace '{config['cassandra_keyspace']}' is ready")

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
              AND default_time_to_live = 2592000;  -- 30 days TTL
            """
            session.execute(SimpleStatement(table_cql))
            logger.info("Table 'trades' ready")

            # Add an index on timestamp for better query performance
            index_cql = """
            CREATE INDEX IF NOT EXISTS ON trades (timestamp);
            """
            session.execute(SimpleStatement(index_cql))
            logger.info("Index on timestamp created")

            return True

        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{max_retries} failed to create Cassandra schema: {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                raise
        finally:
            if 'session' in locals() and session:
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
        # Read streaming data from Kafka with improved retry and timeout settings
        kafka_df = (spark
                    .readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", config['kafka_bootstrap_servers'])
                    .option("subscribe", config['kafka_topic'])
                    .option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "false")
                    .option("maxOffsetsPerTrigger", 5000)  # Reduced from 10000
                    .option("kafka.session.timeout.ms", "120000")
                    .option("kafka.heartbeat.interval.ms", "40000")
                    .option("kafka.request.timeout.ms", "150000")
                    .option("kafka.max.poll.records", "2000")  # Reduced from 5000
                    .option("kafka.fetch.max.wait.ms", "500")
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

        # Create a list to store all queries for proper management
        queries = []

        # Write to console (limited output for debugging)
        console_query = (exploded_df
                         .writeStream
                         .outputMode("append")
                         .format("console")
                         .option("truncate", "false")
                         .option("numRows", 5)
                         .option("checkpointLocation", config['checkpoint_location'] + "/console")
                         .trigger(processingTime="10 seconds")  # Increased from 5 seconds
                         .start())
        logger.info("Started console streaming query")
        queries.append(console_query)

        # Write to Cassandra with optimized write settings
        cassandra_query = (exploded_df
                           .writeStream
                           .format("org.apache.spark.sql.cassandra")
                           .option("keyspace", config['cassandra_keyspace'])
                           .option("table", "trades")
                           .option("checkpointLocation", config['checkpoint_location'] + "/cassandra")
                           .option("spark.cassandra.output.consistency.level", "LOCAL_ONE")  # Changed from LOCAL_QUORUM
                           .option("spark.cassandra.output.concurrent.writes", "4")  # Reduced from 8
                           .option("spark.cassandra.output.batch.size.rows", "64")  # Reduced from 128
                           .option("spark.cassandra.output.throughput_mb_per_sec", "20")  # Reduced from 50
                           .outputMode("append")
                           .trigger(processingTime="10 seconds")  # Increased from 5 seconds
                           .start())
        logger.info("Started Cassandra streaming query")
        queries.append(cassandra_query)

        # Return all queries for management
        return queries

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

        # Create directories for checkpoint if they don't exist
        os.makedirs(config['checkpoint_location'], exist_ok=True)
        os.makedirs(config['checkpoint_location'] + "/console", exist_ok=True)
        os.makedirs(config['checkpoint_location'] + "/cassandra", exist_ok=True)
        os.makedirs(config['output_path'], exist_ok=True)

        # Create Cassandra schema
        logger.info("Creating Cassandra schema")
        create_cassandra_schema(config)
        logger.info("Cassandra schema setup completed")

        # Initialize Spark session
        spark = init_session(config['spark_master'], config)

        # Add delay to ensure workers are registered
        logger.info("Waiting for Spark workers to be fully registered...")
        time.sleep(15)  # Extra safety delay

        # Process streaming data - capture all queries
        queries = process_streaming_data(spark, config)

        # Set up a termination hook for clean shutdown
        import signal
        def signal_handler(sig, frame):
            logger.info("Received shutdown signal, stopping streaming queries")
            for query in queries:
                if query.isActive:
                    query.stop()
                    logger.info("Stopped streaming query")
            if spark:
                spark.stop()
                logger.info("Stopped Spark session")
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Wait for any query to terminate or external signal
        for query in queries:
            query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, stopping streaming queries")
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        sys.exit(1)
    finally:
        # Clean up
        for query in queries:
            if query and query.isActive:
                query.stop()
                logger.info("Stopped streaming query")

        if spark:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()