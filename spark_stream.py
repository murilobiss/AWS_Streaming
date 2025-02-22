import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Configura o log para exibir mensagens de informação e erro
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Função para criar o keyspace no Cassandra se não existir
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    logging.info("Keyspace 'spark_streams' criado com sucesso!")

# Função para criar a tabela no Cassandra se não existir
def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)
    logging.info("Tabela 'created_users' criada com sucesso!")

# Função para inserir dados no Cassandra
def insert_data(session, **kwargs):
    logging.info("Inserindo dados...")

    # Recupera os dados do usuário passados como parâmetros
    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        # Executa a inserção no Cassandra
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Dados inseridos para {first_name} {last_name}")

    except Exception as e:
        logging.error(f'Erro ao inserir dados: {e}')

# Função para criar a conexão com o Spark
def create_spark_connection():
    try:
        # Cria a sessão do Spark
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        # Configura o nível de log do Spark
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Conexão com o Spark criada com sucesso!")
    except Exception as e:
        logging.error(f"Erro ao criar a sessão do Spark: {e}")

    return s_conn

# Função para conectar ao Kafka e ler os dados de uma determinada topic
def connect_to_kafka(spark_conn):
    try:
        # Lê o stream de dados do Kafka
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("DataFrame do Kafka criado com sucesso!")
    except Exception as e:
        logging.warning(f"Erro ao criar o DataFrame do Kafka: {e}")
        return None

    return spark_df

# Função para criar a conexão com o Cassandra
def create_cassandra_connection():
    try:
        # Conecta ao cluster Cassandra
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        logging.info("Conexão com o Cassandra criada com sucesso!")
        return cas_session
    except Exception as e:
        logging.error(f"Erro ao criar a conexão com o Cassandra: {e}")
        return None

# Função para criar o DataFrame de seleção a partir do Kafka
def create_selection_df_from_kafka(spark_df):
    # Define o schema dos dados esperados
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    # Converte o valor da coluna 'value' para o formato JSON
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    logging.info("DataFrame de seleção criado a partir do Kafka.")
    
    return sel

# Função principal para iniciar o processo de streaming
if __name__ == "__main__":
    # Cria a conexão com o Spark
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Conecta ao Kafka e obtém os dados
        spark_df = connect_to_kafka(spark_conn)

        # Cria o DataFrame de seleção
        selection_df = create_selection_df_from_kafka(spark_df)

        # Cria a conexão com o Cassandra
        session = create_cassandra_connection()

        if session is not None:
            # Cria o keyspace e a tabela no Cassandra
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming iniciado...")

            # Inicia o streaming para gravar os dados no Cassandra
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            # Aguardar a terminação do streaming
            streaming_query.awaitTermination()
