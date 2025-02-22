import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Argumentos padrão para o DAG
default_args = {
    "owner": "murilobiss",  # Proprietário da DAG
    "start_date": datetime(2023, 9, 3, 10, 00),  # Data de início do DAG
}


def get_data():
    """
    Obtém dados de um usuário aleatório da API randomuser.me.

    Retorna:
        dict: Dados do usuário em formato JSON ou None em caso de erro.
    """
    import requests

    try:
        # Faz a requisição GET para a API
        res = requests.get("https://randomuser.me/api/")
        res.raise_for_status()  # Levanta uma exceção se a requisição falhar
        res = res.json()
        res = res["results"][0]  # Retorna o primeiro usuário
        logging.info("Dados obtidos da API com sucesso.")
        return res
    except Exception as e:
        logging.error(f"Erro ao obter dados da API: {e}")
        return None


def format_data(res):
    """
    Formata os dados brutos do usuário para um formato mais estruturado.

    Parâmetros:
        res (dict): Dados brutos do usuário.

    Retorna:
        dict: Dados formatados ou None em caso de erro.
    """
    try:
        data = {}
        location = res["location"]
        # Gera um ID único para o usuário
        data["id"] = str(uuid.uuid4())
        # Extrai informações do nome
        data["first_name"] = res["name"]["first"]
        data["last_name"] = res["name"]["last"]
        # Extrai o gênero
        data["gender"] = res["gender"]
        # Formata o endereço
        data["address"] = (
            f"{str(location['street']['number'])} {location['street']['name']}, "
            f"{location['city']}, {location['state']}, {location['country']}"
        )
        # Extrai o código postal
        data["post_code"] = location["postcode"]
        # Extrai o e-mail
        data["email"] = res["email"]
        # Extrai o nome de usuário
        data["username"] = res["login"]["username"]
        # Extrai a data de nascimento
        data["dob"] = res["dob"]["date"]
        # Extrai a data de registro
        data["registered_date"] = res["registered"]["date"]
        # Extrai o telefone
        data["phone"] = res["phone"]
        # Extrai a URL da foto
        data["picture"] = res["picture"]["medium"]
        logging.info("Dados formatados com sucesso.")
        return data
    except Exception as e:
        logging.error(f"Erro ao formatar dados: {e}")
        return None


def stream_data():
    """
    Realiza o streaming dos dados dos usuários para o Kafka.

    O streaming continuará por 1 minuto, enviando um usuário por vez.
    """
    import json
    from kafka import KafkaProducer
    import time

    logging.info("Iniciando a função stream_data...")

    try:
        # Configura o produtor Kafka
        producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],  # Endereço do Kafka Broker
            max_block_ms=10000,  # Tempo máximo para aguardar a conexão com o broker
            value_serializer=lambda v: json.dumps(v).encode(
                "utf-8"
            ),  # Serializa os dados para JSON
        )
        logging.info("Produtor Kafka configurado com sucesso.")

        # Define o tempo de execução (1 minuto)
        curr_time = time.time()
        end_time = curr_time + 60

        # Loop de streaming
        while time.time() < end_time:
            try:
                # Obtém os dados do usuário
                res = get_data()
                if res:
                    # Formata os dados
                    formatted_data = format_data(res)
                    if formatted_data:
                        # Envia os dados para o Kafka
                        producer.send("users_created", formatted_data)
                        logging.info(f"Dados enviados para o Kafka: {formatted_data}")
            except Exception as e:
                logging.error(f"Erro durante o streaming: {e}")

        # Garante que todas as mensagens sejam enviadas antes de encerrar
        producer.flush()
        logging.info("Streaming concluído. Todas as mensagens foram enviadas.")
    except Exception as e:
        logging.error(f"Erro ao configurar o produtor Kafka: {e}")


# Definição do DAG (Directed Acyclic Graph) do Airflow
with DAG(
    "user_automation",  # Nome do DAG
    default_args=default_args,  # Argumentos padrão do DAG
    schedule="@daily",  # Execução diária
    catchup=False,  # Não executa as execuções passadas
) as dag:

    # Tarefa do DAG que chama a função stream_data
    streaming_task = PythonOperator(
        task_id="stream_data_from_api",  # ID da tarefa
        python_callable=stream_data,  # Função Python que será executada
    )