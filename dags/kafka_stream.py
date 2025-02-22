import uuid
import json
import logging
import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from kafka import KafkaProducer

# Configuração básica de logging para facilitar o acompanhamento
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Argumentos padrão para o DAG
default_args = {
    "owner": "airscholar",  # Proprietário da DAG
    "start_date": datetime(2023, 9, 3, 10, 0),  # Data de início do DAG
}


class UserStreamer:
    """
    Classe responsável por buscar dados de usuários e enviá-los para o Kafka.
    """

    def __init__(self, kafka_broker="broker:29092", topic="users_created", duration=60):
        """
        Inicializa o produtor Kafka e configura o tempo de duração da execução.

        :param kafka_broker: Endereço do broker Kafka.
        :param topic: Tópico no Kafka onde os dados serão enviados.
        :param duration: Duração (em segundos) da execução do streaming de dados.
        """
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_broker], max_block_ms=5000
        )
        self.topic = topic
        self.duration = duration

    @staticmethod
    def fetch_user_data():
        """
        Busca os dados de um usuário aleatório usando a API randomuser.me.

        :return: Dados do usuário no formato JSON ou None em caso de erro.
        """
        try:
            # Faz a requisição GET para a API
            response = requests.get("https://randomuser.me/api/")
            response.raise_for_status()  # Gera um erro se a resposta for inválida
            return response.json()["results"][0]  # Retorna o primeiro usuário
        except requests.RequestException as e:
            logging.error(f"Erro ao buscar dados do usuário: {e}")
            return None

    @staticmethod
    def format_user_data(raw_data):
        """
        Formata os dados brutos do usuário para um formato mais estruturado.

        :param raw_data: Dados brutos do usuário.
        :return: Dados formatados ou None caso haja erro.
        """
        if not raw_data:
            return None

        location = raw_data.get("location", {})  # Pega informações de localização
        return {
            "id": str(uuid.uuid4()),  # Gera um ID único para o usuário
            "first_name": raw_data.get("name", {}).get("first", ""),
            "last_name": raw_data.get("name", {}).get("last", ""),
            "gender": raw_data.get("gender", ""),
            "address": f"{location.get('street', {}).get('number', '')} {location.get('street', {}).get('name', '')}, "
            f"{location.get('city', '')}, {location.get('state', '')}, {location.get('country', '')}",
            "post_code": location.get("postcode", ""),
            "email": raw_data.get("email", ""),
            "username": raw_data.get("login", {}).get("username", ""),
            "dob": raw_data.get("dob", {}).get("date", ""),
            "registered_date": raw_data.get("registered", {}).get("date", ""),
            "phone": raw_data.get("phone", ""),
            "picture": raw_data.get("picture", {}).get("medium", ""),
        }

    def stream_users(self):
        """
        Realiza o streaming dos dados dos usuários para o Kafka.

        O streaming continuará por `self.duration` segundos, enviando um usuário por vez.
        """
        end_time = time.time() + self.duration  # Determina o tempo final do streaming
        while time.time() < end_time:
            try:
                # Busca os dados do usuário
                raw_data = self.fetch_user_data()
                # Formata os dados para um formato mais adequado
                user_data = self.format_user_data(raw_data)
                if user_data:
                    # Envia os dados para o Kafka
                    self.producer.send(
                        self.topic, json.dumps(user_data).encode("utf-8")
                    )
                    logging.info(f"Dados do usuário enviados: {user_data['id']}")
            except Exception as e:
                logging.error(f"Erro durante o streaming: {e}")


# Criação da instância da classe UserStreamer
user_streamer = UserStreamer()


# Função Python que será chamada no DAG
def stream_data():
    """
    Função de entrada que chama o método de streaming de usuários.
    """
    user_streamer.stream_users()


# Definição do DAG (Directed Acyclic Graph) do Airflow
with DAG(
    "user_automation",  # Nome do DAG
    default_args=default_args,  # Argumentos padrão do DAG
    schedule_interval="@daily",  # Execução diária
    catchup=False,  # Não executa as execuções passadas
) as dag:

    # Tarefa do DAG que chama a função stream_data
    streaming_task = PythonOperator(
        task_id="stream_data_from_api",  # ID da tarefa
        python_callable=stream_data,  # Função Python que será executada
    )