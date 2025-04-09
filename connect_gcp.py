import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
load_dotenv()
def connect_to_postgres():
    try:
        print("Tentando conectar ao banco de dados PostgreSQL no GCP...")
        # Dados de conexão
        host = os.getenv("SERVER")
        database = os.getenv("DATABASE")
        username = os.getenv("USERNAME")
        password = os.getenv("PASSWORD")
        port = os.getenv("PORT")

        # Conexão com o banco de dados
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=username,
            password=password,
            port=port
        )
        print("Conexão com o banco de dados estabelecida com sucesso!")
        return conn

    except Exception as e:
        print("Erro ao conectar ao banco de dados:", e)
        return None

def fetch_data_from_postgres(conn):
    try:
        # Consulta SQL para buscar os dados
        query = "SELECT * FROM projecao_consolidado"   
        print("Executando consulta SQL...")
        
        # Lendo os dados diretamente para um DataFrame do pandas
        df = pd.read_sql_query(query, conn)
        print("Dados carregados com sucesso!")
        return df

    except Exception as e:
        print("Erro ao buscar dados do banco de dados:", e)
        return None

def get_table_insights(df):
    try:
        # Insights
        print("Insights do DataFrame:")
        print(f"- Número de linhas: {len(df)}")
        print(f"- Número de colunas: {len(df.columns)}")
        print(f"- Colunas disponíveis: {list(df.columns)}")
        print(f"- Tipos de colunas:\n{df.dtypes}")
        print(f"- Primeiras 5 linhas:\n{df.head()}")
        print(f"- Últimas 5 linhas:\n{df.tail()}")

    except Exception as e:
        print("Erro ao obter insights do DataFrame:", e)

# Example usage
if __name__ == "__main__":
    conn = connect_to_postgres()
    if conn is not None:
        df = fetch_data_from_postgres(conn)
        if df is not None:
            get_table_insights(df)
        else:
            print("Não foi possível carregar os dados do banco de dados.")
        conn.close()
    else:
        print("Não foi possível estabelecer conexão com o banco de dados.")


 