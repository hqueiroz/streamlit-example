import pandas as pd
import streamlit as st
#import google.auth
from google.oauth2 import service_account
from google.cloud import bigquery


# # Caminho para o arquivo JSON de chave privada
# key_path = '/app/credentials.json'

# # Autenticação usando a conta de serviço
# credentials = service_account.Credentials.from_service_account_file(key_path)

# Nome do projeto do Google Cloud


# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)


project_id = 'cities-rurax'

def load_data():
    query = """
    SELECT 
    *
    FROM 
    `cities-rurax.insight_esalq.tb_precos_boi` 
    LIMIT 10
    """
    df = pd.read_gbq(query, dialect="standard", project_id=project_id, credentials=credentials)
    return df

# Carregar os dados
data = load_data()

# Título da aplicação
st.title("Tabela de Dados do BigQuery")

# Exibir o DataFrame no Streamlit
st.write(data)