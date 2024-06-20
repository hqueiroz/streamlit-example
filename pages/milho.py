import pandas as pd
import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq
import datetime

#Configuração Página
st.set_page_config(
    page_title="RURAX >> COTAÇÕES"
)

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

project_id = 'cities-rurax'

# Título da aplicação
st.title(":green[>> MILHO]")

#Imagem Topo
st.image('https://i.imgur.com/02wX7AE.png',use_column_width='always')


with st.sidebar:

    #Logo
    st.image("https://i.imgur.com/O50xA7q.png",width=200)

    # # Título da aplicação
    # st.title(":green[>> COTAÇÕES RURAX]")

    st.write("Escolha o período desejado:")

    #Data Atual
    data_atual = datetime.datetime.now()

    #Data inicial do período
    data_inicial = st.date_input("DATA INÍCIO",value=data_atual - datetime.timedelta(weeks=24),min_value=datetime.date(2023,1,1),format="DD/MM/YYYY")

    #Data final do período
    data_final = st.date_input("DATA FINAL",value="default_value_today",min_value=datetime.date(2024,1,1),format="DD/MM/YYYY")

    #st.write(":green[>> BOI GORDO]")
    st.page_link("streamlit_app.py", label=">> BOI GORDO")
    st.image("https://i.imgur.com/edshqj2.png",width=75)

    #st.write(":green[>> SOJA]")
    st.page_link("pages/soja.py", label=">> SOJA")
    st.image("https://i.imgur.com/ck4EKWb.png",width=75)

    #st.write(":green[>> MILHO]")
    st.page_link("pages/milho.py", label=">> MILHO", disabled=True)
    st.image("https://i.imgur.com/QPNz06I.png",width=75)


@st.cache_data
def load_data():
    query = """
    select 
        cast(dt_preco as date format 'dd/mm/yyyy') data_preco, 
        ds_produto, 
        vl_preco_a_vista_br as vl_preco, 
        'INDICADOR DO MILHO ESALQ/BM&FBOVESPA' as ds_serie 
        from `insight_esalq.tb_precos_milho` 
    where 
        cast(dt_preco as date format 'dd/mm/yyyy') > '2023-01-01' 
        and ds_serie = 'Milho | INDICADOR DO MILHO ESALQ/BM&FBOVESPA                                                 '
        and vl_preco_a_vista_br <> '-' 
    order by 
        cast(dt_preco as date format 'dd/mm/yyyy') desc
    """
    df = pandas_gbq.read_gbq(query, dialect="standard", project_id=project_id, credentials=credentials).rename(columns={
        'vl_preco':'PREÇO (R$)',
        'ds_produto':'PRODUTO',
        'data_preco':'DATA COTAÇÃO',
        #'ds_nota':'NOTA',
        'ds_serie':'SÉRIE'})
    df['PREÇO (R$)'] = df['PREÇO (R$)'].replace('-','0',regex=True)
    df['PREÇO (R$)'] = df['PREÇO (R$)'].replace(',','.',regex=True)
    df['PREÇO (R$)'] = df['PREÇO (R$)'].astype(float)
    df['SÉRIE'] = df['SÉRIE'].replace('Milho | ','',regex=True)
    df['DATA COTAÇÃO'] = pd.to_datetime(df['DATA COTAÇÃO'],format='%d/%m/%Y',errors='coerce').dt.date
    return df

# Carregar os dados
data = load_data()

# Filtra Datas
filtro_df = data[(data['DATA COTAÇÃO'] >= data_inicial) & (data['DATA COTAÇÃO'] <= data_final)]
st.write(filtro_df)

# Ordena por data
df_sorted = data.sort_values(by='DATA COTAÇÃO', ascending=False)

#Fonte
st.write(":green[FONTE: INDICADOR DO MILHO ESALQ/BM&FBOVESPA]")
st.write(":green[NOTA: À vista por saca de 60 kg, descontado o Prazo de Pagamento pela taxa CDI/CETIP.]")


# Pega data mais recente
data_mais_recente = df_sorted.iloc[0]
dia_anterior = df_sorted.iloc[1]
inicio_serie = filtro_df.loc[filtro_df['DATA COTAÇÃO'].idxmin()]


# Pega valor e data atual
vl_atual = data_mais_recente['PREÇO (R$)']
dt_atual = data_mais_recente['DATA COTAÇÃO']
vl_inicio_serie = inicio_serie['PREÇO (R$)']

# Pega cotação dia anterior e cálculo variação
vl_anterior = dia_anterior['PREÇO (R$)']
pct_var = (vl_atual / vl_anterior)-1
pct_periodo = (vl_atual / vl_inicio_serie)-1
pct = "{:.4%}".format(pct_var)
ind_pct_periodo= "{:.4%}".format(pct_periodo)
vl_max_serie = filtro_df['PREÇO (R$)'].max()
vl_min_serie = filtro_df['PREÇO (R$)'].min()
vl_medio_serie = round(filtro_df['PREÇO (R$)'].mean(),2)


# Indicadores
col1, col2,col3 = st.columns(3)
col1.metric(label=":calendar: Data Cotação Atual", value=str(dt_atual))
col2.metric(label=":chart: Valor Atual (R$) / Var. dia anterior (%)", value=vl_atual, delta=pct)
#col3.metric(label=":currency_exchange: Valor Anterior (R$)", value=vl_anterior)
col3.metric(label=":chart: Valor Início Período (R$) / Var. Período (%)", value=vl_inicio_serie,delta=ind_pct_periodo)

# Valores Max e Min e Medio
max_serie, min_serie,medio_serie = st.columns(3)
max_serie.metric(label=":arrow_up_small: Valor Máximo no Período (R$)", value=(vl_max_serie))
min_serie.metric(label=":arrow_down_small: Valor Mínimo no Período (R$)", value=(vl_min_serie))
medio_serie.metric(label=":small_orange_diamond: Valor Médio no Período (R$)", value=(vl_medio_serie))

#Campos Gráfico
x_field = 'PREÇO (R$)'
y_field = 'DATA COTAÇÃO'
st.line_chart(filtro_df[[x_field, y_field]].set_index(y_field),color='#50C878')
#st.area_chart(data,y=x_field,x=y_field,color='#50C878')