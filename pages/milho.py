import pandas as pd
import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq
import datetime

#Configuração Página
st.set_page_config(
    page_title="RURAX >> COTAÇÕES",layout="wide"
)

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

project_id = 'cities-rurax'

# Ajusta largura página
css='''
<style>
    section.main > div {max-width:55rem}
</style>
'''
st.markdown(css, unsafe_allow_html=True)

# Título da aplicação
st.title(":green[>> MILHO]")

#Imagem Topo
st.image('https://i.imgur.com/02wX7AE.png', use_column_width='always')


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

  #LINHA 1
    boi, soja = st.columns(2)
    boi.page_link("streamlit_app.py", label="BOI GORDO")
    boi.image('https://i.imgur.com/edshqj2.png',width=60)

    soja.page_link("pages/soja.py", label="SOJA")
    soja.image("https://i.imgur.com/ck4EKWb.png",width=60)
    
    #LINHA 2
    bezerro,milho = st.columns(2) 
    bezerro.page_link("pages/bezerro.py", label="BEZERRO")
    bezerro.image("https://i.imgur.com/Na2tAXo.jpg",width=60)

    milho.page_link("pages/milho.py", label="MILHO", disabled=True)
    milho.image("https://i.imgur.com/jj9iNZo.jpg",width=60)

    #LINHA 3
    arroz,cafe = st.columns(2) 
    arroz.page_link("pages/arroz.py", label="ARROZ")
    arroz.image("https://i.imgur.com/5WiVtve.jpg",width=60)

    cafe.page_link("pages/cafe.py", label="CAFÉ")
    cafe.image("https://i.imgur.com/8kzzJkb.jpg",width=60)

    #LINHA 4
    frango, trigo= st.columns(2) 
    frango.page_link("pages/frango.py", label="FRANGO")
    frango.image("https://i.imgur.com/9GYGVpy.jpg",width=60)

    trigo.page_link("pages/trigo.py", label="TRIGO")
    trigo.image("https://i.imgur.com/KulujJW.jpg",width=60)

    #LINHA 5
    suino, acucar= st.columns(2) 
    suino.page_link("pages/suino.py", label="SUÍNO")
    suino.image("https://i.imgur.com/p7yyzvM.jpg",width=60)

    acucar.page_link("pages/acucar.py", label="AÇUCAR")
    acucar.image("https://i.imgur.com/h4pB7Zl.jpg",width=60)

# Função para formatar o preço
def format_price(price):
    return f"{price:,.2f}".replace(',', 'v').replace('.', ',').replace('v', '.')

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
        'ds_serie':'SÉRIE'})
    #df['PREÇO (R$)'] = 'R$ '+df['PREÇO (R$)'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float).apply(format_price)
    #df['DATA COTAÇÃO'] = pd.to_datetime(df['DATA COTAÇÃO'],format='%d/%m/%Y',errors='coerce').dt.date
    #df['DATA COTAÇÃO'] = df['DATA COTAÇÃO'].dt.strftime('%d/%m/%Y')
    df = df.sort_values(by='DATA COTAÇÃO', ascending=False)
    return df

# Carregar os dados
data = load_data()

#Formata Indicadores
indicadores = load_data()
indicadores['PREÇO (R$)'] = indicadores['PREÇO (R$)'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
indicadores = indicadores[(indicadores['DATA COTAÇÃO'] >= data_inicial) & (indicadores['DATA COTAÇÃO'] <= data_final)]
indicadores = indicadores.sort_values(by='DATA COTAÇÃO', ascending=False)
#indicadores['DATA COTAÇÃO'] = indicadores['DATA COTAÇÃO'].dt.strftime('%d/%m/%Y')

#Pega data mais recente
data_mais_recente = indicadores.iloc[0]
dia_anterior = indicadores.iloc[1]
inicio_serie = indicadores.loc[indicadores['DATA COTAÇÃO'].idxmin()]

# Pega valor e data atual
#data['PREÇO (R$)']=data['PREÇO (R$)'].str.replace('R$ ', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
vl_atual = data_mais_recente['PREÇO (R$)']
vl_anterior = dia_anterior['PREÇO (R$)']
dt_atual = data_mais_recente['DATA COTAÇÃO']
vl_inicio_serie = inicio_serie['PREÇO (R$)']

# Pega cotação dia anterior e cálculo variação
pct_var = (vl_atual / vl_anterior)-1
pct_periodo = (vl_atual / vl_inicio_serie)-1
pct = "{:.2%}".format(pct_var)
ind_pct_periodo= "{:.2%}".format(pct_periodo)
vl_max_serie = indicadores['PREÇO (R$)'].max()
vl_min_serie = indicadores['PREÇO (R$)'].min()
vl_medio_serie = round(indicadores['PREÇO (R$)'].mean(),2)

# Indicadores
col1, col2,col3 = st.columns(3)
dt_metrica = pd.to_datetime(dt_atual).strftime('%d/%m/%Y')
vl_metrica = 'R$ ' + format_price(vl_atual)
vl_inicio = 'R$ ' + format_price(vl_inicio_serie)
col1.metric(label=":calendar: Data Cotação Atual", value=str(dt_metrica))
col2.metric(label=":chart: Valor Atual (R$) / Var. dia anterior (%)", value=vl_metrica, delta=pct)
col3.metric(label=":chart: Valor Início Período (R$) / Var. Período (%)", value=vl_inicio,delta=ind_pct_periodo)

# Valores Max e Min e Medio
max_serie, min_serie,medio_serie = st.columns(3)
vl_max = 'R$ '+ format_price(vl_max_serie)
vl_min = 'R$ ' + format_price(vl_min_serie)
vl_medio = 'R$ ' + format_price(vl_medio_serie)
max_serie.metric(label=":arrow_up_small: Valor Máximo no Período (R$)", value=vl_max)
min_serie.metric(label=":arrow_down_small: Valor Mínimo no Período (R$)", value=vl_min)
medio_serie.metric(label=":small_orange_diamond: Valor Médio no Período (R$)", value=vl_medio)

#Formata tabela
st.write(":green[>> COTAÇÕES]")
tabela = data 
tabela['PREÇO (R$)'] = 'R$ '+tabela['PREÇO (R$)'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float).apply(format_price)
tabela = tabela[(tabela['DATA COTAÇÃO'] >= data_inicial) & (tabela['DATA COTAÇÃO'] <= data_final)]
tabela['DATA COTAÇÃO'] = pd.to_datetime(tabela['DATA COTAÇÃO'],format='%d/%m/%Y',errors='coerce')
tabela['DATA COTAÇÃO'] = tabela['DATA COTAÇÃO'].dt.strftime('%d/%m/%Y')
st.write(tabela)

#Fonte
st.write(":green[FONTE: INDICADOR DO MILHO ESALQ/BM&FBOVESPA]")
st.write(":green[NOTA: À vista por saca de 60 kg, descontado o Prazo de Pagamento pela taxa CDI/CETIP.]")

#Campos Gráfico
st.write(" ")
st.write(" ")
st.write(":green[>> SÉRIE - PERÍODO SELECIONADO]")
st.write(" ")
grafico = load_data()
grafico['PREÇO (R$)'] = 'R$ '+grafico['PREÇO (R$)'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float).apply(format_price)
grafico = grafico[(grafico['DATA COTAÇÃO'] >= data_inicial) & (grafico['DATA COTAÇÃO'] <= data_final)]
grafico = grafico.sort_values(by='DATA COTAÇÃO', ascending=False)
#grafico.set_index('DATA COTAÇÃO',inplace=True)
x_field = 'PREÇO (R$)'
y_field = 'DATA COTAÇÃO'
st.line_chart(grafico[[x_field, y_field]].set_index(y_field),color='#50C878')