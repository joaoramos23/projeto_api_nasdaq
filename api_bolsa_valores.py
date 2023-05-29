
"""
Comando para download da pasta .zip: 'kaggle datasets download -d paultimothymooney/stock-market-data
Basta voce colocar o destino da sua pasta stock-market-data.zip e usar a função de extração.
Diferenças de csv e json:
- Json é mais pesado.
- Json é mais legivel.
- É mais facil de tratar dados com Json.
- Json suporta tipo de dados mais complexos.
- Json é mais utilizado para pegar dados em API.
- CSV é mais utilizado para importar/exportar dados de planilhas ou banco de dados.
- Diferente do CSV o Json tem suporte para metadados.
"""
from zipfile import ZipFile
import os
import pandas as pd
import glob
import psycopg2
import psycopg2.extras as extras
import datetime


def connect_db():  # conexão com o banco.
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="USER",
        password="SENHA")
    conn.autocommit = True
    if conn.closed == 0:
        print("Conexão estabelecida.")
    else:
        print("A conexão está fechada.")
    return conn


def create_cursor():
    conn = connect_db()
    cursor = conn.cursor()  # criando cursor para executar no banco.
    return cursor


def extract_csv_files():
    with ZipFile('stock-market-data.zip', 'r') as z:
        for file in z.namelist():
            if 'nasdaq/csv' in file:
                z.extract(member=file, path='nasdaq_csv')


def extract_json_files():
    with ZipFile('stock-market-data.zip', 'r') as zip_file:
        for file in zip_file.namelist():
            if 'nasdaq/json' in file:
                zip_file.extract(file, path='nasdaq_json')


def create_table_csv(cursor):
    cursor.execute("DROP TABLE IF EXISTS mercado_financeiro_csv;CREATE TABLE mercado_financeiro_csv (Id BIGSERIAL primary key,Id_Market INT,Name_Market VARCHAR(10),Date DATE,Low FLOAT,Open FLOAT,Volume FLOAT,High FLOAT,Close FLOAT,Adjusted_Close FLOAT);")

def read_insert_csv(cursor):
    # criando tabela
    create_table_csv(cursor)
    # listar todos os csv nessa pasta.
    csv_files = glob.glob(
        r'C:\Users\jofin\Desktop\PROJETOS\Projeto API Bolsa de Valores\nasdaq_csv\stock_market_data\nasdaq\csv\*.csv')
    id_market = 1
    dados = []
    for file in csv_files:
        name_file = os.path.basename(file)[:-4]
        try:
            df = pd.read_csv(file)
            if not df.empty:
                for _, row in df.iterrows():
                    lista = [id_market, name_file, row['Date'], row['Low'], row['Open'], row['Volume'], row['High'], row['Close'], row['Adjusted Close']]
                    dados.append(lista)
                id_market += 1
        except (pd.errors.EmptyDataError, pd.errors.ParserError, KeyError):
            pass
    extras.execute_values(cursor,"INSERT INTO mercado_financeiro_csv (Id_Market, Name_Market, Date, Low, Open, Volume, High, Close, Adjusted_Close) VALUES %s", dados)

def create_table_json(cursor):
    cursor.execute("DROP TABLE IF EXISTS mercado_financeiro_json ; CREATE TABLE mercado_financeiro_json (Id BIGSERIAL primary key,Id_Market INT,Name_Market VARCHAR(10),Date DATE,Low FLOAT,Open FLOAT,Volume FLOAT,High FLOAT,Close FLOAT,Adjusted_Close FLOAT);")

def cl_name(column,i,df):
    if column == 'adjclose':
        return df['indicators'][0][column][0][column][i]
    return df['indicators'][0]['quote'][0][column][i]

def read_insert_json(cursor):
    # criando tabela
    create_table_json(cursor)
    # listar todos os csv nessa pasta.
    json_files = glob.glob(
        r'C:\Users\jofin\Desktop\PROJETOS\Projeto API Bolsa de Valores\nasdaq_json\stock_market_data\nasdaq\json\*.json')
    id_market = 1
    dados = []
    for file in json_files:
        try:
            df = pd.read_json(file)
            df = df['chart']['result']
            df = pd.DataFrame(df)
            name_file = os.path.basename(file)[:-5]
            for i, item in enumerate(df['timestamp'][0]):
                item = datetime.datetime.fromtimestamp(item)
                lista = [id_market,name_file,item,cl_name('low',i,df),cl_name('open',i,df),cl_name('volume',i,df),cl_name('high',i,df),cl_name('close',i,df),cl_name('adjclose',i,df)]
                dados.append(lista)
            id_market += 1    
        except (pd.errors.EmptyDataError, pd.errors.ParserError, KeyError,ValueError):
            pass
    extras.execute_values(cursor,"INSERT INTO mercado_financeiro_json (Id_Market, Name_Market, Date, Low, Open, Volume, High, Close, Adjusted_Close) VALUES %s", dados)
        
def main_fuction():
    # extract_csv_files()
    # extract_json_files()
    cursor = create_cursor()
    print(datetime.datetime.now())
    read_insert_csv(cursor)
    #read_insert_json(cursor)
    print(datetime.datetime.now())


main_fuction()


