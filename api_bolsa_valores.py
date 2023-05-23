
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

def connect_db():  # conexão com o banco.
    conn = psycopg2.connect(
        host="localhost",
        database="nasdaq_csv",
        user="postgres",
        password="klo5s871")
    conn.autocommit=True
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

def create_table(cursor):
    cursor.execute("DROP TABLE IF EXISTS mercado_financeiro;CREATE TABLE mercado_financeiro (Id BIGSERIAL primary key,Id_Market INT,Name_Market VARCHAR(10),Date DATE,Low FLOAT,Open FLOAT,Volume FLOAT,High FLOAT,Close FLOAT,Adjusted_Close FLOAT);")

def read_insert_csv(cursor):
    #listar todos os csv nessa pasta.
    csv_files = glob.glob(
        r'C:\Users\jofin\Desktop\PROJETOS\Projeto API Bolsa de Valores\nasdaq_csv\stock_market_data\nasdaq\csv\*.csv')
    id_market = 1
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            if not df.empty:
                name_file = os.path.basename(file)[:-4]
                for _, row in df.iterrows():
                    cursor.execute("INSERT INTO mercado_financeiro (Id_Market, Name_Market, Date, Low, Open, Volume, High, Close, Adjusted_Close) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);", (id_market, name_file, row['Date'], row['Low'], row['Open'], row['Volume'], row['High'], row['Close'], row['Adjusted Close']))
                id_market += 1
        except pd.errors.EmptyDataError:
            print('Pulando pasta com erro.')
        except pd.errors.ParserError:
            print('Pulando pasta com erro.')

def main_fuction():
    #extract_csv_files()
    #extract_json_files()
    cursor = create_cursor()
    create_table(cursor)
    read_insert_csv(cursor)

main_fuction()