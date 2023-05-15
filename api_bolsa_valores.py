import kaggle
import zipfile
import json
import csv


def extract_csv_files():
    z = zipfile.ZipFile('stock-market-data.zip', 'r')
    for arquivo in z.namelist():
        if 'nasdaq/csv' in arquivo:
            z.extract(arquivo, 'nasdaq_csv')
    z.close()


def extract_json_files():
    z = zipfile.ZipFile('stock-market-data.zip', 'r')
    for arquivo in z.namelist():
        if 'nasdaq/json' in arquivo:
            z.extract(arquivo, 'nasdaq_json')
    z.close()

with open('AAL.csv', "r") as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',')
    for i, row in enumerate(spamreader):
        if i == 0:
            print('Cabecalho: ' + str(row))
        else:
            print('Valor: ' + str(row))

#utilizar pandas
