import urllib3
import pandas as pd
import hashlib
import time
import sqlite3
import os

def get_data(endpoint_url: str) -> str:
    http = urllib3.PoolManager()
    response = http.request('GET', endpoint_url)
    return response.data

def encrypt_data_with_sha1(data: str) -> str:
    return hashlib.sha1(data).hexdigest().upper()

def make_dataframe(response: str) -> pd.DataFrame:
    sha1_NA = encrypt_data_with_sha1('NA'.encode())
    data_t = pd.read_json(response)
    data_t.languages.fillna('NA', inplace=True)
    df = pd.DataFrame(columns=['Region', 'City Name', 'Languaje', 'Time'])

    for index in range(len(data_t)):
        start = time.perf_counter()
        list_temp = [data_t.region[index], data_t.name[index]['common']]

        if data_t.languages[index] == 'NA':
            list_temp.append(sha1_NA)
        else:
            list_temp.append(encrypt_data_with_sha1(list(data_t.languages[index].values())[0].encode()))

        end = time.perf_counter()
        list_temp.append(round((end - start)*1000, 3))
        df.loc[index] = list_temp

    return df

def print_statistics(dataframe: pd.DataFrame):
    print(f'Tiempo total en construir el datafreame = {round(dataframe.Time.sum(),2)} ms')
    print(f'Tiempo promedio en construir las filas del  datafreame = {round(dataframe.Time.mean(),2)} ms')
    print(f'Tiempo mínimo en agregar una fila = {dataframe.Time.min()} ms')
    print(f'Tiempo máximo en agregar una fila = {dataframe.Time.max()} ms')

def create_db(dataframe:  pd.DataFrame):
    conn = sqlite3.connect(os.getcwd()+'/data.db')
    dataframe.to_sql('challenge', conn, if_exists='replace', index=False)
    cursor = conn.cursor()

    for index in range(len(dataframe)):
        cursor.execute("INSERT INTO challenge VALUES (?,?,?,?)", dataframe.iloc[index])
    conn.close()

def create_json():
    ''' Leemos la base de datos y la convertimos en un archivo json'''
    conn = sqlite3.connect(os.getcwd()+'/data.db')
    dataframe = pd.read_sql_query("SELECT * FROM challenge", conn)
    dataframe.to_json('data.json', orient='records')
    conn.close()
