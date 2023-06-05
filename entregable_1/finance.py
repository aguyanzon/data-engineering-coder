import os

import pandas as pd
import redshift_connector
import requests
from dotenv import load_dotenv

load_dotenv('.env')

def extract_data(symbol):
    try:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={symbol}&apikey={os.getenv("API_KEY")}'
        response = requests.get(url)
        json_data = response.json()
        data = pd.DataFrame(json_data['Monthly Time Series'])
        data = data.T
        data['symbol'] = symbol
        
        return data

    except requests.exceptions.RequestException as e:
        print(f"Error de solicitud: {e}")
        return None

def transform_data(data):
    data.rename(columns = {
        "1. open" : "open", 
        "2. high" : "high", 
        "3. low" : "low", 
        "4. close" : "close", 
        "5. volume" : "volume"
        }, inplace=True)
    for column in data[["open", "high", "low", "close", "volume"]]:
        data[column] = data[column].astype(float)

    data.reset_index(inplace=True)
    data.rename(columns={"index":"date"}, inplace=True)
    data.date = pd.to_datetime(data.date)

    return data

def load_data(data):
    # Nombre de la tabla y esquema
    tabla = "finance_ibm"
    esquema = "aguyanzon_coderhouse"

    # Crea la conexión a Redshif
    print("Conectando a Redshift...")
    conn = redshift_connector.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=int(os.getenv("REDSHIFT_PORT")),
        database=os.getenv("REDSHIFT_DATABASE"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD")
    )
    print('Conexion a Redshift creada...')

    # Crea la tabla en el esquema especificado
    create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {esquema}.{tabla} (
            "date" DATE distkey,
            "open" VARCHAR(255),
            high VARCHAR(255),
            low VARCHAR(255),
            close VARCHAR(255), 
            volume VARCHAR(255),
            symbol VARCHAR(255)
        ) sortkey(date);
    '''

    with conn.cursor() as cursor:
        print(f"Creando tabla {tabla}")
        cursor.execute(create_table_query)
        for index, row in data.iterrows():
            print(f"Insertando fila {index + 1} de {len(data)}")
            cursor.execute(
                f'''INSERT INTO {esquema}.{tabla} ("date", "open", high, low, close, volume, symbol) VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                (row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['symbol'])
            )
    conn.commit()

    # Cierra la conexión a Redshift
    conn.close()

if __name__ == "__main__":

    print("Extrayendo datos...")
    data_ibm = extract_data('IBM')
    data_aapl = extract_data('AAPL')
    data_tsla = extract_data('TSLA')
    data = pd.concat([data_ibm, data_aapl, data_tsla], axis=0)

    if data is not None:
        print("Datos extraídos exitosamente.")

        print("Transformando datos...")
        transformed_data = transform_data(data)
        print("Datos transformados exitosamente.")

        print("Cargando datos en Redshift...")
        load_data(transformed_data)
        print("Datos cargados exitosamente.")
