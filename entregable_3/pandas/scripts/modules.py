import requests
import pandas as pd
import redshift_connector

from os import environ as env

class ETL_Pandas:
    def __init__(self):
        self.REDSHIFT_HOST = env["REDSHIFT_HOST"]
        self.REDSHIFT_PORT = env["REDSHIFT_PORT"]
        self.REDSHIFT_DB = env["REDSHIFT_DB"]
        self.REDSHIFT_USER = env["REDSHIFT_USER"]
        self.REDSHIFT_PASSWORD = env["REDSHIFT_PASSWORD"]
        self.REDSHIFT_URL = env["REDSHIFT_URL"]
        self.REDSHIFT_SCHEMA = env["REDSHIFT_SCHEMA"]

    def extract(self, symbol):
        """
        Extrae datos de la API
        """
        print(">>> [E] Extrayendo datos de la API...")

        try:
            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={symbol}&apikey={env["API_KEY"]}'
            response = requests.get(url)
            json_data = response.json()
            data = pd.DataFrame(json_data['Monthly Time Series'])
            data = data.T
            data['symbol'] = symbol

            return data

        except requests.exceptions.RequestException as e:
            print(f"Error de solicitud: {e}")
            return None

    def transform(self, data):
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")

        data.rename(columns={
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. volume": "volume"
        }, inplace=True)

        for column in data[["open", "high", "low", "close", "volume"]]:
            data[column] = data[column].astype(float)

        data.reset_index(inplace=True)
        data.rename(columns={"index": "date"}, inplace=True)
        data.date = pd.to_datetime(data.date)

        return data

    def load(self, df_final):
        """
        Carga los datos transformados en Redshift
        """
        print(">>> [L] Cargando datos en Redshift...")
        print("Conectando a Redshift...")

        conn = redshift_connector.connect(
            host=self.REDSHIFT_HOST,
            port=int(self.REDSHIFT_PORT),
            database=self.REDSHIFT_DB,
            user=self.REDSHIFT_USER,
            password=self.REDSHIFT_PASSWORD
        )

        print('Conexion a Redshift creada...')

        # Crea la tabla en el esquema especificado
        create_table_query = f'''
            CREATE TABLE IF NOT EXISTS {self.REDSHIFT_SCHEMA}.finance (
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
            print(f"Creando tabla finance")
            cursor.execute(create_table_query)
            for index, row in df_final.iterrows():
                print(f"Insertando fila {index + 1} de {len(df_final)}")
                cursor.execute(
                    f'''INSERT INTO {self.REDSHIFT_SCHEMA}.finance ("date", "open", high, low, close, volume, symbol) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                    (row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['symbol'])
                )
        conn.commit()

        # Cierra la conexión a Redshift
        conn.close()

        print(">>> [L] Datos cargados exitosamente")

    def run(self):
        print("Extrayendo datos...")
        data_ibm = self.extract('IBM')
        data_aapl = self.extract('AAPL')
        data_tsla = self.extract('TSLA')
        data = pd.concat([data_ibm, data_aapl, data_tsla], axis=0)

        if data is not None:
            print("Datos extraídos exitosamente.")

            print("Transformando datos...")
            transformed_data = self.transform(data)
            print("Datos transformados exitosamente.")

            print("Cargando datos en Redshift...")
            self.load(transformed_data)
            print("Datos cargados exitosamente.")


if __name__ == "__main__":
    loader = ETL_Pandas()
    loader.run()