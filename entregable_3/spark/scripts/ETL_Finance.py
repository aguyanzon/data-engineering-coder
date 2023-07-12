# Este script está pensado para correr en Spark y hacer el proceso de ETL de la tabla users

import requests
from datetime import datetime, timedelta
from os import environ as env

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import concat, col, lit, when, expr, to_date, max, lag


from commons import ETL_Spark

class ETL_Finance(ETL_Spark):
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")

    def run(self):
        process_date = datetime.now().strftime("%Y-%m-%d")
        self.execute(process_date)

    def extract(self, symbol):
        """
        Extrae datos de la API
        """
        print(">>> [E] Extrayendo datos de la API...")

        try:
            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={symbol}&apikey={env["API_KEY"]}'
            response = requests.get(url)
            json_data = response.json()

            data_list = []
            for date, values_dict in json_data["Monthly Time Series"].items():
                data = (date, values_dict["1. open"], values_dict["2. high"], values_dict["3. low"], values_dict["4. close"], values_dict["5. volume"])
                data_list.append(data)

            # Crear el DataFrame con todos los datos
            df = self.spark.createDataFrame(data_list, ["date_from", "1. open", "2. high", "3. low", "4. close", "5. volume"])
            df = df.withColumn("symbol", lit(symbol))

            return df

        except requests.exceptions.RequestException as e:
            print(f"Error de solicitud: {e}")
            return None
    
    
    def union_data(self):
        print(">>> [E] Concatenando DataFrames...")

        data_ibm = self.extract('IBM')
        data_aapl = self.extract('AAPL')
        data_tsla = self.extract('TSLA')
        data = data_ibm.union(data_aapl).union(data_tsla)

        return data


    def transform(self, df_original):
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")

        total_rows = df_original.count()
        distinct_rows = df_original.dropDuplicates().count()

        # Compara la cantidad de filas antes y después de eliminar los duplicados
        if total_rows == distinct_rows:
            print("El DataFrame no tiene duplicados.")
        else:
            print("El DataFrame tiene duplicados.")

        window_spec = Window.partitionBy('symbol').orderBy('date_from')
        df_original = df_original.withColumn('monthly variation (%)', (col('`4. close`') - lag('`4. close`').over(window_spec)) / lag('`4. close`').over(window_spec) * 100)

        return df_original


    def load(self, df_final):
        """
        Carga los datos transformados en Redshift
        """
        print(">>> [L] Cargando datos en Redshift...")

        # add process_date column
        df_final = df_final.withColumn("process_date", lit(self.process_date))

        df_final.write \
            .format("jdbc") \
            .option("url", env['REDSHIFT_URL']) \
            .option("dbtable", f"{env['REDSHIFT_SCHEMA']}.finance") \
            .option("user", env['REDSHIFT_USER']) \
            .option("password", env['REDSHIFT_PASSWORD']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(">>> [L] Datos cargados exitosamente")


if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_Finance()
    etl.run()
