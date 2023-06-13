import os
import psycopg2
import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col

def extract_data(symbol):
    try:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={symbol}&apikey={env["API_KEY"]}'
        response = requests.get(url)
        json_data = response.json()

        data_list = []
        for date, values_dict in json_data["Monthly Time Series"].items():
            data = (date, values_dict["1. open"], values_dict["2. high"], values_dict["3. low"], values_dict["4. close"], values_dict["5. volume"])
            data_list.append(data)

        # Crear el DataFrame con todos los datos
        df = spark.createDataFrame(data_list, ["date_from", "1. open", "2. high", "3. low", "4. close", "5. volume"])
        df = df.withColumn("symbol", lit(symbol))
        return df

    except requests.exceptions.RequestException as e:
        print(f"Error de solicitud: {e}")
        return None
    

def transform_data(data):
    '''Se verifica si el DataFrame tiene duplicados'''
    total_rows = data.count()
    distinct_rows = data.dropDuplicates().count()

    # Compara la cantidad de filas antes y después de eliminar los duplicados
    if total_rows == distinct_rows:
        print("El DataFrame no tiene duplicados.")
    else:
        print("El DataFrame tiene duplicados.")

def load_data(data):
    env = os.environ

    # Connect to Redshift using psycopg2
    conn = psycopg2.connect(
        host=env['AWS_REDSHIFT_HOST'],
        port=env['AWS_REDSHIFT_PORT'],
        dbname=env['AWS_REDSHIFT_DBNAME'],
        user=env['AWS_REDSHIFT_USER'],
        password=env['AWS_REDSHIFT_PASSWORD']
    )

    cursor = conn.cursor()
    cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {env['AWS_REDSHIFT_SCHEMA']}.{env['AWS_REDSHIFT_TABLE']} (
                "date_from" VARCHAR(10),
                "1. open" VARCHAR(10),
                "2. high" VARCHAR(10),
                "3. low" VARCHAR(10),
                "4. close" VARCHAR(10), 
                "5. volume" VARCHAR(10),
                symbol VARCHAR(10) distkey
            ) sortkey(date_from);
        ''')
    conn.commit()
    cursor.close()
    print("Table created!")

    data.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{env['AWS_REDSHIFT_HOST']}:{env['AWS_REDSHIFT_PORT']}/{env['AWS_REDSHIFT_DBNAME']}") \
        .option("dbtable", f"{env['AWS_REDSHIFT_SCHEMA']}.{env['AWS_REDSHIFT_TABLE']}") \
        .option("user", env['AWS_REDSHIFT_USER']) \
        .option("password", env['AWS_REDSHIFT_PASSWORD']) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

if __name__ == "__main__":
    # Postgres and Redshift JDBCs
    driver_path = "/home/coder/working_dir/driver_jdbc/postgresql-42.2.27.jre7.jar"

    os.environ['PYSPARK_SUBMIT_ARGS'] = f'--driver-class-path {driver_path} --jars {driver_path} pyspark-shell'
    os.environ['SPARK_CLASSPATH'] = driver_path

    # Create SparkSession 
    spark = SparkSession.builder \
            .master("local") \
            .appName("Conexion entre Pyspark y Redshift") \
            .config("spark.jars", driver_path) \
            .config("spark.executor.extraClassPath", driver_path) \
            .getOrCreate()
    
    print("Extrayendo datos...")
    data_ibm = extract_data('IBM')
    data_aapl = extract_data('AAPL')
    data_tsla = extract_data('TSLA')
    data = data_ibm.union(data_aapl).union(data_tsla)

    if data is not None:
        print("Datos extraídos exitosamente.")

        print("Transformando datos...")
        transformed_data = transform_data(data)
        print("Datos transformados exitosamente.")

        print("Cargando datos en Redshift...")
        load_data(transformed_data)
        print("Datos cargados exitosamente.")