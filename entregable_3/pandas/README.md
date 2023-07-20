# Entregable 3
El script de la segunda entrega debe correr en un container de Docker y estará embebido en un DAG de Airflow dentro del container.

# Desarrollo
Para el siguiente trabajo se extrajeron los datos de
[Alpha Vantage](https://www.alphavantage.co/) que proporciona datos del mercado financiero de nivel empresarial. Para poder acceder a los datos es necesario generar una API_KEY que proporciona la página de manera gratuita.

La extracción se realizó vía API y se tomaron los valores mensuales (último día de negociación de cada mes, apertura mensual, máximo mensual, mínimo mensual, cierre mensual, volumen mensual) de las acciones de IBM (IBM), APPLE (AAPL) y TESLA (TSLA), que cubre más de 20 años de datos históricos.

# Distribución de los archivos
Los archivos a tener en cuenta son:
* `docker_images/`: Contiene el Dockerfile para crear las imagen utilizada de Airflow.
* `docker-compose.yml`: Archivo de configuración de Docker Compose. Contiene la configuración del servicio de Airflow.
* `.env`: Archivo de variables de entorno. Contiene variables de conexión a Redshift y driver de Postgres.
* `dags/`: Carpeta con los archivos de los DAGs.
    * `etl_finance.py`: DAG principal que ejecuta el pipeline de extracción, transformación y carga de datos de usuarios.
* `logs/`: Carpeta con los archivos de logs de Airflow.
* `plugins/`: Carpeta con los plugins de Airflow.
* `postgres_data/`: Carpeta con los datos de Postgres.
* `scripts/`: Carpeta con los scripts de Python.
    * `__init__.py`: Archivo para definir que es un módulo.
    * `modules.py`: Archivo que contiene las funciones de extracción, transformación y carga de datos.

# Pasos para ejecutar el ejemplo
1. Posicionarse en la carpeta `entregable_3/pandas`. A esta altura debería ver el archivo `docker-compose.yml`.
2. Crear las siguientes carpetas a la misma altura del `docker-compose.yml`.
```bash
mkdir -p dags,logs,plugins,postgres_data,scripts
```
3. Crear un archivo con variables de entorno llamado `.env` ubicado a la misma altura que el `docker-compose.yml`. Cuyo contenido sea:
```bash
REDSHIFT_HOST=...
REDSHIFT_PORT=5439
REDSHIFT_DB=...
REDSHIFT_USER=...
REDSHIFT_SCHEMA=...
REDSHIFT_PASSWORD=...
API_KEY=
REDSHIFT_URL="jdbc:postgresql://${REDSHIFT_HOST}:${REDSHIFT_PORT}/${REDSHIFT_DB}?user=${REDSHIFT_USER}&password=${REDSHIFT_PASSWORD}"
DRIVER_PATH=/tmp/drivers/postgresql-42.5.2.jar
```
4. Construir la imagen de Airflow en el directorio `docker_images/airflow`.
```bash
docker build -t lucastrubiano/airflow:airflow_2_6_2 .
```
5. Ejecutar el siguiente comando para levantar el servicio de Airflow.
```bash
docker-compose up --build
```
6. Una vez que los servicios estén levantados, ingresar a Airflow en `http://localhost:8080/`.
7. En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Redshift:
    * Conn Id: `redshift_default`
    * Conn Type: `Amazon Redshift`
    * Host: `host de redshift`
    * Database: `base de datos de redshift`
    * Schema: `esquema de redshift`
    * User: `usuario de redshift`
    * Password: `contraseña de redshift`
    * Port: `5439`
8. Ejecutar el DAG `etl_finance`.