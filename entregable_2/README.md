# Entregable 2
## Consigna
El script del entregable 1 deberá adaptar datos leídos de la API y cargarlos en la tabla creada en la pre-entrega anterior en Redshift.

## Desarrollo
Para el siguiente trabajo se extrajeron los datos de
[Alpha Vantage](https://www.alphavantage.co/) que proporciona datos del mercado financiero de nivel empresarial. Para poder acceder a los datos es necesario generar una API_KEY que proporciona la página de manera gratuita.

La extracción se realizó vía API y se tomaron los valores mensuales (último día de negociación de cada mes, apertura mensual, máximo mensual, mínimo mensual, cierre mensual, volumen mensual) de las acciones de IBM (IBM), APPLE (AAPL) y TESLA (TSLA), que cubre más de 20 años de datos históricos.

En la entrega anterior, los datos fueron cargados a Redshift mediante Pandas. Para este segundo entregable se optó por utlizar Spark para la carga de datos. A su vez se agrega distkey y sortkey a la tabla y se verifica si existen datos duplicados.

Para poder ejecutar el siguiente trabajo se optó por disponiblizar dos métodos. El primero consiste en un Jupyter Notebook y el segundo un script .py.
En primer lugar se debe crear un archivo .env en la carpeta docker_shared_folder/working_dir/ que contenga los siguientes parámetros:
```
AWS_REDSHIFT_USER=
AWS_REDSHIFT_PASSWORD=
AWS_REDSHIFT_HOST=
AWS_REDSHIFT_PORT=
AWS_REDSHIFT_DBNAME=
AWS_REDSHIFT_SCHEMA=
AWS_REDSHIFT_TABLE=
API_KEY=
```
Luego debe abrirse Docker Desktop en su computadora y posicionarse en la carpeta que contiene el archivo docker-compose.

Una vez allí se deben ejecutar los siguiente comandos:
```
docker-compose up --build
```
Esto creará el container necesario para ejecutar los archivos. A su vez, se instalarán las dependencias necesarias para poder ejecutar el archivo .py

Para acceder al Notebook se debe acceder al siguiente enlace [Jupyter Notebook]( http://127.0.0.1:10003/lab?token=coder).

Por el contrario, si se decida ejecutar el archivo entregable_2_redshift.py desde la consola, se debe abrir una nueva terminal en el mismo directorio y ejecutar los siguientes comandos:
```
docker-compose exec pyspark bash
```
```
source /home/coder/working_dir/venv/bin/activate
```
```
python /home/coder/working_dir/entregable_2_redshift.py
```