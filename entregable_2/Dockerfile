FROM jupyter/pyspark-notebook:2023-04-24

USER root

# Instalar las dependencias del sistema
RUN apt-get update && \
    apt-get install -y python3-venv

# Copiar el archivo requirements.txt al contenedor
COPY requirements.txt /home/coder/working_dir/requirements.txt

# Instalar las dependencias dentro del entorno base del contenedor
RUN pip3 install --no-cache-dir -r /home/coder/working_dir/requirements.txt

# Cambiar al usuario 'coder'
USER coder

# Establecer el directorio de trabajo
WORKDIR /home/coder/working_dir

