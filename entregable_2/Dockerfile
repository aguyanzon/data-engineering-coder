FROM jupyter/pyspark-notebook:2023-04-24

USER root

# Instalar el entorno virtual y las dependencias
RUN apt-get update && \
    apt-get install -y python3-venv

# Crear y activar el entorno virtual
RUN python3 -m venv /home/coder/working_dir/venv
ENV PATH="/home/coder/working_dir/venv/bin:$PATH"

# Instalar las dependencias dentro del entorno virtual
RUN /home/coder/working_dir/venv/bin/pip install --no-cache-dir -r /home/coder/working_dir/requirements.txt

# Cambiar al usuario 'coder'
USER coder

# Establecer el directorio de trabajo
WORKDIR /home/coder/working_dir
