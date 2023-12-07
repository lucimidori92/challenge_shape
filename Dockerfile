# Use a imagem base do Apache Spark com Python
FROM apache/spark-py:latest

# Define o diretório de trabalho dentro do contêiner
WORKDIR /home

USER root
RUN chown -R 185:0 /home
USER 185

# Copia os arquivos necessários para o diretório de trabalho do contêiner
COPY main.py /home/main.py
COPY . .

# Define o comando padrão para ser executado ao iniciar o contêiner
CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "/home/main.py"]
