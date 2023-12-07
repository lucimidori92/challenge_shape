from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank



# Restante do seu código aqui...


# Configuração do Spark e desativa todos os logs
spark = SparkSession.builder \
    .appName("CHALLENGE_Luciana") \
    .config("spark.logConf", "false") \
    .getOrCreate()

# Define o nível de log para OFF em todas as mensagens
spark.sparkContext.setLogLevel("OFF")

# Função para extração dos 3 arquivos disponibilizados nesse desafio
def extracao_arquivos(path, format):
    if format == 'text':
        df = spark.read.option("encoding", "UTF-8").format(format).load(path)
        df = (df
                  .withColumn("timestamp", regexp_extract("value", r"\[(.*?)\]", 1))
                    .withColumn("log_type", regexp_extract("value", r"ERROR|WARNING", 0))
                    .withColumn("sensor_id", regexp_extract("value", r"sensor\[(.*?)\]", 1))
                    .withColumn("temperature", regexp_extract("value", r"temperature\s*(-?\d+\.\d+)", 1))
                    .withColumn("vibration", regexp_extract("value", r"vibration\s*(.+)\)", 1))
                    .drop('value')
                  )
    elif format == 'csv':
        df = spark.read.option("header", "true").option("encoding", "UTF-8").format(format).load(path)
    elif format == 'json':
        df = spark.read.option("multiline", "true").option("encoding", "UTF-8").format(format).load(path)
    else:
        print(f"{format} e o {path} não está correto. Tente novamente.")

    return df


print(100*'=')
print("Análise de erros de equipamentos e sensores - Challenge Luciana Murata")

# Variáveis contendo os caminhos dos arquivos extraídos e analisados. Sendo eles em formato: txt, csv e json.
path_txt = '/home/equpment_failure_sensors.txt'
path_csv = '/home/equipment_sensors.csv'
path_json = '/home/equipment.json'

# Código em que faz a leitura do arquivo txt, extração dos dados, separação dos dados em colunas e seus tratamentos.
df_equipment_failure = extracao_arquivos(path_txt,"text")

# Código em que faz a leitura do arquivo csv, extração dos dados em um dataframe.
df_equipment_sensors = extracao_arquivos(path_csv,"csv")

# Código em que faz a leitura do arquivo json, extração dos dados em um dataframe.
df_equipment = extracao_arquivos(path_json,"json")

# 1. Total equipment failures that happened?
df_total= df_equipment_failure.count()
df_total_failure = df_equipment_failure.filter("log_type = 'ERROR'").count()

# Print do resultado
print(60*"-")
print(f"1 - Total de falhas dos equipamentos: {df_total_failure} em um total de {df_total}")
print(60*"-")

# 2. Which equipment name had most failures?
# Join entre as tabelas equipment_sensors, log e equipment
df_equipment_failure = (df_equipment_failure.join(df_equipment_sensors, df_equipment_failure.sensor_id == df_equipment_sensors.sensor_id , "left")
                        .join(df_equipment, df_equipment_sensors.equipment_id == df_equipment.equipment_id, "left")
                        .select("log_type", df_equipment_sensors.equipment_id, "name", "group_name", df_equipment_failure.sensor_id))

# Filtragem de log_type 'ERROR' e contagem de falhas
df_most_failures = (df_equipment_failure
                    .filter(col("log_type") == "ERROR")
                    .groupby("equipment_id", "name")  # Incluído "name" na contagem
                    .agg(count("*").alias("count_failures"))
)

# Encontra o máximo de contagens de falhas
df_max_failures = df_most_failures.agg(max("count_failures")).first()["max(count_failures)"]

# Filtra os equipamentos com o número máximo de falhas
df_most_failures = df_most_failures.filter(col("count_failures") == df_max_failures)
name_equipment = df_most_failures.first()["name"]

# Print do resultado
print(f"2 - O equipamento com maior numero de falhas: {name_equipment}")
print(60*"-")

# 3. Average amount of failures across equipment group, ordered by the number of failures in ascending order?
df_count_failures = (df_equipment_failure
                    .filter(col("log_type") == "ERROR")
                    .groupby("group_name")
                    .agg(count("*").alias("count_failures"))
                )

df_avg_failures = (df_count_failures
                    .groupby("group_name")
                    .agg(avg("count_failures").alias("avg_failures"))
                    .orderBy("avg_failures") # Como é uma ordenação asc, não precisa definir, pois é uma funcionalidade default
                )

# Print do resultado
print("3 - Media de falhas por grupo de equipamentos, ordenado pelo numero de falhas de forma asc: ")

df_avg_failures.show()

print(60*"-")

# 4. Rank the sensors which present the most number of errors by equipment name in an equipment group.
df_group_sensors = (df_equipment_failure.filter("log_type = 'ERROR'")
                                        .groupby("name", "group_name", "sensor_id")
                                        .agg(count("*").alias("count_errors"))
                )

# Variável que armazena a função de particionamento por nome e grupo de equipamentos, ordenado pelo count_erros de forma descending
window_spec = Window.partitionBy("name", "group_name").orderBy(col("count_errors").desc())

df_rank_sensors = (df_group_sensors
                   .withColumn("rank", dense_rank().over(window_spec)) # Cria uma coluna com o rank (utilizando a função dense_rank() para ranquear os sensores com maiores numero de erros)
                    .filter(col("rank") == 1)  # Seleciona apenas os sensores top ranks em cada grupo e nome de equipamentos
                    .select("group_name", "name", "sensor_id", "count_errors", "rank")
                    .orderBy("group_name", "name", "rank")
)

# Print do resultado
print("4 - Ranking de sensores que apresentam o maior numero de erros por nome e grupo de equipamentos:")
df_rank_sensors.show()

print("Fim da análise! Obrigada pela atenção.")
print(100*'=')
