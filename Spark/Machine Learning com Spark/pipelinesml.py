# Importação das bibliotecas necessárias para Machine Learning no PySpark
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

# Leitura do arquivo CSV "Carros.csv" para criar o DataFrame "Carros_temp"
# Parâmetros:
# - inferSchema=True: Infere automaticamente os tipos das colunas
# - header=True: O arquivo possui cabeçalho
# - sep=";": O separador de colunas é o ponto e vírgula
carros_temp = spark.read.csv("/home/victor-m/download/Carros.csv", inferSchema=True, header=True, sep=";")

# Seleção das colunas relevantes para o modelo de Machine Learning
carros = carros_temp.select("Consumo", "Cilindros", "Cilindradas", "HP")

# Criação de um VectorAssembler para combinar as colunas de características em uma única coluna
# Parâmetros:
# - inputCols: Lista das colunas que serão usadas como características
# - outputCol: Nome da nova coluna que conterá o vetor de características
veccaracteristicas = VectorAssembler(inputCols=["Consumo", "Cilindros", "Cilindradas"], outputCol="caracteristicas")

# Transformação do DataFrame "Carros" para incluir a coluna "caracteristicas"
vec_carrosTreino = veccaracteristicas.transform(carros)

# Criação do modelo de Regressão Linear
# Parâmetros:
# - featuresCol: Coluna com as características (vetor de características)
# - labelCol: Coluna com o valor real (HP)
reglin = LinearRegression(featuresCol="caracteristicas", labelCol="HP")

# Criação de um Pipeline para automatizar o processo de transformação e modelagem
# O Pipeline contém duas etapas:
# 1. VectorAssembler: Transforma as colunas em um vetor de características
# 2. LinearRegression: Treina o modelo de regressão linear
pipeline = Pipeline(stages=[veccaracteristicas, reglin])

# Treinamento do Pipeline
pipelineModel = pipeline.fit(carros)

# Previsão dos valores usando o modelo treinado
previsao = pipelineModel.transform(carros)

# Exibição das previsões
print("Previsões do modelo de Regressão Linear usando Pipeline:")
previsao.show()