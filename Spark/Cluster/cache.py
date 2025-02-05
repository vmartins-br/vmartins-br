# Importação das bibliotecas necessárias
from pyspark.sql.types import *
from pyspark import StorageLevel

# Leitura do arquivo CSV "despachantes.csv" para criar o DataFrame "despachantes"
# Parâmetros:
# - header=False: O arquivo não possui cabeçalho
# - inferSchema=True: Infere automaticamente os tipos das colunas
despachantes = spark.read.csv("/home/victor-m/download/despachantes.csv", header=False, inferSchema=True)

# Exibição das primeiras 2 linhas do DataFrame "despachantes"
print("Primeiras 2 linhas do DataFrame 'despachantes':")
despachantes.show(2)

# Cache do DataFrame "despachantes" para otimizar o desempenho
# O cache armazena os dados na memória (ou em disco, se necessário) para acesso rápido
despachantes.cache()

# Persistência do DataFrame "despachantes" com nível de armazenamento MEMORY_AND_DISK
# Isso garante que os dados sejam armazenados na memória, mas, se não houver espaço suficiente, serão armazenados em disco
# O método .count() é chamado para forçar a execução da persistência
despachantes.persist(StorageLevel.MEMORY_AND_DISK).count()

# Exibição das primeiras 3 linhas do DataFrame "despachantes" após a persistência
print("Primeiras 3 linhas do DataFrame 'despachantes' após persistência:")
despachantes.show(3)

# Gravação do DataFrame "despachantes" como uma tabela particionada
# A partição é feita pela coluna "_c3" (supondo que seja uma coluna relevante para particionamento)
despachantes.write.partitionBy("_c3").saveAsTable("desp_UI")

# Consulta SQL para selecionar dados da tabela "desp_UI" onde a coluna "_c0" é igual a 2
print("Resultado da consulta SQL na tabela 'desp_UI':")
spark.sql("select * from desp_UI where _c0 = 2").show()