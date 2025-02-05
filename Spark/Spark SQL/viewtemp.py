# Criação do DataFrame "despachantes"
# Supondo que os dados estejam em um arquivo CSV com cabeçalho
despachantes = spark.read.csv("/home/victor-m/download/despachantes.csv", header=True, inferSchema=True)

# Exibição das primeiras linhas do DataFrame "despachantes" para verificação
print("Dados do DataFrame 'despachantes':")
despachantes.show()

# Criação de uma view temporária chamada "Despachantes_view1" a partir do DataFrame "despachantes"
# Uma view temporária é visível apenas na sessão atual do Spark
despachantes.createOrReplaceTempView("Despachantes_view1")

# Consulta SQL para selecionar todos os dados da view temporária "Despachantes_view1"
print("Dados da view temporária 'Despachantes_view1':")
spark.sql("select * from Despachantes_view1").show()

# Criação de uma view global chamada "Despachantes_view2" a partir do DataFrame "despachantes"
# Uma view global é visível em todas as sessões do Spark, mas precisa ser acessada com o prefixo "global_temp"
despachantes.createOrReplaceGlobalTempView("Despachantes_view2")

# Consulta SQL para selecionar todos os dados da view global "Despachantes_view2"
# Note o uso do prefixo "global_temp" para acessar a view global
print("Dados da view global 'Despachantes_view2':")
spark.sql("select * from global_temp.Despachantes_view2").show()

# Criação de uma view temporária chamada "DESP_VIEW" usando SQL
# A view é criada diretamente a partir de uma consulta SQL
spark.sql("CREATE OR REPLACE TEMP VIEW DESP_VIEW AS SELECT * FROM despachantes")

# Consulta SQL para selecionar todos os dados da view temporária "DESP_VIEW"
print("Dados da view temporária 'DESP_VIEW':")
spark.sql("select * from DESP_VIEW").show()

# Criação de uma view global chamada "DESP_VIEW2" usando SQL
# A view é criada diretamente a partir de uma consulta SQL e é acessível globalmente
spark.sql("CREATE OR REPLACE GLOBAL TEMP VIEW DESP_VIEW2 AS SELECT * FROM despachantes")

# Consulta SQL para selecionar todos os dados da view global "DESP_VIEW2"
# Note o uso do prefixo "global_temp" para acessar a view global
print("Dados da view global 'DESP_VIEW2':")
spark.sql("select * from global_temp.DESP_VIEW2").show()