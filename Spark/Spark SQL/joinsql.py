# Definição do schema para o DataFrame "reclamacoes"
# Campos:
# - idrec: Inteiro (identificador da reclamação)
# - datarec: String (data da reclamação)
# - iddesp: Inteiro (identificador do despachante)
recschema = "idrec INT, datarec STRING, iddesp INT"

# Leitura do arquivo CSV "reclamacoes.csv" para criar o DataFrame "reclamacoes"
# Parâmetros:
# - header=False: O arquivo não possui cabeçalho
# - schema=recschema: Utiliza o schema definido anteriormente
reclamacoes = spark.read.csv("/home/victor-m/download/reclamacoes.csv", header=False, schema=recschema)

# Exibição das primeiras linhas do DataFrame "reclamacoes"
reclamacoes.show()

# Gravação do DataFrame "reclamacoes" como uma tabela no Spark SQL
reclamacoes.write.saveAsTable("reclamacoes")

# Consulta SQL para selecionar todos os dados da tabela "reclamacoes"
spark.sql("select * from reclamacoes").show()

# Consulta SQL para realizar um INNER JOIN entre as tabelas "despachantes" e "reclamacoes"
# Seleciona todas as colunas de "reclamacoes" e a coluna "nome" de "despachantes"
# A junção é feita com base na igualdade entre "despachantes.id" e "reclamacoes.iddesp"
spark.sql("select reclamacoes.*, despachantes.nome from despachantes inner join reclamacoes on (despachantes.id = reclamacoes.iddesp)").show()

# Equivalente na API do PySpark:
# Realiza um INNER JOIN entre os DataFrames "despachantes" e "reclamacoes"
# Seleciona as colunas "idrec", "datarec", "iddesp" e "nome"
despachantes.join(reclamacoes, despachantes.id == reclamacoes.iddesp, "inner").select("idrec", "datarec", "iddesp", "nome").show()