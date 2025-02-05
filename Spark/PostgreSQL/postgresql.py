from pyspark.sql import SparkSession

# Leitura da tabela "Vendas" do banco de dados PostgreSQL
# Parâmetros:
# - url: URL de conexão com o banco de dados PostgreSQL
# - dbtable: Nome da tabela a ser lida
# - user: Nome do usuário do banco de dados
# - password: Senha do usuário
# - driver: Driver JDBC para PostgreSQL

resumo = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vendas") \
    .option("dbtable", "Vendas") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Exibição das primeiras linhas da tabela "Vendas" para inspeção dos dados
print("Dados da tabela 'Vendas':")

resumo.show()

# Leitura da tabela "Clientes" do banco de dados PostgreSQL
# Utilizamos os mesmos parâmetros de conexão, apenas alterando o nome da tabela

clientes = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vendas") \
    .option("dbtable", "Clientes") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Exibição das primeiras linhas da tabela "Clientes" para inspeção dos dados
print("Dados da tabela 'Clientes':")

clientes.show()

# Seleção das colunas "data" e "total" da tabela "Vendas"
# Criamos um novo DataFrame contendo apenas essas colunas

vendadata = resumo.select("data", "total")

# Exibição das primeiras linhas do DataFrame "vendadata" para inspeção dos dados

print("Dados selecionados (data e total) da tabela 'Vendas':")
vendadata.show()

# Gravação do DataFrame "vendadata" em uma nova tabela no PostgreSQL
# Parâmetros:
# - url: URL de conexão com o banco de dados PostgreSQL
# - dbtable: Nome da nova tabela a ser criada
# - user: Nome do usuário do banco de dados
# - password: Senha do usuário
# - driver: Driver JDBC para PostgreSQL

vendadata.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vendas") \
    .option("dbtable", "Vendadata") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .save()

print("Dados gravados com sucesso na tabela 'Vendadata' do PostgreSQL.")

# Encerramento da sessão do Spark para liberar recursos
spark.stop()