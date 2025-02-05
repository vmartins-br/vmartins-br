# Comando para acessar o shell do MongoDB
mongosh

# Importação de dados para o MongoDB a partir de um arquivo JSON
# Parâmetros:
# --db: Nome do banco de dados (posts)
# --collection: Nome da coleção (post)
# --legacy: Modo de compatibilidade para versões mais antigas
# --file: Caminho do arquivo JSON a ser importado
mongoimport --db posts --collection post --legacy --file /home/victor-m/download/mongo/posts.json

# Seleciona o banco de dados "posts" no MongoDB
use posts

# Consulta todos os documentos da coleção "post"
db.post.find()  # Retorna todos os documentos

# Consulta todos os documentos da coleção "post" de forma formatada (pretty)
db.post.find().pretty()

# Inicialização do PySpark com o conector do MongoDB
# O pacote necessário para o conector MongoDB é especificado aqui
pyspark --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1

# Leitura dos dados da coleção "post" do banco de dados "posts" no MongoDB
# Parâmetros:
# - uri: URL de conexão com o MongoDB e especificação da coleção
posts = spark.read.format("mongo").option("uri", "mongodb://127.0.0.1/posts.post").load()

# Exibição das primeiras linhas dos dados lidos do MongoDB
posts.show()

# Gravação dos dados em uma nova coleção chamada "post" no banco de dados "posts2"
# Parâmetros:
# - uri: URL de conexão com o MongoDB e especificação da nova coleção
posts.write.format("mongo").option("uri", "mongodb://127.0.0.1/posts2.post").save()

# Verificação dos dados gravados no MongoDB
# Comandos executados no shell do MongoDB:
show dbs  # Lista todos os bancos de dados
use posts2  # Seleciona o banco de dados "posts2"
db.post.find()  # Consulta todos os documentos da coleção "post"