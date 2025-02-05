# Importação da biblioteca necessária para criar uma sessão do Spark
from pyspark.sql import SparkSession

# Bloco principal do programa
if __name__ == "__main__":
    # Inicialização da sessão do Spark
    spark = SparkSession.builder.appName("Streaming").getOrCreate()

    # Definição do schema para os dados JSON
    # Campos:
    # - nome: String (nome do usuário)
    # - postagem: String (conteúdo da postagem)
    # - data: Inteiro (timestamp da postagem)
    jsonschema = "nome STRING, postagem STRING, data INT"

    # Leitura dos dados de streaming a partir de um diretório de arquivos JSON
    # Parâmetros:
    # - path: Caminho do diretório onde os arquivos JSON estão sendo gravados
    # - schema: Schema dos dados JSON
    df = spark.readStream.json("/home/victor-m/testestream/", schema=jsonschema)

    # Definição do diretório para checkpoint (necessário para streaming)
    diretorio = "/home/victor-m/temp"

    # Configuração do streaming para processar os dados em batch
    # Parâmetros:
    # - foreachBatch: Função que será chamada para processar cada batch de dados
    # - outputMode: Modo de saída (append para adicionar novos dados)
    # - trigger: Intervalo de tempo para processamento (5 segundos)
    # - checkpointLocation: Diretório para checkpoint (necessário para garantir a consistência do streaming)
    Stcal = df.writeStream \
        .foreachBatch(atualizapostgres) \
        .outputMode("append") \
        .trigger(processingTime="5 second") \
        .option("checkpointLocation", diretorio) \
        .start()

    # Aguarda o término do streaming (execução contínua)
    Stcal.awaitTermination()