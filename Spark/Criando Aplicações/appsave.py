# Importação das bibliotecas necessárias
import sys
import getopt
from pyspark.sql import SparkSession

# Bloco principal do programa
if __name__ == "__main__":
    # Inicialização da sessão do Spark
    spark = SparkSession.builder.appName("Exemplo").getOrCreate()

    # Leitura dos argumentos passados na linha de comando
    # Os argumentos esperados são:
    # -t: Formato de saída (ex: csv, parquet, json)
    # -i: Caminho do arquivo de entrada
    # -o: Diretório de saída
    opts, args = getopt.getopt(sys.argv[1:], "t:i:o:")

    # Inicialização das variáveis para armazenar os valores dos argumentos
    formato, infile, outdir = "", "", ""

    # Processamento dos argumentos
    for opt, arg in opts:
        if opt == "-t":
            formato = arg  # Define o formato de saída
        elif opt == "-i":
            infile = arg  # Define o caminho do arquivo de entrada
        elif opt == "-o":
            outdir = arg  # Define o diretório de saída

    # Leitura do arquivo de entrada usando o Spark
    # Parâmetros:
    # - header=False: O arquivo não possui cabeçalho
    # - inferSchema=True: Infere automaticamente os tipos das colunas
    dados = spark.read.csv(infile, header=False, inferSchema=True)

    # Gravação dos dados no formato especificado no diretório de saída
    dados.write.format(formato).save(outdir)

    # Encerramento da sessão do Spark
    spark.stop()

    