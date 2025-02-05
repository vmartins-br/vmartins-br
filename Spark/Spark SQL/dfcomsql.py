from pyspark.sql import functions as Func
from pyspark.sql.functions import *

# Consulta SQL para selecionar todos os dados da tabela "Despachantes"
spark.sql("select * from Despachantes").show()

# Consulta SQL para selecionar as colunas "nome" e "vendas" da tabela "Despachantes"
spark.sql("select nome, vendas from Despachantes").show()

# Equivalente na API do PySpark:
# Seleciona as colunas "nome" e "vendas" do DataFrame "despachantes"
despachantes.select("nome", "vendas").show()

# Consulta SQL para selecionar "nome" e "vendas" onde as vendas são maiores que 20
spark.sql("select nome, vendas from Despachantes where vendas > 20").show()

# Equivalente na API do PySpark:
# Filtra as colunas "nome" e "vendas" onde as vendas são maiores que 20
despachantes.select("nome", "vendas").where(Func.col("vendas") > 20).show()

# Consulta SQL para agrupar por "cidade", somar as vendas e ordenar de forma decrescente
spark.sql("select cidade, sum(vendas) from Despachantes group by cidade order by 2 desc").show()

# Equivalente na API do PySpark:
# Agrupa por "cidade", soma as vendas e ordena de forma decrescente
despachantes.groupBy("cidade").agg(Func.sum("vendas")).orderBy(Func.col("sum(vendas)").desc()).show()