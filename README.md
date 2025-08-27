# User Defined Function (UDF)

**Author:** Prof. Barbosa  
**Contact:** infobarbosa@gmail.com  
**Github:** [infobarbosa](https://github.com/infobarbosa)

---

#### Atenção aos custos!
**Atenção!** Ao realizar os laboratórios deste módulo, lembre-se de que a execução na AWS pode gerar custos. A responsabilidade pela gestão desses custos é do aluno.

---

### Configuração Inicial

Antes de começar, prepare seu ambiente:

ATENÇÃO! Se estiver utilizando Cloud9, utilize esse [tutorial](https://github.com/infobarbosa/data-engineering-cloud9]).

1. **Instale o Java 17:**
  ```bash
  sudo apt upgrade -y && sudo apt update -y

  ```

  ```bash
  sudo apt install -y openjdk-17-jdk

  ```

2. **Crie uma pasta para o projeto:**
    ```bash
    mkdir -p data-eng-pyspark-udf/src
    mkdir -p /tmp/data/input
    mkdir -p /tmp/data/output
    
    ```
    
    ```bash
    cd data-eng-pyspark-udf
    
    ```

3. **Crie um ambiente virtual e instale as dependências:**
    ```bash
    python3 -m venv .venv
    
    ```

    ```bash
    source .venv/bin/activate
    
    ```

    ```bash
    pip install pyspark
    
    ```

4. **Baixe os datasets:**
    Execute o script para baixar os dados necessários para a pasta `data/`.
    
    **Clientes**
    ```bash
    curl -L -o /tmp/data/input/clientes.gz https://raw.githubusercontent.com/infobarbosa/dataset-json-clientes/main/data/clientes.json.gz
    
    ```

    Um olhada rápida no arquivo de clientes
    ```bash
    gunzip -c /tmp/data/input/clientes.gz | head -n 5

    ```

5. Configure o log:
  - Crie o arquivo `log4j2.properties`:
    ```bash
    touch log4j2.properties

    ```

  - Adicione o seguinte conteúdo ao arquivo `log4j2.properties`:
    
    ```
    # Define o status do logger interno do Log4j2
    status = debug# Define o appender de arquivo
    appender.file.type = File
    appender.file.name = file
    appender.file.fileName = pyspark-udf-lab.log
    appender.file.layout.type = PatternLayout
    appender.file.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

    # Adiciona o appender de arquivo ao logger raiz
    rootLogger.appenderRef.file.ref = file

    # Define o nome da configuração
    name = PySparkLogConfig

    # Configura o appender do console
    appender.console.type = Console
    appender.console.name = stderr
    appender.console.target = SYSTEM_ERR
    appender.console.layout.type = PatternLayout
    appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

    # Configura o logger raiz
    rootLogger.level = debug
    rootLogger.appenderRef.stdout.ref = stderr

    # AQUI ESTÁ A MÁGICA:
    # Sobrescrevemos o nível de log para a classe que de fato executa a UDF.
    logger.PythonUDFRunner.name = org.apache.spark.sql.execution.python
    logger.PythonUDFRunner.level = trace
    logger.PythonUDFRunner.additivity = false
    logger.PythonUDFRunner.appenderRef.stderr.ref = stderr

    ```

---

## 1. Introdução

UDFs permitem a aplicação de funções personalizadas em colunas de um DataFrame. Elas são úteis para operações complexas que não são diretamente suportadas pelas funções nativas do Spark.

## 2. Laboratório
  - Crie o arquivo `main.py`
    ```bash
    touch main.py
    
    ```

  - Adicione o seguinte conteúdo ao arquivo `main.py`:

    ```python
    from datetime import datetime
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import udf, col
    from pyspark.sql.types import IntegerType, StructType, StructField, StringType, DateType, ArrayType, LongType, BooleanType

    spark = SparkSession.builder \
        .appName("data-eng-udf-lab") \
        .getOrCreate()

    schema_clientes = StructType([
        StructField("id", LongType(), True),
        StructField("nome", StringType(), True),
        StructField("data_nasc", DateType(), True),
        StructField("cpf", StringType(), True),
        StructField("email", StringType(), True),
        StructField("interesses", ArrayType(StringType()), True)
    ])

    print("Abrindo o dataframe de clientes, deixando o Spark inferir o schema")
    clientes_df = spark.read.option("compression", "gzip").json("/tmp/data/input/clientes.gz", schema=schema_clientes)

    clientes_df.show(20, truncate=False)

    # Definindo uma UDF
    @udf(IntegerType())
    def calcular_idade(data_nasc: DateType):
        data_atual = datetime.now().date()
        idade = data_atual.year - data_nasc.year - ((data_atual.month, data_atual.day) < (data_nasc.month, data_nasc.day))
        return idade

    # Aplicando a UDF
    clientes_df = clientes_df.withColumn("idade", calcular_idade(clientes_df["data_nasc"]))

    # 1. UDF com a lógica de negócio real.
    @udf(BooleanType())
    def eh_maior_que_30(data_nasc: DateType):
        if not data_nasc:
            return False
        data_atual = datetime.now().date()
        idade = data_atual.year - data_nasc.year - ((data_atual.month, data_atual.day) < (data_nasc.month, data_nasc.day))
        return idade > 30

    # 2. Adiciona a coluna com o resultado da UDF.
    clientes_df = clientes_df.withColumn("maior_de_30", eh_maior_que_30(col("data_nasc")))

    # 3. Marca o DataFrame para ser colocado em cache.
    #    A execução ainda não aconteceu, é uma operação lazy.
    print("Marcando o DataFrame para cache...")
    clientes_df.cache()

    # 4. Executa uma ação leve (.count()) para forçar a execução do plano
    #    e a materialização do DataFrame em cache. Neste ponto, a UDF é
    #    executada para todas as linhas.
    print("Forçando a execução e o cache com a ação .count()...")
    total_clientes = clientes_df.count()
    print(f"Processamento e cache concluídos. Total de clientes: {total_clientes}")

    clientes_df = clientes_df.filter(col("maior_de_30") == True)

    # 5. Agora o DataFrame está em memória e a UDF já foi executada.
    #    Qualquer operação subsequente será muito mais rápida.
    print("Exibindo o DataFrame final a partir do cache:")
    clientes_df.show(1000, truncate=False)

    print("Parando a SparkSession")
    spark.stop()


    ```

  - Execute a aplicação:

    ```bash
    spark-submit \
    --files log4j2.properties \
    --conf "spark.sql.execution.pythonUDF.arrow.enabled=false" \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=log4j2.properties" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=log4j2.properties" \
    main.py
    ```

Os componentes-chave a observarmos nos logs:

   1. Regra de Otimização `ExtractPythonUDFs`: O Spark primeiro aplica esta regra ao plano lógico. Ela identifica a UDF Python e a extrai para uma execução separada.
   2. Operador Físico `BatchEvalPython`: Após a otimização, o Spark insere este operador no plano físico. Ele é o responsável por:
       * Serializar os dados do DataFrame da JVM.
       * Enviá-los para um processo de trabalho Python.
       * Executar a função Python (sua UDF) nesse processo.
       * Receber os resultados de volta.
       * Desserializar os resultados na JVM.

---

## 4. Laboratório 2: Analisando Transações com Potencial de Fraude

Você recebeu um conjunto de dados de pedidos de uma plataforma de e-commerce que contém informações sobre cada pedido, incluindo seu status e se foi marcado como fraude ou não.

Cada registro no conjunto de dados é representado pelo seguinte formato JSON:
```json
[
    {"id_pedido": "89b90ce1-c12b-440e-a7db-ca43a4dd87ba", "status": true, "fraude": false},
    {"id_pedido": "aa18591a-e54d-4f84-bc20-ebbb653e016d", "status": false, "fraude": false},
    ...
]
```

### Objetivo:
O desafio é implementar uma UDF (User Defined Function) que verificará se um pedido é considerado de **alto risco**. Um pedido é considerado de alto risco se:
- O status for `false` (indicando que o pedido não foi processado corretamente).
- A flag de fraude for `true` (indicando que o pedido foi marcado como potencialmente fraudulento).

### Passos:
1. **Carregar os dados JSON no PySpark**: Carregue o conjunto de dados JSON fornecido utilizando a API de DataFrame do PySpark. Faça o clone do seguinte repositório:
```sh
git clone https://github.com/infobarbosa/dataset-json-pagamentos

```

2. **Criar a UDF**: Implemente uma função que será usada como UDF para identificar se um pedido é de alto risco.
3. **Aplicar a UDF no DataFrame**: Utilize a UDF para criar uma nova coluna no DataFrame chamada `risco_alto`, que será `True` se o pedido for de alto risco e `False` caso contrário.
4. **Filtrar pedidos de alto risco**: Filtre e exiba apenas os pedidos que são considerados de alto risco.

### Dicas:
- Você pode usar o método `.withColumn()` para adicionar uma nova coluna ao DataFrame.

### Exemplo de UDF:

Aqui está um exemplo básico de como criar e usar uma UDF:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType

# Defina a função de alto risco
def pedido_alto_risco(LISTA_DE_PARAMETROS...):
    ADICIONE_SUA_LOGICA_AQUI

# Registre a função como UDF
pedido_alto_risco_udf = REGISTRE_A_UDF_AQUI

# Inicializando a sessão Spark
spark = SparkSession.builder.appName("DesafioUDF").getOrCreate()

# Carregar o JSON em um DataFrame
df = spark.read.json("./dataset-json-pagamentos/pagamentos.json")
df.show(truncate=False)

# Aplicar a UDF para criar uma nova coluna 'risco_alto'
df_com_risco = df.withColumn("risco_alto", APLIQUE_A_UDF_AQUI)

# Filtrar os pedidos de alto risco
df_pedidos_risco_alto = df_com_risco.filter( ACRESCENTE_AQUI_A_LOGICA_DE_FILTRO )

# Exibir os resultados
df_pedidos_risco_alto.show()

```

---

## 5. Visão interna
1. Crie o diretório de dados
```bash
mkdir -p data/input

```

1. Baixe o dataset:
```bash
curl -L -o ./data/input/clientes.gz https://raw.githubusercontent.com/infobarbosa/dataset-json-clientes/main/data/clientes.json.gz

```

```bash
gunzip -c data/input/clientes.gz  | head

```

2. O código:
```bash
touch main.py

```

```python

# src/main.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                               ArrayType, DateType, FloatType, TimestampType)
from pyspark.sql.functions import udf
from datetime import datetime

spark = SparkSession.builder.appName("data-eng-pyspark-udf").getOrCreate()

print("Definindo schema do dataframe de clientes")
schema_clientes = StructType([
    StructField("id", LongType(), True),
    StructField("nome", StringType(), True),
    StructField("data_nasc", DateType(), True),
    StructField("cpf", StringType(), True),
    StructField("email", StringType(), True),
    StructField("interesses", ArrayType(StringType()), True)
])
print("Abrindo o dataframe de clientes")
clientes_df = spark.read.option("compression", "gzip").json("data/input/clientes.gz", schema=schema_clientes)

clientes_df.show(5, truncate=False)

# Definindo a UDF para calcular a idade e criar a saudação
@udf(StringType())
def saudacao_personalizada(nome, data_nascimento):
    # A coluna 'data_nascimento' já é um objeto date, não precisa de conversão.
    # Obtendo a data atual
    data_atual = datetime.now().date()
    # Calculando a idade
    idade = data_atual.year - data_nascimento.year - ((data_atual.month, data_atual.day) < (data_nascimento.month, data_nascimento.day))
    # Criando a saudação
    saudacao = f"Olá, {nome}! Você tem {idade} anos."
    return saudacao

# Aplicando a UDF ao DataFrame
clientes_df = clientes_df.withColumn("saudacao", saudacao_personalizada(clientes_df.nome, clientes_df.data_nasc))

# Exibindo o DataFrame resultante
# Usamos foreach para forçar a execução em todos os dados, o que ativará o logger
clientes_df.select("nome", "data_nasc", "saudacao").foreach(lambda row: None)
clientes_df.select("nome", "data_nasc", "saudacao").show(truncate=False)

spark.stop()

```

```bash
touch log4j.properties

```

```
# Define o status do logger interno do Log4j2
status = warn

# Define o nome da configuração
name = PySparkLogConfig

# Configura o appender do console
appender.console.type = Console
appender.console.name = stderr
appender.console.target = SYSTEM_ERR
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# Configura o logger raiz
rootLogger.level = warn
rootLogger.appenderRef.stdout.ref = stderr

# AQUI ESTÁ A MÁGICA:
# Sobrescrevemos o nível de log para a classe que de fato executa a UDF.
logger.PythonUDFRunner.name = org.apache.spark.sql.execution.python.PythonUDFWithNamedArgumentsRunner
logger.PythonUDFRunner.level = debug
logger.PythonUDFRunner.additivity = false
logger.PythonUDFRunner.appenderRef.stderr.ref = stderr

```


```bash
spark-submit \
  --master "local[2]" \
  --files log4j2.properties \
  --conf "spark.sql.execution.pythonUDF.arrow.enabled=false" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=log4j2.properties" \
  main.py
```

---

## 6. Parabéns!
Parabéns! Nesse módulo você aprendeu sobre UDFs e como aplicar esse conhecimento em desafios práticos. <br> 
Continue assim e bons estudos!

## 7. Destruição dos recursos
Para evitar custos desnecessários, lembre-se de destruir os recursos criados durante este módulo:
- Exclua quaisquer instâncias do AWS Cloud9 que não sejam mais necessárias.
- Remova dados temporários ou resultados intermediários armazenados no S3.

