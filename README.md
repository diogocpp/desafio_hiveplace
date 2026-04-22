# HivePlace — Data Engineering Challenge

Pipeline batch em **PySpark** para processamento e análise do dataset **NYC Taxi Trip Duration**, com foco em:

- qualidade e padronização dos dados
- processamento distribuído
- saídas analíticas em **Parquet**
- validação de integridade ao final da execução

---

## 1. Instruções de Execução

### 1.1. Pré-requisitos

- **Python** 3.8+
- **Java** 11
- **Apache Spark** 3.5.3 (utilizado no desenvolvimento)
- Ambiente configurado para rodar `spark-submit` na linha de comando

### 1.2. Estrutura de pastas

```text
hiveplace-de-challenge/
├── data/
│   ├── raw/                    # CSV bruto (não versionado)
│   └── output/
│       ├── clean/              # saída tratada (Parquet)
│       └── agg/                # saída agregada (Parquet)
├── reports/                    # relatórios JSON + breakdown de descartes
└── src/
    └── jobs/
        └── process.py          # pipeline principal
```

Coloque o arquivo `input.csv` em `data/raw/`.

> Dataset de teste: https://www.kaggle.com/datasets/parisrohan/nyc-taxi-trip-duration?resource=download 

### 1.3. Execução do pipeline

A partir da raiz do projeto:

```bash
spark-submit \
  --master local[*] \
  src/jobs/process.py \
  --input data/raw/input.csv \
  --output-clean data/output/clean \
  --output-agg data/output/agg
```

Parâmetros:

- `--input`         caminho do CSV bruto
- `--output-clean`  diretório da camada tratada (Parquet)
- `--output-agg`    diretório da camada agregada (Parquet)
- `--output-report` diretório onde o relatório JSON será salvo (default: `reports`)

---

## 2. Decisões Técnicas

### 2.1. Leitura e schema explícito

- A leitura do CSV é feita com **PySpark**, usando `spark.read.csv`.
- O schema é declarado explicitamente em `RAW_SCHEMA`:

  ```python
  StructField("pickup_datetime",    TimestampType(), True),
  StructField("dropoff_datetime",   TimestampType(), True),
  StructField("passenger_count",    IntegerType(),   True),
  StructField("pickup_longitude",   DoubleType(),    True),
  ...
  ```

- Isso evita inferência automática, garantindo consistência de tipos entre execuções e ambientes.

### 2.2. Padronização de nomes (convenção de prefixos)

Após a leitura, todas as colunas são padronizadas com uma convenção de nomenclatura baseada em **prefixos semânticos**, por exemplo:

| Prefixo | Tipo                  | Exemplo                     |
|---------|-----------------------|-----------------------------|
| `id`    | Identificador         | `id_corrida`                |
| `cd`    | Código                | `cd_fornecedor`             |
| `ts`    | Timestamp             | `ts_embarque`               |
| `dt`    | Data                  | `dt_embarque`               |
| `hh`    | Hora (0–23)           | `hh_embarque`               |
| `qt`    | Quantidade            | `qt_passageiro`             |
| `te`    | Tempo (duração)       | `te_duracao_segundo`        |
| `lt`    | Latitude              | `lt_embarque`               |
| `lg`    | Longitude             | `lg_embarque`               |
| `in`    | Indicador booleano    | `in_corrida_longa`          |
| `vl`    | Valor decimal         | `vl_media_duracao_minuto`   |
| `pe`    | Percentual            | `pe_corrida_longa`          |

Alguns exemplos de renomeação:

- `pickup_datetime`   → `ts_embarque`
- `dropoff_datetime`  → `ts_desembarque`
- `passenger_count`   → `qt_passageiro`
- `pickup_latitude`   → `lt_embarque`
- `pickup_longitude`  → `lg_embarque`
- `trip_duration`     → `te_duracao_segundo`
- `store_and_fwd_flag` → `in_armazenamento_envio`

Essa padronização facilita entendimento, governança e reutilização dos dados.

### 2.3. Regras de qualidade e tratamento de nulos/invalidos

As regras de qualidade são centralizadas na função `get_discard_rules()`, que retorna um dicionário nomeado de condições:

```python
{
  "ts_embarque_nulo":           F.col("ts_embarque").isNull(),
  "ts_desembarque_nulo":        F.col("ts_desembarque").isNull(),
  "duracao_nula_ou_negativa":   (F.col("te_duracao_segundo") <= 0) | F.col("te_duracao_segundo").isNull(),
  "passageiros_invalidos":      (F.col("qt_passageiro") <= 0) | F.col("qt_passageiro").isNull(),
  "desembarque_antes_embarque": F.col("ts_desembarque") <= F.col("ts_embarque"),
}
```

Cada linha é classificada com um `_motivo_descarte` via `tag_records(df)`:

- Registros que **não** violam nenhuma regra seguem como válidos.
- Registros que violam alguma regra recebem o nome do motivo de descarte e são usados para o breakdown de descartes no relatório.

### 2.4. Remoção de duplicados

Duplicados são removidos usando `dropDuplicates` em um conjunto de colunas que, conjuntas, identificam uma corrida:

```python
df = df.dropDuplicates([
    "ts_embarque", "ts_desembarque",
    "lg_embarque", "lt_embarque",
    "lg_desembarque", "lt_desembarque",
])
```

Isso evita manter múltiplas linhas idênticas para o mesmo trajeto.

### 2.5. Colunas derivadas

As colunas derivadas são criadas usando **funções nativas** do Spark (`pyspark.sql.functions`), sem UDFs Python:

- `te_duracao_minuto` – duração em minutos, derivada de `te_duracao_segundo`
- `dt_embarque` – data (yyyy-MM-dd) a partir de `ts_embarque`
- `hh_embarque` – hora inteira do dia (0–23)
- `in_corrida_longa` – indicador se a corrida ultrapassa 30 minutos
- `in_armazenamento_envio` – normalização de `"Y"/"N"` para `True/False`

Exemplo:

```python
df = (
  df
  .withColumn("te_duracao_minuto",
      F.round(F.col("te_duracao_segundo") / 60.0, 4))
  .withColumn("hh_embarque",
      F.hour("ts_embarque"))
  .withColumn("dt_embarque",
      F.to_date("ts_embarque"))
  .withColumn("in_corrida_longa",
      F.when(F.col("te_duracao_minuto") > 30, True).otherwise(False))
  .withColumn("in_armazenamento_envio",
      F.when(F.col("in_armazenamento_envio") == "Y", True).otherwise(False))
)
```

### 2.6. Saídas em Parquet

O pipeline gera duas camadas:

1. **Camada tratada (clean)**  
   - DataFrame: `clean_df`  
   - Escrita em **Parquet**, particionada por data de embarque:
   ```python
   clean_df.write \
       .mode("overwrite") \
       .partitionBy("dt_embarque") \
       .parquet(args.output_clean)
   ```

2. **Camada agregada (agg)**  
   - DataFrame: `agg_df`  
   - Métricas por dia/hora de embarque:
     - `qt_corrida_total`
     - `vl_media_duracao_minuto`
     - `qt_corrida_longa`
     - `pe_corrida_longa`
     - `vl_media_passageiro`
   - Escrita em Parquet:
   ```python
   agg_df.write.mode("overwrite").parquet(args.output_agg)
   ```

---

## 3. Escalabilidade

O pipeline foi pensado para rodar tanto em ambiente local (modo `local[*]`) quanto em um cluster distribuído (YARN/Standalone/K8s...), com poucas alterações.

### 3.1. Processamento distribuído

- Todas as transformações (filtros, agregações, derivadas) usam funções nativas do Spark.
- **Não** é utilizado `toPandas()` ou processamento fora do Spark para os dados de fato; todo o volume principal é tratado de forma distribuída.
- **Não** são usados UDFs Python, o que evita overhead de serialização e mantém o plano de execução otimizado.

### 3.2. Ajustes de paralelismo

Foi configurado:

```python
SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "8")
```

Em produção, esse valor pode (e deve) ser ajustado conforme:

- tamanho do dataset
- capacidade do cluster
- tipo de carga (batch pesado x incremental)

Por exemplo, em um cluster com muitos executores poderíamos aumentar para 64 ou 128 partições, garantindo melhor balanceamento de carga entre nós.

### 3.3. Formato de saída

- O uso de **Parquet** como formato padrão facilita:
  - compressão eficiente
  - leitura seletiva de colunas
  - integração com outros motores (Spark SQL, Presto/Trino, Hive, etc.)
- O particionamento por `dt_embarque` na camada clean permite:
  - partição em queries analíticas (por data)
  - menor volume lido em consultas que focam em janelas temporais específicas

### 3.4. Evolução natural para produção

Com poucas mudanças é possível:

- Substituir `--master local[*]` por `--master yarn` ou similar.
- Apontar `--input`/`--output-*` para um data lake (S3, GCS, HDFS).
- Integrar o job em um orquestrador, mantendo o mesmo código.

---

## 4. Validação de Integridade

Ao final da execução, o pipeline gera um relatório JSON em `reports/` com:

- **Total de registros lidos**  
  `total_lido`

- **Válidos processados** (após regras de qualidade e deduplicação)  
  `validos_processados`

- **Descartados (com motivo)**  
  - descartados: `total_descartados`
  - breakdown por motivo em `descartados_por_motivo`, por exemplo:
    ```json
    {
      "ts_embarque_nulo": 123,
      "ts_desembarque_nulo": 45,
      "duracao_nula_ou_negativa": 67,
      "passageiros_invalidos": 12,
      "desembarque_antes_embarque": 3
    }
    ```

- **Registros persistidos**  
  - `persistidos_camada_clean`
  - `persistidos_camada_agg`

- **Validação de integridade final**  
  - Campo booleano: `validos_igual_persistidos`
  - Campo de status: `"APROVADO"` ou `"REPROVADO"`
  - Mensagem detalhada:
    - `"Todos os registros válidos foram persistidos com sucesso."`  
      ou  
    - `"Divergência: X válidos x Y persistidos."`

Exemplo de estrutura do relatório:

```json
{
  "gerado_em": "2026-04-19 21:15:32 UTC",
  "pipeline": "hiveplace-batch-v1",
  "resumo_registros": {
    "total_lido": 1458644,
    "validos_processados": 1432100,
    "total_descartados": 26544,
    "persistidos_camada_clean": 1432100,
    "persistidos_camada_agg": 744
  },
  "descartados_por_motivo": {
    "ts_embarque_nulo": 120,
    "ts_desembarque_nulo": 80,
    "duracao_nula_ou_negativa": 21000,
    "passageiros_invalidos": 5200,
    "desembarque_antes_embarque": 144
  },
  "validacao_integridade": {
    "validos_igual_persistidos": true,
    "status": "APROVADO",
    "detalhe": "Todos os registros válidos foram persistidos com sucesso."
  }
}
```

Além do JSON, o mesmo resumo é logado no console ao final da execução, facilitando a inspeção rápida do resultado do batch.

---

## 5. Restrições Atendidas

O pipeline foi implementado respeitando rigidamente as restrições do desafio:

- **Não** utilizar `collect()` para manipular o dataset principal (apenas contagens pontuais, como `df.count()`, para métricas de integridade)
- **Não** utilizar `toPandas()`
- **Não** realizar processamento fora do Spark sobre o volume principal de dados
- **Não** utilizar UDF em Python quando há alternativa nativa Spark (todas as transformações usam `pyspark.sql.functions`)

Essas decisões garantem que o código é escalável e adequado para grandes volumes de dados em ambiente distribuído.