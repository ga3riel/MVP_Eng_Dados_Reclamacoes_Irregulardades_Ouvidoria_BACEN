# Databricks notebook source
# DBTITLE 1,Camada Silver — Limpeza, Tipos e Padronização
# MAGIC %md
# MAGIC # Camada Silver — Limpeza, Tipos e Padronização
# MAGIC
# MAGIC Transforma os dados raw da camada Bronze aplicando:
# MAGIC - **Conversão de tipos**: strings → INT, DECIMAL, TIMESTAMP
# MAGIC - **Tratamento de nulos**: espaços " " → NULL em colunas numéricas
# MAGIC - **Padronização**: trimestres ("1º" → 1), índices ("58,84" → 58.84)
# MAGIC - **Deduplicação**: ROW_NUMBER() mantém o registro mais recente
# MAGIC
# MAGIC ## Tabelas Silver
# MAGIC
# MAGIC | Tabela | Origem Bronze | Dedupla por |
# MAGIC | --- | --- | --- |
# MAGIC | `reclamacoes_bacen` | `reclamacoes_bacen` | ano + trimestre + instituição |
# MAGIC | `ouvidorias_bacen` | `ouvidorias_bacen` | ano + período + tipo + instituição |
# MAGIC | `consorcios_bacen` | `consorcios_bacen` | ano + semestre + administradora |
# MAGIC
# MAGIC > **Pré-requisito**: execute `01_bronze_bacen` antes deste notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Resumo da Camada Silver — Objetivo do MVP
# MAGIC
# MAGIC ### Por que foi feita
# MAGIC
# MAGIC Os dados extraídos do portal do Bacen chegam à **camada Bronze** em formato bruto: todas as colunas como texto (`STRING`), com sujeira típica de extrações tabulares — espaços vazios em vez de `NULL`, separadores de milhar e vírgula decimal em valores numéricos, trimestres e semestres rotulados como `"1º"`, `"2º"` e registros duplicados decorrentes de múltiplas cargas históricas.
# MAGIC
# MAGIC Esses dados, no estado em que se encontram, **não são adequados para análise nem modelagem**. Qualquer cálculo de média, soma ou comparação exigiria limpeza repetida em cada consulta, tornando os dashboards frágeis e inconsistentes. A camada Silver resolve esse problema transformando os dados uma única vez, de forma **reprodutível e versionada**.
# MAGIC
# MAGIC ### O que foi feito
# MAGIC
# MAGIC Para cada uma das três fontes (Reclamações, Ouvidorias e Consórcios), aplicou-se um pipeline de transformação com quatro etapas:
# MAGIC
# MAGIC | Etapa | Descrição |
# MAGIC | --- | --- |
# MAGIC | **Conversão de tipos** | `CAST` de strings para `INT`, `LONG`, `DECIMAL` e `TIMESTAMP`, garantindo que métricas sejam numericamente operáveis |
# MAGIC | **Tratamento de nulos** | `NULLIF(TRIM(coluna), '')` converte espaços e strings vazias em `NULL` real, evitando falhas em agregações |
# MAGIC | **Padronização** | `REPLACE` de separadores de milhar (`.`) e vírgula decimal (`,` → `.`) em índices e notas; `SUBSTRING` para extrair o número de trimestre/semestre (`"1º"` → `1`) |
# MAGIC | **Deduplicação** | `ROW_NUMBER()` particionado pela chave de negócio de cada tabela, ordenado por `data_carga DESC`, mantendo apenas o registro mais recente |
# MAGIC
# MAGIC ### Resultado
# MAGIC
# MAGIC Três tabelas **tipadas, limpas e sem duplicatas**, prontas para consumo pela camada Gold — onde ocorrem as agregações finais e a construção de indicadores de negócio:
# MAGIC
# MAGIC - `workspace.bacen_silver.reclamacoes_bacen`
# MAGIC - `workspace.bacen_silver.ouvidorias_bacen`
# MAGIC - `workspace.bacen_silver.consorcios_bacen`
# MAGIC
# MAGIC > **Valor para o MVP:** a camada Silver atua como a *fonte única de verdade* intermediária. Ao centralizar a limpeza aqui, garantimos que qualquer análise, dashboard ou modelo construído sobre esses dados parta de uma base consistente — reduzindo retrabalho e eliminando inconsistências entre consultas.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Reclamações — Silver
# MAGIC %md
# MAGIC ## 1. Reclamações de Clientes
# MAGIC
# MAGIC Limpeza e padronização dos dados de reclamações: conversão de tipos, tratamento de nulos, padronização de trimestres e índices (vírgula → ponto decimal). Deduplicação por (ano, trimestre, instituição).

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Silver CTAS — Reclamações
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- CAMADA SILVER - LIMPEZA, TIPOS E PADRONIZAÇÃO (CTAS)
# MAGIC -- ============================================================================
# MAGIC -- Cria a tabela Silver de Reclamações a partir da Bronze raw. Aplica:
# MAGIC --   • CAST de strings para INT/DECIMAL/TIMESTAMP em todas as colunas
# MAGIC --   • NULLIF(TRIM(...), '') para converter espaços vazios em NULL real
# MAGIC --   • REPLACE de separadores de milhar e vírgula decimal no índice
# MAGIC --   • SUBSTRING para extrair o número do trimestre ("1º" → 1)
# MAGIC --   • ROW_NUMBER() particionado por (ano, trimestre, instituição) para
# MAGIC --     eliminar duplicatas mantendo o registro de data_carga mais recente
# MAGIC -- Resultado: tabela tipada, sem duplicatas, pronta para a camada Gold.
# MAGIC -- ============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.bacen_silver.reclamacoes_bacen AS
# MAGIC WITH bronze_clean AS (
# MAGIC   SELECT
# MAGIC     -- Dimensões temporais
# MAGIC     CAST(ano AS INT)                                       AS ano,
# MAGIC     CAST(SUBSTRING(trimestre, 1, 1) AS INT)               AS trimestre,
# MAGIC     -- Dimensões categóricas
# MAGIC     TRIM(categoria)                                        AS categoria,
# MAGIC     TRIM(tipo)                                             AS tipo,
# MAGIC     NULLIF(TRIM(cnpj_if), '')                              AS cnpj_if,
# MAGIC     TRIM(instituicao_financeira)                           AS instituicao_financeira,
# MAGIC     -- Métricas (índice: remove separador de milhar '.', troca vírgula por ponto decimal)
# MAGIC     CAST(REPLACE(REPLACE(NULLIF(TRIM(indice), ''), '.', ''), ',', '.')
# MAGIC          AS DECIMAL(10, 2))                               AS indice,
# MAGIC     CAST(NULLIF(TRIM(quantidade_de_reclamacoes_reguladas_procedentes), '') AS LONG)  AS qtd_reclamacoes_reguladas_procedentes,
# MAGIC     CAST(NULLIF(TRIM(quantidade_de_reclamacoes_reguladas_outras), '')        AS LONG)  AS qtd_reclamacoes_reguladas_outras,
# MAGIC     CAST(NULLIF(TRIM(quantidade_de_reclamacoes_nao_reguladas), '')           AS LONG)  AS qtd_reclamacoes_nao_reguladas,
# MAGIC     CAST(NULLIF(TRIM(quantidade_total_de_reclamacoes), '')                   AS LONG)  AS qtd_total_reclamacoes,
# MAGIC     CAST(NULLIF(TRIM(quantidade_total_de_clientes_ccs_e_scr), '')            AS LONG)  AS qtd_total_clientes_ccs_scr,
# MAGIC     CAST(NULLIF(TRIM(quantidade_de_clientes_ccs), '')                       AS LONG)  AS qtd_clientes_ccs,
# MAGIC     CAST(NULLIF(TRIM(quantidade_de_clientes_scr), '')                       AS LONG)  AS qtd_clientes_scr,
# MAGIC     CAST(NULLIF(TRIM(quantidade_de_clientes_fgc), '')                       AS LONG)  AS qtd_clientes_fgc,
# MAGIC     -- Colunas extras (2024+)
# MAGIC     CAST(NULLIF(TRIM(quantidade_total_de_reclamacoes_respondidas), '')                  AS LONG) AS qtd_total_reclamacoes_respondidas,
# MAGIC     CAST(NULLIF(TRIM(quantidade_de_reclamacoes_reguladas_procedentes_extrapoladas), '') AS LONG) AS qtd_reclamacoes_reguladas_proc_extrapoladas,
# MAGIC     CAST(NULLIF(TRIM(quantidade_de_reclamacoes_procedentes), '')                         AS LONG) AS qtd_reclamacoes_procedentes,
# MAGIC     CAST(NULLIF(TRIM(quantidade_de_reclamacoes_procedentes_extrapoladas), '')            AS LONG) AS qtd_reclamacoes_procedentes_extrapoladas,
# MAGIC     CAST(NULLIF(TRIM(quantidade_total_de_reclamacoes_analisadas), '')                    AS LONG) AS qtd_total_reclamacoes_analisadas,
# MAGIC     -- Metadados de carga
# MAGIC     CAST(ano_extracao AS INT)                              AS ano_extracao,
# MAGIC     CAST(periodo_extracao AS INT)                          AS periodo_extracao,
# MAGIC     TRIM(tipo_instituicao)                                 AS tipo_instituicao,
# MAGIC     CAST(data_carga AS TIMESTAMP)                          AS data_carga
# MAGIC   FROM workspace.bacen_bronze.reclamacoes_bacen
# MAGIC ),
# MAGIC dedup AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY ano, trimestre, instituicao_financeira
# MAGIC       ORDER BY data_carga DESC
# MAGIC     ) AS _rn
# MAGIC   FROM bronze_clean
# MAGIC )
# MAGIC SELECT * EXCEPT (_rn)
# MAGIC FROM dedup
# MAGIC WHERE _rn = 1

# COMMAND ----------

# DBTITLE 1,Verificação Silver — Reclamações
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- VERIFICAÇÃO DA CAMADA SILVER — RECLAMAÇÕES
# MAGIC -- ============================================================================
# MAGIC -- Valida a tabela Silver recém-criada com cinco consultas:
# MAGIC --   1. DESCRIBE — schema completo com tipos convertidos
# MAGIC --   2. COUNT + DISTINCT — total de registros e chaves únicas
# MAGIC --   3. GROUP BY ano_extracao, periodo_extracao — distribuição por período
# MAGIC --   4. Amostra de 10 linhas com indice não-nulo para inspeção visual
# MAGIC --   5. Soma de NULLs por coluna — confirma que espaços viraram NULL
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- Schema e contagem
# MAGIC DESCRIBE workspace.bacen_silver.reclamacoes_bacen;
# MAGIC
# MAGIC -- Total de registros e colunas
# MAGIC SELECT COUNT(*) AS total_registros, COUNT(DISTINCT CONCAT(ano, '-', trimestre, '-', instituicao_financeira)) AS chaves_unicas
# MAGIC FROM workspace.bacen_silver.reclamacoes_bacen;
# MAGIC
# MAGIC -- Distribuição por ano e trimestre
# MAGIC SELECT ano_extracao, periodo_extracao, COUNT(*) AS registros
# MAGIC FROM workspace.bacen_silver.reclamacoes_bacen
# MAGIC GROUP BY ano_extracao, periodo_extracao
# MAGIC ORDER BY ano_extracao, periodo_extracao;
# MAGIC
# MAGIC -- Amostra: verificar tipos limpos (índice decimal, trimestre como INT, nulos reais)
# MAGIC SELECT ano, trimestre, tipo, instituicao_financeira, indice,
# MAGIC        qtd_total_reclamacoes, qtd_clientes_ccs, qtd_clientes_scr, qtd_clientes_fgc
# MAGIC FROM workspace.bacen_silver.reclamacoes_bacen
# MAGIC WHERE indice IS NOT NULL
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Verificar: espaços viraram NULL?
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN qtd_clientes_ccs IS NULL THEN 1 ELSE 0 END) AS ccs_nulls,
# MAGIC   SUM(CASE WHEN qtd_clientes_scr IS NULL THEN 1 ELSE 0 END) AS scr_nulls,
# MAGIC   SUM(CASE WHEN qtd_clientes_fgc IS NULL THEN 1 ELSE 0 END) AS fgc_nulls,
# MAGIC   SUM(CASE WHEN cnpj_if IS NULL THEN 1 ELSE 0 END) AS cnpj_nulls,
# MAGIC   SUM(CASE WHEN indice IS NULL THEN 1 ELSE 0 END) AS indice_nulls
# MAGIC FROM workspace.bacen_silver.reclamacoes_bacen;

# COMMAND ----------

# DBTITLE 1,Ouvidorias — Silver
# MAGIC %md
# MAGIC ## 2. Ouvidorias
# MAGIC
# MAGIC Limpeza e tipagem dos dados de ouvidorias: notas como DECIMAL, tempo médio como DECIMAL, deduplicação por (ano, período, tipo_período, instituição).

# COMMAND ----------

# DBTITLE 1,Silver CTAS — Ouvidorias
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- CAMADA SILVER OUVIDORIAS - LIMPEZA E TIPOS (CTAS)
# MAGIC -- ============================================================================
# MAGIC -- Cria a tabela Silver de Ouvidorias a partir da Bronze raw. Aplica:
# MAGIC --   • CAST de strings para INT (contagens) e DECIMAL (notas e tempo médio)
# MAGIC --   • REPLACE de vírgula decimal por ponto em nota_prazo, nota_qualidade,
# MAGIC --     nota_final e tempo_medio_respostas
# MAGIC --   • NULLIF(TRIM(...), '') em cnpj_if para espaços vazios
# MAGIC --   • ROW_NUMBER() particionado por (ano, periodo, tipo_periodo,
# MAGIC --     instituição) para eliminar duplicatas mantendo o registro mais recente
# MAGIC -- Resultado: tabela tipada com notas decimais, pronta para a camada Gold.
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE workspace.bacen_silver.ouvidorias_bacen AS
# MAGIC WITH bronze_clean AS (
# MAGIC   SELECT
# MAGIC     CAST(ano AS INT) AS ano, CAST(periodo AS INT) AS periodo,
# MAGIC     TRIM(tipo_periodo) AS tipo_periodo, TRIM(categoria) AS categoria, TRIM(tipo) AS tipo,
# MAGIC     NULLIF(TRIM(cnpj_if), '') AS cnpj_if, TRIM(instituicao_financeira) AS instituicao_financeira,
# MAGIC     CAST(num_respostas_fornecidas AS INT) AS num_respostas_fornecidas,
# MAGIC     CAST(num_respostas_atrasadas AS INT) AS num_respostas_atrasadas,
# MAGIC     CAST(REPLACE(tempo_medio_respostas, ',', '.') AS DECIMAL(10, 6)) AS tempo_medio_respostas,
# MAGIC     CAST(qtd_reclamacoes_proc_qualid AS INT) AS qtd_reclamacoes_proc_qualid,
# MAGIC     CAST(qtd_reclamacoes_encerradas AS INT) AS qtd_reclamacoes_encerradas,
# MAGIC     CAST(qtd_reclamacoes_proc_ouvidoria AS INT) AS qtd_reclamacoes_proc_ouvidoria,
# MAGIC     TRIM(adesao_consumidor_gov) AS adesao_consumidor_gov,
# MAGIC     CAST(REPLACE(nota_prazo, ',', '.') AS DECIMAL(10, 6)) AS nota_prazo,
# MAGIC     CAST(REPLACE(nota_qualidade, ',', '.') AS DECIMAL(10, 6)) AS nota_qualidade,
# MAGIC     CAST(REPLACE(nota_final, ',', '.') AS DECIMAL(10, 6)) AS nota_final,
# MAGIC     CAST(data_carga AS TIMESTAMP) AS data_carga
# MAGIC   FROM workspace.bacen_bronze.ouvidorias_bacen
# MAGIC ),
# MAGIC dedup AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY ano, periodo, tipo_periodo, instituicao_financeira ORDER BY data_carga DESC) AS _rn
# MAGIC   FROM bronze_clean
# MAGIC )
# MAGIC SELECT * EXCEPT (_rn) FROM dedup WHERE _rn = 1

# COMMAND ----------

# DBTITLE 1,Consórcios — Silver
# MAGIC %md
# MAGIC ## 3. Consórcios
# MAGIC
# MAGIC Limpeza e tipagem dos dados de consórcios: índice decimal, semestre como INT, deduplicação por (ano, semestre, administradora).

# COMMAND ----------

# DBTITLE 1,Silver CTAS — Consórcios
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- CAMADA SILVER CONSÓRCIOS - LIMPEZA E TIPOS (CTAS)
# MAGIC -- ============================================================================
# MAGIC -- Cria a tabela Silver de Consórcios a partir da Bronze raw. Aplica:
# MAGIC --   • CAST de strings para INT (semestre, contagens) e DECIMAL (índice)
# MAGIC --   • REPLACE de separadores de milhar e vírgula decimal no índice
# MAGIC --   • SUBSTRING para extrair o número do semestre ("1º" → 1)
# MAGIC --   • NULLIF(TRIM(...), '') em cnpj_ac e colunas numéricas
# MAGIC --   • ROW_NUMBER() particionado por (ano, semestre, administradora) para
# MAGIC --     eliminar duplicatas mantendo o registro de data_carga mais recente
# MAGIC -- Resultado: tabela tipada, sem duplicatas, pronta para a camada Gold.
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE workspace.bacen_silver.consorcios_bacen AS
# MAGIC WITH bronze_clean AS (
# MAGIC   SELECT
# MAGIC     CAST(ano AS INT) AS ano,
# MAGIC     CAST(SUBSTRING(semestre, 1, 1) AS INT) AS semestre,
# MAGIC     NULLIF(TRIM(cnpj_ac), '') AS cnpj_ac,
# MAGIC     TRIM(administradora_de_consorcio) AS administradora,
# MAGIC     CAST(REPLACE(REPLACE(NULLIF(TRIM(indice), ''), '.', ''), ',', '.') AS DECIMAL(10, 2)) AS indice,
# MAGIC     CAST(NULLIF(TRIM(quantidade_de_reclamacoes_reguladas_procedentes), '') AS LONG) AS qtd_reclamacoes_procedentes,
# MAGIC     CAST(NULLIF(TRIM(quantidade_de_reclamacoes_reguladas_outras), '') AS LONG) AS qtd_reclamacoes_outras,
# MAGIC     CAST(NULLIF(TRIM(quantidade_de_reclamacoes_nao_reguladas), '') AS LONG) AS qtd_reclamacoes_nao_reguladas,
# MAGIC     CAST(NULLIF(TRIM(quantidade_total_de_reclamacoes), '') AS LONG) AS qtd_total_reclamacoes,
# MAGIC     CAST(NULLIF(TRIM(quantidade_de_clientes_consorciados), '') AS LONG) AS qtd_clientes_consorciados,
# MAGIC     CAST(_ano_extracao AS INT) AS ano_extracao,
# MAGIC     CAST(_periodo_extracao AS INT) AS periodo_extracao,
# MAGIC     CAST(_data_carga AS TIMESTAMP) AS data_carga
# MAGIC   FROM workspace.bacen_bronze.consorcios_bacen
# MAGIC ),
# MAGIC dedup AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY ano, semestre, administradora ORDER BY data_carga DESC) AS _rn
# MAGIC   FROM bronze_clean
# MAGIC )
# MAGIC SELECT * EXCEPT (_rn) FROM dedup WHERE _rn = 1

# COMMAND ----------

# DBTITLE 1,Verificação Final — Todas as Tabelas Silver
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- VERIFICAÇÃO FINAL — TODAS AS TABELAS SILVER
# MAGIC -- ============================================================================
# MAGIC -- Conta os registros de cada tabela Silver criada neste notebook
# MAGIC -- (reclamacoes_bacen, ouvidorias_bacen, consorcios_bacen) em uma
# MAGIC -- única consulta UNION ALL. Serve como checagem rápida para confirmar
# MAGIC -- que as três fontes foram transformadas e não estão vazias.
# MAGIC -- ============================================================================
# MAGIC SELECT 'reclamacoes_bacen' AS tabela, COUNT(*) AS registros FROM workspace.bacen_silver.reclamacoes_bacen
# MAGIC UNION ALL SELECT 'ouvidorias_bacen', COUNT(*) FROM workspace.bacen_silver.ouvidorias_bacen
# MAGIC UNION ALL SELECT 'consorcios_bacen', COUNT(*) FROM workspace.bacen_silver.consorcios_bacen
# MAGIC ORDER BY tabela