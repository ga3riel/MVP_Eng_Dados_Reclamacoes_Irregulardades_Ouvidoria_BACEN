# Databricks notebook source
# DBTITLE 1,Camada Gold — Tabelas Analíticas
# MAGIC %md
# MAGIC # Camada Gold — Tabelas Analíticas
# MAGIC
# MAGIC Cria tabelas analíticas prontas para consumo a partir da camada Silver:
# MAGIC - **Rankings** por período com ROW_NUMBER()
# MAGIC - **Resumos anuais** com agregações
# MAGIC - **Cruzamentos** entre fontes (Reclamações × Ouvidorias, Bancos × Consórcios)
# MAGIC - **Classificação de risco** combinando volume de reclamações + nota da ouvidoria
# MAGIC
# MAGIC ## Tabelas Gold
# MAGIC
# MAGIC | Tabela | Descrição | Origem |
# MAGIC | --- | --- | --- |
# MAGIC | `ranking_reclamacoes` | Ranking de reclamações por banco e período | Silver `reclamacoes_bacen` |
# MAGIC | `resumo_anual_reclamacoes` | Resumo anual por categoria | Silver `reclamacoes_bacen` |
# MAGIC | `ranking_ouvidorias` | Ranking de qualidade das ouvidorias | Silver `ouvidorias_bacen` |
# MAGIC | `resumo_ouvidorias_anual` | Resumo anual das ouvidorias | Silver `ouvidorias_bacen` |
# MAGIC | `ranking_consorcios` | Ranking de reclamações de consórcios | Silver `consorcios_bacen` |
# MAGIC | `resumo_consorcios_anual` | Resumo anual de consórcios | Silver `consorcios_bacen` |
# MAGIC | `reclamacoes_ouvidorias` | Cruzamento Reclamações × Ouvidorias + risco | Gold `ranking_reclamacoes` + Silver `ouvidorias_bacen` |
# MAGIC | `comparativo_anual` | Comparativo Bancos × Consórcios | Gold `ranking_reclamacoes` + Gold `ranking_consorcios` |
# MAGIC
# MAGIC > **Pré-requisito**: execute `02_silver_bacen` antes deste notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo do Notebook
# MAGIC
# MAGIC ### Resumo
# MAGIC
# MAGIC Este notebook implementa a **camada Gold** do projeto BACEN, criando tabelas analíticas prontas para consumo a partir dos dados já tratados na camada Silver. São gerados rankings, resumos anuais, cruzamentos entre fontes e uma classificação de risco, totalizando **8 tabelas Gold** no schema `workspace.bacen_gold`.
# MAGIC
# MAGIC ### Objetivo
# MAGIC
# MAGIC Transformar os dados padronizados da camada Silver em tabelas analíticas de alto valor, com rankings, agregações anuais e cruzamentos entre fontes distintas (Reclamações, Ouvidorias e Consórcios), possibilitando análise comparativa e classificação de risco por instituição.
# MAGIC
# MAGIC ### O que foi feito
# MAGIC
# MAGIC 1. **Ranking de Reclamações** (`ranking_reclamacoes`): padroniza categoria em Top/Demais, unifica o volume de reclamações com `COALESCE` (resolve mudança de schema entre 2017–2023 e 2024+), calcula taxa por milhão de clientes e atribui `ranking_periodo` com `ROW_NUMBER()`.
# MAGIC 2. **Resumo Anual de Reclamações** (`resumo_anual_reclamacoes`): agrega por ano e categoria (qtd_instituicoes, total_reclamacoes, indice_medio, total_clientes).
# MAGIC 3. **Ranking de Ouvidorias** (`ranking_ouvidorias`): preserva notas (prazo, qualidade, final) e métricas de atendimento; atribui dois rankings — `ranking_nota` e `ranking_volume_reclamacoes`.
# MAGIC 4. **Resumo Anual de Ouvidorias** (`resumo_ouvidorias_anual`): médias anuais das notas, totais de respostas, reclamações encerradas e contagem de adesão ao Consumidor.gov.
# MAGIC 5. **Ranking de Consórcios** (`ranking_consorcios`): calcula taxa de reclamações por milhão de consorciados e atribui `ranking_volume` e `ranking_indice`.
# MAGIC 6. **Resumo Anual de Consórcios** (`resumo_consorcios_anual`): agrega por ano (qtd_administradoras, total_reclamacoes, indice_medio, total_procedentes, total_nao_reguladas).
# MAGIC 7. **Cruzamento Reclamações × Ouvidorias** (`reclamacoes_ouvidorias`): `LEFT JOIN` da Gold `ranking_reclamacoes` com a Silver `ouvidorias_bacen` (chave: ano + trimestre = período + nome sem "(conglomerado)"), com `classificacao_risco` em 4 níveis (CRÍTICO, ALTO, MÉDIO, BAIXO).
# MAGIC 8. **Comparativo Bancos × Consórcios** (`comparativo_anual`): `UNION ALL` de duas CTEs agregadas por ano, unificando métricas para comparação direta entre as duas fontes.
# MAGIC
# MAGIC ### Fluxo geral
# MAGIC
# MAGIC ```text
# MAGIC Silver (reclamacoes_bacen, ouvidorias_bacen, consorcios_bacen)
# MAGIC   │
# MAGIC   ├─▶ ranking_reclamacoes          ──┐
# MAGIC   │                                   ├─▶ reclamacoes_ouvidorias (LEFT JOIN + classificacao_risco)
# MAGIC   ├─▶ ouvidorias_bacen ──────────────┘
# MAGIC   │
# MAGIC   ├─▶ ranking_consorcios ────────────┐
# MAGIC   │                                   ├─▶ comparativo_anual (UNION ALL)
# MAGIC   └─▶ ranking_reclamacoes ───────────┘
# MAGIC   │
# MAGIC   └─▶ resumo_anual_reclamacoes, resumo_ouvidorias_anual, resumo_consorcios_anual (agregações anuais)
# MAGIC ```
# MAGIC
# MAGIC ### Verificação
# MAGIC
# MAGIC Cada seção possui queries de validação, e há uma verificação final que conta os registros de todas as 8 tabelas Gold:
# MAGIC
# MAGIC - **Reclamações**: contagem das duas tabelas; Top 10 instituições do período mais recente (2026 T2); evolução anual por categoria.
# MAGIC - **Ouvidorias**: *(implícita via queries subsequentes)*
# MAGIC - **Combinado**: cobertura total e % de match com ouvidoria; distribuição por `classificacao_risco`; Top 10 instituições CRÍTICAS.
# MAGIC - **Comparativo**: listagem de todas as linhas de `comparativo_anual` para comparação direta Bancos vs Consórcios.
# MAGIC - **Verificação final**: `UNION ALL` com `COUNT(*)` de todas as 8 tabelas Gold, ordenado por nome da tabela.
# MAGIC
# MAGIC > **Pré-requisito**: executar `02_silver_bacen` antes deste notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Reclamações — Gold
# MAGIC %md
# MAGIC ## 1. Reclamações — Ranking e Resumo Anual
# MAGIC
# MAGIC Cria `ranking_reclamacoes` (ranking por período com taxa por milhão de clientes) e `resumo_anual_reclamacoes` (agregação anual por categoria Top/Demais).

# COMMAND ----------

# DBTITLE 1,Gold CTAS — Reclamações
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- CAMADA GOLD - TABELA 1: RANKING DE RECLAMAÇÕES
# MAGIC -- ============================================================================
# MAGIC -- Cria ranking_reclamacoes a partir da Silver. Padroniza categoria em
# MAGIC -- Top/Demais, unifica o volume de reclamações (COALESCE resolve mudança
# MAGIC -- de schema entre 2017-2023 e 2024+), calcula taxa por milhão de
# MAGIC -- clientes e atribui ranking_periodo com ROW_NUMBER(). Cria também
# MAGIC -- resumo_anual_reclamacoes com agregações anuais por categoria
# MAGIC -- (qtd_instituicoes, total_reclamacoes, indice_medio, total_clientes).
# MAGIC -- ============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.bacen_gold.ranking_reclamacoes AS
# MAGIC SELECT
# MAGIC   ano,
# MAGIC   trimestre,
# MAGIC   -- Padronizar categorias em dois grupos: Top e Demais
# MAGIC   CASE
# MAGIC     WHEN categoria LIKE 'Top%' OR categoria LIKE 'Mais de quatro%' THEN 'Top'
# MAGIC     ELSE 'Demais'
# MAGIC   END AS categoria,
# MAGIC   tipo,
# MAGIC   cnpj_if,
# MAGIC   instituicao_financeira,
# MAGIC   indice,
# MAGIC   -- Volume unificado (resolve mudança de schema em 2024+)
# MAGIC   COALESCE(qtd_total_reclamacoes, qtd_total_reclamacoes_respondidas) AS qtd_reclamacoes,
# MAGIC   qtd_reclamacoes_reguladas_procedentes,
# MAGIC   qtd_reclamacoes_reguladas_outras,
# MAGIC   qtd_reclamacoes_nao_reguladas,
# MAGIC   qtd_total_clientes_ccs_scr,
# MAGIC   qtd_clientes_ccs,
# MAGIC   qtd_clientes_scr,
# MAGIC   qtd_clientes_fgc,
# MAGIC   -- Taxa de reclamações por milhão de clientes
# MAGIC   CASE
# MAGIC     WHEN qtd_total_clientes_ccs_scr > 0
# MAGIC     THEN ROUND(
# MAGIC       CAST(COALESCE(qtd_total_reclamacoes, qtd_total_reclamacoes_respondidas) AS DECIMAL(18, 2))
# MAGIC       / qtd_total_clientes_ccs_scr * 1000000, 2)
# MAGIC     ELSE NULL
# MAGIC   END AS taxa_reclamacoes_por_milhao,
# MAGIC   -- Ranking dentro de cada período
# MAGIC   ROW_NUMBER() OVER (
# MAGIC     PARTITION BY ano, trimestre
# MAGIC     ORDER BY COALESCE(qtd_total_reclamacoes, qtd_total_reclamacoes_respondidas) DESC NULLS LAST
# MAGIC   ) AS ranking_periodo,
# MAGIC   tipo_instituicao,
# MAGIC   data_carga
# MAGIC FROM workspace.bacen_silver.reclamacoes_bacen;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- CAMADA GOLD - TABELA 2: RESUMO ANUAL POR CATEGORIA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.bacen_gold.resumo_anual_reclamacoes AS
# MAGIC SELECT
# MAGIC   ano,
# MAGIC   CASE
# MAGIC     WHEN categoria LIKE 'Top%' OR categoria LIKE 'Mais de quatro%' THEN 'Top'
# MAGIC     ELSE 'Demais'
# MAGIC   END AS categoria,
# MAGIC   COUNT(DISTINCT instituicao_financeira) AS qtd_instituicoes,
# MAGIC   SUM(COALESCE(qtd_total_reclamacoes, qtd_total_reclamacoes_respondidas)) AS total_reclamacoes,
# MAGIC   ROUND(AVG(indice), 2) AS indice_medio,
# MAGIC   SUM(qtd_total_clientes_ccs_scr) AS total_clientes
# MAGIC FROM workspace.bacen_silver.reclamacoes_bacen
# MAGIC GROUP BY
# MAGIC   ano,
# MAGIC   CASE
# MAGIC     WHEN categoria LIKE 'Top%' OR categoria LIKE 'Mais de quatro%' THEN 'Top'
# MAGIC     ELSE 'Demais'
# MAGIC   END
# MAGIC ORDER BY ano, categoria;

# COMMAND ----------

# DBTITLE 1,Verificação Gold — Reclamações
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- VERIFICAÇÃO DA CAMADA GOLD — RECLAMAÇÕES
# MAGIC -- ============================================================================
# MAGIC -- Três consultas de validação: (1) contagem de registros das duas
# MAGIC -- tabelas via UNION ALL; (2) Top 10 instituições do período mais recente
# MAGIC -- (2026 T2) com ranking, taxa por milhão e índice; (3) evolução anual
# MAGIC -- por categoria com qtd_instituicoes, total_reclamacoes e indice_medio.
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- Contagem das duas tabelas
# MAGIC SELECT 'ranking_reclamacoes' AS tabela, COUNT(*) AS total_registros
# MAGIC FROM workspace.bacen_gold.ranking_reclamacoes
# MAGIC UNION ALL
# MAGIC SELECT 'resumo_anual_reclamacoes', COUNT(*)
# MAGIC FROM workspace.bacen_gold.resumo_anual_reclamacoes;
# MAGIC
# MAGIC -- Top 10 instituições com mais reclamações em 2026 T2
# MAGIC SELECT ano, trimestre, ranking_periodo, categoria, instituicao_financeira,
# MAGIC        qtd_reclamacoes, taxa_reclamacoes_por_milhao, indice
# MAGIC FROM workspace.bacen_gold.ranking_reclamacoes
# MAGIC WHERE ano = 2026 AND trimestre = 2
# MAGIC ORDER BY ranking_periodo
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Evolução anual por categoria padronizada
# MAGIC SELECT ano, categoria, qtd_instituicoes, total_reclamacoes,
# MAGIC        indice_medio, total_clientes
# MAGIC FROM workspace.bacen_gold.resumo_anual_reclamacoes
# MAGIC ORDER BY ano, categoria;

# COMMAND ----------

# DBTITLE 1,Ouvidorias — Gold
# MAGIC %md
# MAGIC ## 2. Ouvidorias — Ranking e Resumo Anual
# MAGIC
# MAGIC Cria `ranking_ouvidorias` (ranking por nota final e volume de reclamações) e `resumo_ouvidorias_anual` (médias anuais das notas e totais).

# COMMAND ----------

# DBTITLE 1,Gold CTAS — Ouvidorias
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- CAMADA GOLD OUVIDORIAS - RANKING E RESUMO (CTAS)
# MAGIC -- ============================================================================
# MAGIC -- Cria ranking_ouvidorias a partir da Silver: padroniza categoria em
# MAGIC -- Top/Demais, preserva todas as notas (prazo, qualidade, final) e
# MAGIC -- métricas de atendimento, e atribui dois rankings com ROW_NUMBER():
# MAGIC -- ranking_nota (por nota_final DESC) e ranking_volume_reclamacoes (por
# MAGIC -- qtd_reclamacoes_proc_ouvidoria DESC). Cria também resumo_ouvidorias_anual
# MAGIC -- com médias anuais das notas, totais de respostas, reclamações
# MAGIC -- encerradas e contagem de adesão ao Consumidor.gov.
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE workspace.bacen_gold.ranking_ouvidorias AS
# MAGIC SELECT ano, periodo, tipo_periodo,
# MAGIC   CASE WHEN categoria LIKE 'Top%' THEN 'Top' ELSE 'Demais' END AS categoria,
# MAGIC   tipo, cnpj_if, instituicao_financeira,
# MAGIC   num_respostas_fornecidas, num_respostas_atrasadas, tempo_medio_respostas,
# MAGIC   qtd_reclamacoes_proc_qualid, qtd_reclamacoes_encerradas, qtd_reclamacoes_proc_ouvidoria,
# MAGIC   adesao_consumidor_gov, nota_prazo, nota_qualidade, nota_final,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY ano, periodo, tipo_periodo ORDER BY nota_final DESC NULLS LAST) AS ranking_nota,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY ano, periodo, tipo_periodo ORDER BY qtd_reclamacoes_proc_ouvidoria DESC NULLS LAST) AS ranking_volume_reclamacoes,
# MAGIC   data_carga
# MAGIC FROM workspace.bacen_silver.ouvidorias_bacen;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.bacen_gold.resumo_ouvidorias_anual AS
# MAGIC SELECT ano,
# MAGIC   CASE WHEN categoria LIKE 'Top%' THEN 'Top' ELSE 'Demais' END AS categoria,
# MAGIC   COUNT(DISTINCT instituicao_financeira) AS qtd_instituicoes,
# MAGIC   ROUND(AVG(nota_final), 2) AS nota_final_media,
# MAGIC   ROUND(AVG(nota_prazo), 2) AS nota_prazo_media,
# MAGIC   ROUND(AVG(nota_qualidade), 2) AS nota_qualidade_media,
# MAGIC   ROUND(AVG(tempo_medio_respostas), 2) AS tempo_medio_respostas,
# MAGIC   SUM(num_respostas_fornecidas) AS total_respostas,
# MAGIC   SUM(num_respostas_atrasadas) AS total_respostas_atrasadas,
# MAGIC   SUM(qtd_reclamacoes_encerradas) AS total_reclamacoes_encerradas,
# MAGIC   SUM(CASE WHEN adesao_consumidor_gov = 'SIM' THEN 1 ELSE 0 END) AS qtd_adesao_consumidor_gov
# MAGIC FROM workspace.bacen_silver.ouvidorias_bacen
# MAGIC GROUP BY ano, CASE WHEN categoria LIKE 'Top%' THEN 'Top' ELSE 'Demais' END
# MAGIC ORDER BY ano, categoria

# COMMAND ----------

# DBTITLE 1,Consórcios — Gold
# MAGIC %md
# MAGIC ## 3. Consórcios — Ranking e Resumo Anual
# MAGIC
# MAGIC Cria `ranking_consorcios` (ranking por volume e índice) e `resumo_consorcios_anual` (agregação anual).

# COMMAND ----------

# DBTITLE 1,Gold CTAS — Consórcios
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- CAMADA GOLD CONSÓRCIOS - RANKING E RESUMO (CTAS)
# MAGIC -- ============================================================================
# MAGIC -- Cria ranking_consorcios a partir da Silver: calcula taxa de
# MAGIC -- reclamações por milhão de consorciados e atribui dois rankings com
# MAGIC -- ROW_NUMBER(): ranking_volume (por qtd_total_reclamacoes DESC) e
# MAGIC -- ranking_indice (por indice DESC). Cria também resumo_consorcios_anual
# MAGIC -- com agregações anuais (qtd_administradoras, total_reclamacoes,
# MAGIC -- indice_medio, total_procedentes, total_nao_reguladas).
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE workspace.bacen_gold.ranking_consorcios AS
# MAGIC SELECT ano, semestre, cnpj_ac, administradora, indice,
# MAGIC   qtd_reclamacoes_procedentes, qtd_reclamacoes_outras, qtd_reclamacoes_nao_reguladas,
# MAGIC   qtd_total_reclamacoes, qtd_clientes_consorciados,
# MAGIC   CASE WHEN qtd_clientes_consorciados > 0
# MAGIC     THEN ROUND(CAST(qtd_total_reclamacoes AS DECIMAL(18, 2)) / qtd_clientes_consorciados * 1000000, 2)
# MAGIC     ELSE NULL END AS taxa_reclamacoes_por_milhao,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY ano, semestre ORDER BY qtd_total_reclamacoes DESC NULLS LAST) AS ranking_volume,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY ano, semestre ORDER BY indice DESC NULLS LAST) AS ranking_indice,
# MAGIC   data_carga
# MAGIC FROM workspace.bacen_silver.consorcios_bacen;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.bacen_gold.resumo_consorcios_anual AS
# MAGIC SELECT ano,
# MAGIC   COUNT(DISTINCT administradora) AS qtd_administradoras,
# MAGIC   SUM(qtd_total_reclamacoes) AS total_reclamacoes,
# MAGIC   ROUND(AVG(indice), 2) AS indice_medio,
# MAGIC   SUM(qtd_clientes_consorciados) AS total_clientes,
# MAGIC   SUM(qtd_reclamacoes_procedentes) AS total_procedentes,
# MAGIC   SUM(qtd_reclamacoes_nao_reguladas) AS total_nao_reguladas
# MAGIC FROM workspace.bacen_silver.consorcios_bacen
# MAGIC GROUP BY ano ORDER BY ano

# COMMAND ----------

# DBTITLE 1,Combinado — Gold
# MAGIC %md
# MAGIC ## 4. Combinado: Reclamações × Ouvidorias
# MAGIC
# MAGIC Cruza os dados de **Reclamações** (reclamações de clientes) com **Ouvidorias** (qualidade do atendimento) por instituição e período.
# MAGIC
# MAGIC **Join key**: `ano + trimestre = periodo + REPLACE(instituicao_financeira, ' (conglomerado)', '')`
# MAGIC **Cobertura**: 37,3% dos registros de Reclamações têm match com Ouvidorias (2017-2021)
# MAGIC **Classificação de risco**: CRÍTICO, ALTO, MÉDIO, BAIXO — combina volume de reclamações + nota da ouvidoria

# COMMAND ----------

# DBTITLE 1,Gold Combinado — CTAS
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- GOLD COMBINADO: RECLAMAÇÕES × OUVIDORIAS
# MAGIC -- ============================================================================
# MAGIC -- Cria reclamacoes_ouvidorias com LEFT JOIN da Gold ranking_reclamacoes
# MAGIC -- com a Silver ouvidorias_bacen. Mantém todas as Reclamações (2017-2026)
# MAGIC -- e traz Ouvidorias quando há match (2017-2021). Join key: ano +
# MAGIC -- trimestre=periodo + nome sem "(conglomerado)". Calcula classificacao_risco
# MAGIC -- combinando volume de reclamações + nota da ouvidoria em 4 níveis:
# MAGIC -- CRÍTICO, ALTO, MÉDIO, BAIXO. Registros sem match (2022+) ficam NULL.
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- LEFT JOIN: mantém todas as Reclamações (2017-2026), traz Ouvidorias quando há match (2017-2021)
# MAGIC -- Join key: ano + trimestre=periodo + nome sem "(conglomerado)"
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.bacen_gold.reclamacoes_ouvidorias AS
# MAGIC SELECT
# MAGIC   r.ano, r.trimestre, r.categoria, r.tipo, r.cnpj_if, r.instituicao_financeira,
# MAGIC   -- Reclamações
# MAGIC   r.qtd_reclamacoes, r.indice, r.taxa_reclamacoes_por_milhao,
# MAGIC   r.ranking_periodo AS ranking_reclamacoes,
# MAGIC   -- Ouvidorias (NULL quando não há match = 2022+)
# MAGIC   o.nota_final AS nota_ouvidoria, o.nota_prazo AS nota_prazo_ouvidoria,
# MAGIC   o.nota_qualidade AS nota_qualidade_ouvidoria, o.tempo_medio_respostas,
# MAGIC   o.num_respostas_fornecidas, o.num_respostas_atrasadas,
# MAGIC   o.qtd_reclamacoes_encerradas AS qtd_reclamacoes_encerradas_ouvidoria,
# MAGIC   o.adesao_consumidor_gov,
# MAGIC   CASE WHEN o.nota_final IS NOT NULL THEN 1 ELSE 0 END AS tem_dado_ouvidoria,
# MAGIC   -- Classificação de risco combinada
# MAGIC   CASE
# MAGIC     WHEN o.nota_final IS NOT NULL AND r.qtd_reclamacoes IS NOT NULL THEN
# MAGIC       CASE
# MAGIC         WHEN r.qtd_reclamacoes > 1000 AND o.nota_final < 2.0 THEN 'CRÍTICO'
# MAGIC         WHEN r.qtd_reclamacoes > 500  AND o.nota_final < 3.0 THEN 'ALTO'
# MAGIC         WHEN r.qtd_reclamacoes > 100  AND o.nota_final < 3.5 THEN 'MÉDIO'
# MAGIC         ELSE 'BAIXO'
# MAGIC       END
# MAGIC     ELSE NULL
# MAGIC   END AS classificacao_risco,
# MAGIC   r.data_carga
# MAGIC FROM workspace.bacen_gold.ranking_reclamacoes r
# MAGIC LEFT JOIN workspace.bacen_silver.ouvidorias_bacen o
# MAGIC   ON  r.ano = o.ano
# MAGIC   AND r.trimestre = o.periodo
# MAGIC   AND o.tipo_periodo = 'T'
# MAGIC   AND REPLACE(r.instituicao_financeira, ' (conglomerado)', '') = o.instituicao_financeira

# COMMAND ----------

# DBTITLE 1,Verificação Gold Combinado
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- VERIFICAÇÃO DO GOLD COMBINADO
# MAGIC -- ============================================================================
# MAGIC -- Três consultas de validação: (1) cobertura total e % de registros
# MAGIC -- com dado de ouvidoria; (2) distribuição por classificacao_risco com
# MAGIC -- média de reclamações e nota; (3) Top 10 instituições CRÍTICAS com
# MAGIC -- qtd_reclamacoes, nota_ouvidoria, notas de prazo/qualidade e taxa por
# MAGIC -- milhão.
# MAGIC -- ============================================================================
# MAGIC -- Cobertura e contagem
# MAGIC SELECT COUNT(*) AS total, SUM(tem_dado_ouvidoria) AS com_ouvidoria,
# MAGIC        ROUND(SUM(tem_dado_ouvidoria) * 100.0 / COUNT(*), 1) AS pct_cobertura
# MAGIC FROM workspace.bacen_gold.reclamacoes_ouvidorias;
# MAGIC
# MAGIC -- Distribuição por classificação de risco
# MAGIC SELECT classificacao_risco, COUNT(*) AS qtd,
# MAGIC   ROUND(AVG(qtd_reclamacoes), 0) AS media_reclamacoes,
# MAGIC   ROUND(AVG(nota_ouvidoria), 2) AS media_nota_ouvidoria
# MAGIC FROM workspace.bacen_gold.reclamacoes_ouvidorias
# MAGIC WHERE classificacao_risco IS NOT NULL
# MAGIC GROUP BY classificacao_risco
# MAGIC ORDER BY CASE classificacao_risco WHEN 'CRÍTICO' THEN 1 WHEN 'ALTO' THEN 2 WHEN 'MÉDIO' THEN 3 WHEN 'BAIXO' THEN 4 END;
# MAGIC
# MAGIC -- Top 10 instituições CRÍTICAS
# MAGIC SELECT ano, trimestre, instituicao_financeira, qtd_reclamacoes, nota_ouvidoria,
# MAGIC   nota_prazo_ouvidoria, nota_qualidade_ouvidoria, taxa_reclamacoes_por_milhao
# MAGIC FROM workspace.bacen_gold.reclamacoes_ouvidorias
# MAGIC WHERE classificacao_risco = 'CRÍTICO'
# MAGIC ORDER BY qtd_reclamacoes DESC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Comparativo — Gold
# MAGIC %md
# MAGIC ## 5. Comparativo: Bancos × Consórcios
# MAGIC
# MAGIC Compara reclamações entre **Bancos/Financeiras** (trimestral) e **Consórcios** (semestral) ao longo dos anos.
# MAGIC
# MAGIC **Observação**: Não há join por entidade — CNPJ e nomes são distintos entre as duas fontes. O cruzamento é feito no nível anual, unificando métricas para comparação.
# MAGIC **Cobertura**: Bancos 2017-2026 | Consórcios 2014-2026

# COMMAND ----------

# DBTITLE 1,Gold Comparativo — CTAS
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- GOLD COMPARATIVO: RECLAMAÇÕES BANCOS × CONSÓRCIOS
# MAGIC -- ============================================================================
# MAGIC -- Cria comparativo_anual com UNION ALL de duas CTEs: rec_anual agrega
# MAGIC -- ranking_reclamacoes por ano (fonte Bancos, periodicidade Trimestral) e
# MAGIC -- con_anual agrega ranking_consorcios por ano (fonte Consórcios,
# MAGIC -- periodicidade Semestral). Cada CTE calcula qtd_instituicoes,
# MAGIC -- total_reclamacoes, indice_medio, total_clientes e taxa_por_milhao.
# MAGIC -- Resultado: tabela única para comparação ano a ano entre os dois tipos.
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE TABLE workspace.bacen_gold.comparativo_anual AS
# MAGIC WITH rec_anual AS (
# MAGIC   SELECT 'Bancos e Financeiras' AS fonte, ano, 'Trimestral' AS periodicidade,
# MAGIC     COUNT(DISTINCT instituicao_financeira) AS qtd_instituicoes,
# MAGIC     SUM(qtd_reclamacoes) AS total_reclamacoes,
# MAGIC     ROUND(AVG(indice), 2) AS indice_medio,
# MAGIC     SUM(qtd_total_clientes_ccs_scr) AS total_clientes,
# MAGIC     CASE WHEN SUM(qtd_total_clientes_ccs_scr) > 0
# MAGIC       THEN ROUND(SUM(qtd_reclamacoes) * 1000000.0 / SUM(qtd_total_clientes_ccs_scr), 2)
# MAGIC       ELSE NULL END AS taxa_por_milhao
# MAGIC   FROM workspace.bacen_gold.ranking_reclamacoes
# MAGIC   GROUP BY ano
# MAGIC ),
# MAGIC con_anual AS (
# MAGIC   SELECT 'Consórcios' AS fonte, ano, 'Semestral' AS periodicidade,
# MAGIC     COUNT(DISTINCT administradora) AS qtd_instituicoes,
# MAGIC     SUM(qtd_total_reclamacoes) AS total_reclamacoes,
# MAGIC     ROUND(AVG(indice), 2) AS indice_medio,
# MAGIC     SUM(qtd_clientes_consorciados) AS total_clientes,
# MAGIC     CASE WHEN SUM(qtd_clientes_consorciados) > 0
# MAGIC       THEN ROUND(SUM(qtd_total_reclamacoes) * 1000000.0 / SUM(qtd_clientes_consorciados), 2)
# MAGIC       ELSE NULL END AS taxa_por_milhao
# MAGIC   FROM workspace.bacen_gold.ranking_consorcios
# MAGIC   GROUP BY ano
# MAGIC )
# MAGIC SELECT * FROM rec_anual
# MAGIC UNION ALL SELECT * FROM con_anual
# MAGIC ORDER BY ano, fonte

# COMMAND ----------

# DBTITLE 1,Verificação Gold Comparativo
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- VERIFICAÇÃO DO GOLD COMPARATIVO
# MAGIC -- ============================================================================
# MAGIC -- Lista todas as linhas de comparativo_anual ordenadas por ano e fonte,
# MAGIC -- mostrando periodicidade, qtd_instituicoes, total_reclamacoes,
# MAGIC -- indice_medio e taxa_por_milhao para comparação direta Bancos vs Consórcios.
# MAGIC -- ============================================================================
# MAGIC SELECT ano, fonte, periodicidade, qtd_instituicoes, total_reclamacoes, indice_medio, taxa_por_milhao
# MAGIC FROM workspace.bacen_gold.comparativo_anual
# MAGIC ORDER BY ano, fonte

# COMMAND ----------

# DBTITLE 1,Verificação Final — Todas as Tabelas Gold
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- VERIFICAÇÃO FINAL — TODAS AS TABELAS GOLD
# MAGIC -- ============================================================================
# MAGIC -- Conta os registros de cada uma das 8 tabelas Gold criadas neste
# MAGIC -- notebook (ranking_reclamacoes, resumo_anual_reclamacoes,
# MAGIC -- ranking_ouvidorias, resumo_ouvidorias_anual, ranking_consorcios,
# MAGIC -- resumo_consorcios_anual, reclamacoes_ouvidorias, comparativo_anual)
# MAGIC -- em uma única consulta UNION ALL ordenada por nome da tabela.
# MAGIC -- ============================================================================
# MAGIC SELECT 'ranking_reclamacoes' AS tabela, COUNT(*) AS registros FROM workspace.bacen_gold.ranking_reclamacoes
# MAGIC UNION ALL SELECT 'resumo_anual_reclamacoes', COUNT(*) FROM workspace.bacen_gold.resumo_anual_reclamacoes
# MAGIC UNION ALL SELECT 'ranking_ouvidorias', COUNT(*) FROM workspace.bacen_gold.ranking_ouvidorias
# MAGIC UNION ALL SELECT 'resumo_ouvidorias_anual', COUNT(*) FROM workspace.bacen_gold.resumo_ouvidorias_anual
# MAGIC UNION ALL SELECT 'ranking_consorcios', COUNT(*) FROM workspace.bacen_gold.ranking_consorcios
# MAGIC UNION ALL SELECT 'resumo_consorcios_anual', COUNT(*) FROM workspace.bacen_gold.resumo_consorcios_anual
# MAGIC UNION ALL SELECT 'reclamacoes_ouvidorias', COUNT(*) FROM workspace.bacen_gold.reclamacoes_ouvidorias
# MAGIC UNION ALL SELECT 'comparativo_anual', COUNT(*) FROM workspace.bacen_gold.comparativo_anual
# MAGIC ORDER BY tabela