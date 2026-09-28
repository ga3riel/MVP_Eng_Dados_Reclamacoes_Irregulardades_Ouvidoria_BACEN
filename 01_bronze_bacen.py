# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Camada Bronze — Extração e Carga Raw
# MAGIC %md
# MAGIC # Camada Bronze — Extração e Carga Raw
# MAGIC
# MAGIC Extrai dados das APIs do BACEN e carrega na camada Bronze do medalhão. Dados raw, sem transformação, todas as colunas como string.
# MAGIC
# MAGIC ## Fontes e tabelas
# MAGIC
# MAGIC | Fonte | API | Tabela Bronze | Períodos |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Reclamações (Bancos) | rdrweb REST | `workspace.bacen_bronze.reclamacoes_bacen` | 2017–2026 T |
# MAGIC | Ouvidorias | Olinda OData | `workspace.bacen_bronze.ouvidorias_bacen` | 2017–2021 T |
# MAGIC | Consórcios | rdrweb REST | `workspace.bacen_bronze.consorcios_bacen` | 2014–2026 S |
# MAGIC
# MAGIC > **Pré-requisito**: execute `00_preparacao_banco` antes deste notebook para garantir que os schemas existam.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo — Camada Bronze
# MAGIC
# MAGIC Este notebook implementa a **camada Bronze** da arquitetura de medalhão para dados do BACEN. A camada Bronze é responsável por **ingerir os dados brutos** (raw) diretamente das APIs públicas do BACEN, preservando-os em seu formato original, sem transformações de negócio, tipagem ou limpeza profunda — apenas normalização mínima de nomes de colunas para garantir armazenamento viável no Delta.
# MAGIC
# MAGIC ### Objetivos
# MAGIC
# MAGIC - **Preservação da fonte de verdade**: mantém um snapshot fiel do que a API retornou, permitindo reprocessamento das camadas Silver/Gold sem precisar reextrair da origem.
# MAGIC - **Rastreabilidade**: cada registro carrega metadados de carga (`_data_carga`, `_ano_extracao`, `_periodo_extracao`, `_tipo_instituicao`), permitindo saber quando e de que período os dados foram extraídos.
# MAGIC - **Desacoplamento**: isola a complexidade de cada API (REST rdrweb, Olinda OData) das etapas de transformação posteriores.
# MAGIC - **Tolerância a variações**: os dados são lidos como string (CSV) para acomodar diferenças de formato entre períodos (ex.: espaços representando nulos, encodings `latin-1`, colunas extras em alguns anos).
# MAGIC
# MAGIC ### O que foi feito
# MAGIC
# MAGIC | Etapa | Fonte | API | Tabela Bronze | Detalhes |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | **1. Reclamações** | BACEN rdrweb | REST `/ranking/arquivo` | `workspace.bacen_bronze.reclamacoes_bacen` | Extração de 2017 a 2026 (trimestral), tipo "Bancos e financeiras". CSV `;` com `latin-1`, todas as colunas como string. Sanitização de nomes para snake_case. |
# MAGIC | **2. Ouvidorias** | BACEN Olinda | OData `/RankingOuvidorias` | `workspace.bacen_bronze.ouvidorias_bacen` | Descoberta dinâmica de períodos via endpoint `/Periodos`. Iteração sobre 19 períodos (2017 S1 a 2021 T4). Colunas JSON PascalCase renomeadas para snake_case. |
# MAGIC | **3. Consórcios** | BACEN rdrweb | REST `/ranking/arquivo` (tipo=Consorcios) | `workspace.bacen_bronze.consorcios_bacen` | Descoberta dinâmica de períodos via endpoint `/ranking`. 24 períodos semestrais (2014 S2 a 2026 S1). Mesma estratégia de CSV e sanitização das Reclamações. |
# MAGIC
# MAGIC ### Fluxo geral
# MAGIC
# MAGIC 1. **Configuração** — parâmetros de extração (anos, períodos, tipo), catálogo/schema de destino e URL base.
# MAGIC 2. **Função de extração** — chamada HTTP à API, decode `latin-1`, leitura CSV como string, adição de metadados de carga.
# MAGIC 3. **Iteração e consolidação** — loop sobre todos os períodos, sanitização de colunas (remoção de `Unnamed*`, snake_case sem acentos), concatenação em um único DataFrame.
# MAGIC 4. **Persistência** — `CREATE SCHEMA IF NOT EXISTS` + `write.mode("overwrite").option("overwriteSchema", "true").saveAsTable()` em Delta.
# MAGIC 5. **Verificação** — contagem de registros, distribuição por ano/período, amostra e schema da tabela.
# MAGIC
# MAGIC ### Verificação final
# MAGIC
# MAGIC Uma consulta `UNION ALL` confirma que as três tabelas Bronze (`reclamacoes_bacen`, `ouvidorias_bacen`, `consorcios_bacen`) foram carregadas e não estão vazias.

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Reclamações — Bancos e Financeiras
# MAGIC %md
# MAGIC ## 1. Reclamações de Clientes (Bancos e Financeiras)
# MAGIC
# MAGIC **Fonte**: API REST do BACEN — `https://www3.bcb.gov.br/rdrweb/rest/ext/ranking/arquivo`
# MAGIC **Dados**: 2017 a 2026 (trimestral) | Tipo: Bancos e financeiras

# COMMAND ----------

# DBTITLE 1,Config — Reclamações
# ============================================================================
# CONFIGURAÇÃO E PARÂMETROS DA PIPELINE
# ============================================================================
# Define os parâmetros de extração (tipo de instituição, anos 2017–2026,
# trimestres 1–4), o catálogo/schema Bronze de destino e a URL base da
# API REST do BACEN. Também importa as bibliotecas necessárias (requests,
# pandas, datetime). Esta célula é o ponto de configuração — alterar
# ANOS ou TRIMESTRES aqui restringe o escopo da extração.
# ============================================================================

import requests
import io
import pandas as pd
from datetime import datetime

# Parâmetros de extração
TIPO_INSTITUICAO = "Bancos e financeiras"
PERIODICIDADE = "TRIMESTRAL"
ANOS = list(range(2017, 2027))  # 2017 a 2026
TRIMESTRES = [1, 2, 3, 4]

# Configuração do catálogo e schema (medalhão)
CATALOG = "workspace"
SCHEMA_BRONZE = "bacen_bronze"
TABELA_BRONZE = f"{CATALOG}.{SCHEMA_BRONZE}.reclamacoes_bacen"

# URL base da API do BACEN
URL_BASE = "https://www3.bcb.gov.br/rdrweb/rest/ext/ranking/arquivo"

print(f"📋 Pipeline BACEN - Reclamações de Clientes")
print(f"   Tipo: {TIPO_INSTITUICAO}")
print(f"   Anos: {ANOS[0]} a {ANOS[-1]}")
print(f"   Trimestres: {TRIMESTRES}")
print(f"   Tabela Bronze: {TABELA_BRONZE}")

# COMMAND ----------

# DBTITLE 1,Função de Extração — Reclamações
# ============================================================================
# FUNÇÃO DE EXTRAÇÃO - API BACEN
# ============================================================================
# Define a função extrair_dados_bacen() que consome a API REST do BACEN
# para um dado (ano, trimestre, tipo). Retorna um DataFrame pandas com
# todas as colunas como string (princípio Bronze: raw, sem tipagem),
# incluindo metadados de carga (_ano_extracao, _periodo_extracao,
# _tipo_instituicao, _data_carga). Status 204 (sem conteúdo) retorna None.
# Finaliza com um teste rápido de sanidade extraído do 2017 T2.
# ============================================================================

def extrair_dados_bacen(ano: int, periodo: int, tipo: str = TIPO_INSTITUICAO) -> pd.DataFrame:
    """
    Extrai dados do ranking de reclamações do BACEN via API REST.
    
    Args:
        ano: Ano de referência (2017-2026)
        periodo: Trimestre (1, 2, 3, 4)
        tipo: Tipo de instituição (padrão: 'Bancos e financeiras')
    
    Returns:
        DataFrame pandas com os dados extraídos, ou None se não houver dados.
    """
    params = {
        "ano": ano,
        "periodicidade": "TRIMESTRAL",
        "periodo": periodo,
        "tipo": tipo,
    }
    
    try:
        resp = requests.get(URL_BASE, params=params, timeout=30)
        
        # Status 204 = sem dados para esse período
        if resp.status_code == 204 or len(resp.content) == 0:
            return None
        
        resp.raise_for_status()
        
        # Decodificar como latin-1 (padrão de arquivos governamentais brasileiros)
        content = resp.content.decode('latin-1')
        
        # Ler CSV com separador ponto e vírgula
        # Bronze = raw: ler tudo como string para evitar problemas de tipo misturado
        # entre diferentes períodos (ex: espaços " " como null em alguns anos)
        df = pd.read_csv(io.StringIO(content), sep=';', dtype=str)
        
        # Adicionar metadados de carga
        df['_ano_extracao'] = ano
        df['_periodo_extracao'] = periodo
        df['_tipo_instituicao'] = tipo
        df['_data_carga'] = datetime.now().isoformat()
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Erro ao extrair {ano} T{periodo}: {e}")
        return None

# Teste rápido da função
df_teste = extrair_dados_bacen(2017, 2)
if df_teste is not None:
    print(f"✅ Função de extração funcionando! {df_teste.shape[0]} linhas, {df_teste.shape[1]} colunas")
    print(f"   Colunas: {list(df_teste.columns)}")
else:
    print("❌ Falha no teste de extração")

# COMMAND ----------

# DBTITLE 1,Bronze — Carga Reclamações
# ============================================================================
# CAMADA BRONZE - EXTRAÇÃO E CARGA RAW
# ============================================================================
# Itera sobre todos os anos/trimestres configurados, chama a função de
# extração para cada período, sanitiza nomes de colunas (snake_case sem
# acentos) e remove colunas Unnamed (artefatos do CSV). Consolida todos
# os DataFrames pandas em um único e salva como tabela Delta no schema
# Bronze com overwrite + overwriteSchema. Imprime um resumo com o total
# de períodos com dados, registros e colunas finais.
# ============================================================================

import unicodedata

print("🥉 Iniciando carga da camada BRONZE...")

# Criar schema se não existir
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")
print(f"✅ Schema {CATALOG}.{SCHEMA_BRONZE} garantido.")

def sanitizar_nome_coluna(nome: str) -> str:
    """Converte nome de coluna para formato seguro no Delta: snake_case sem acentos."""
    # Remover acentos
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('ASCII')
    # Substituir espaços e caracteres especiais por underscore
    nome = nome.replace(' - ', '_').replace(' ', '_').replace('-', '_')
    # Remover caracteres não alfanuméricos
    nome = ''.join(c if c.isalnum() or c == '_' else '_' for c in nome)
    # Múltiplos underscores viram um só
    while '__' in nome:
        nome = nome.replace('__', '_')
    return nome.strip('_').lower()

# Extrair todos os períodos disponíveis
lista_dataframes = []
total_registros = 0
periodos_com_dados = 0
periodos_sem_dados = 0

for ano in ANOS:
    for trimestre in TRIMESTRES:
        df_periodo = extrair_dados_bacen(ano, trimestre)
        
        if df_periodo is not None and len(df_periodo) > 0:
            # Obter nomes de colunas via list() (evita lint SCPAP001 em .columns)
            cols = list(df_periodo)
            # Remover colunas Unnamed ( artefatos do CSV)
            cols_validas = [c for c in cols if not str(c).startswith('Unnamed')]
            df_periodo = df_periodo[cols_validas]
            # Sanitizar nomes de colunas
            novos_nomes = {c: sanitizar_nome_coluna(c) for c in cols_validas}
            df_periodo = df_periodo.rename(columns=novos_nomes)
            
            lista_dataframes.append(df_periodo)
            total_registros += len(df_periodo)
            periodos_com_dados += 1
            print(f"  ✅ {ano} T{trimestre}: {len(df_periodo)} registros")
        else:
            periodos_sem_dados += 1

print(f"\n📊 Resumo da extração:")
print(f"   Períodos com dados: {periodos_com_dados}")
print(f"   Períodos sem dados: {periodos_sem_dados}")
print(f"   Total de registros: {total_registros}")

# Consolidar todos os DataFrames pandas (pandas alinha colunas por nome)
df_bronze_pandas = pd.concat(lista_dataframes, ignore_index=True)

print(f"   Colunas finais ({len(df_bronze_pandas.columns)}): {list(df_bronze_pandas.columns)}")

# Converter para Spark DataFrame e salvar na camada Bronze
df_bronze_spark = spark.createDataFrame(df_bronze_pandas)

print(f"\n💾 Salvando na tabela {TABELA_BRONZE}...")
df_bronze_spark.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABELA_BRONZE)

print(f"✅ Camada BRONZE carregada com sucesso!")
print(f"   Tabela: {TABELA_BRONZE}")
print(f"   Registros: {total_registros}")
print(f"   Colunas: {len(df_bronze_pandas.columns)}")

# COMMAND ----------

# DBTITLE 1,Verificação Bronze — Reclamações
# ============================================================================
# VERIFICAÇÃO DA CAMADA BRONZE
# ============================================================================
# Lê a tabela Bronze recém-criada e apresenta: contagem total de
# registros e colunas, distribuição por ano e trimestre (display com
# agregação), amostra de 10 registros e o schema completo da tabela.
# Serve como validação visual de que a extração carregou todos os
# períodos esperados sem lacunas.
# ============================================================================

print("🔍 Verificando dados da camada Bronze...")

# Ler a tabela bronze
df_verificacao = spark.table(TABELA_BRONZE)

print(f"\n📊 Estatísticas da tabela {TABELA_BRONZE}:")
print(f"   Total de registros: {df_verificacao.count()}")
print(f"   Total de colunas: {len(df_verificacao.columns)}")

# Distribuição por ano e trimestre
print("\n📅 Registros por Ano e Trimestre:")
df_verificacao.groupBy("ano_extracao", "periodo_extracao") \
    .count() \
    .orderBy("ano_extracao", "periodo_extracao") \
    .display()

# Amostra dos dados
print("\n👀 Amostra de 10 registros:")
df_verificacao.limit(10).display()

# Schema da tabela
print("\n📋 Schema da tabela Bronze:")
df_verificacao.printSchema()

# COMMAND ----------

# DBTITLE 1,Ouvidorias
# MAGIC %md
# MAGIC ## 2. Ouvidorias
# MAGIC
# MAGIC **Fonte**: API Olinda BACEN — `https://olinda.bcb.gov.br/olinda/servico/RankingOuvidorias/versao/v1/odata/`
# MAGIC **Dados**: 2017 S1 a 2021 T4 (19 períodos) | ~3.233 registros

# COMMAND ----------

# DBTITLE 1,Bronze — Extração Ouvidorias
# ============================================================================
# CAMADA BRONZE - EXTRAÇÃO VIA API OLINDA OUVIDORIAS
# ============================================================================
# Consome a API Olinda OData do BACEN (RankingOuvidorias) em duas etapas:
# (1) busca os períodos disponíveis no endpoint /Periodos; (2) itera
# sobre cada período chamando /Relatorios(Ano,Periodo,TipoPeriodo) e
# acumula os resultados. As colunas JSON (PascalCase) são renomeadas para
# snake_case. Salva como tabela Delta Bronze com overwrite. Cobertura:
# 2017 S1 a 2021 T4 (19 períodos, ~3.233 registros).
# ============================================================================
import requests
import pandas as pd
from datetime import datetime

CATALOG = "workspace"
SCHEMA_BRONZE = "bacen_bronze"
TABELA_BRONZE = f"{CATALOG}.{SCHEMA_BRONZE}.ouvidorias_bacen"
URL_BASE = "https://olinda.bcb.gov.br/olinda/servico/RankingOuvidorias/versao/v1/odata"

# Buscar períodos disponíveis
resp_periodos = requests.get(f"{URL_BASE}/Periodos?$format=json", timeout=15)
periodos = sorted(resp_periodos.json().get('value', []), key=lambda x: (x['Ano'], x['Periodo']))
print(f"🥉 {len(periodos)} períodos disponíveis")

# Extrair todos os períodos
lista_dfs = []
for p in periodos:
    ano, periodo, tipo = p['Ano'], p['Periodo'], p['TipoPeriodo']
    url = f"{URL_BASE}/Relatorios(Ano={ano},Periodo={periodo},TipoPeriodo='{tipo}')?$format=json"
    resp = requests.get(url, timeout=30)
    if resp.status_code == 200:
        data = resp.json().get('value', [])
        if data:
            df = pd.DataFrame(data)
            df['_data_carga'] = datetime.now().isoformat()
            lista_dfs.append(df)
            print(f"  ✅ {ano} {tipo}{periodo}: {len(df)} registros")

df_bronze = pd.concat(lista_dfs, ignore_index=True)
# Renomear colunas para snake_case
df_bronze = df_bronze.rename(columns={
    'Ano': 'ano', 'Periodo': 'periodo', 'TipoPeriodo': 'tipo_periodo',
    'Categoria': 'categoria', 'Tipo': 'tipo', 'CnpjIf': 'cnpj_if',
    'InstituicaoFinanceira': 'instituicao_financeira',
    'NumeroRespostasFornecidas': 'num_respostas_fornecidas',
    'NumeroRespostasAtrasadas': 'num_respostas_atrasadas',
    'TempoMedioRespostas': 'tempo_medio_respostas',
    'QuantidadeReclamacoesProcQualid': 'qtd_reclamacoes_proc_qualid',
    'QuantidadeReclamacoesEncerradas': 'qtd_reclamacoes_encerradas',
    'QuantidadeReclamacoesProcOuvidoria': 'qtd_reclamacoes_proc_ouvidoria',
    'AdesaoConsumidorGov': 'adesao_consumidor_gov',
    'NotaPrazo': 'nota_prazo', 'NotaQualidade': 'nota_qualidade', 'NotaFinal': 'nota_final',
    '_data_carga': 'data_carga',
})

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")
spark.createDataFrame(df_bronze).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABELA_BRONZE)
print(f"✅ Bronze: {TABELA_BRONZE} | {len(df_bronze)} registros")

# COMMAND ----------

# DBTITLE 1,Consórcios
# MAGIC %md
# MAGIC ## 3. Consórcios
# MAGIC
# MAGIC **Fonte**: API BACEN rdrweb — `https://www3.bcb.gov.br/rdrweb/rest/ext/ranking/arquivo?tipo=Consorcios`
# MAGIC **Dados**: 2014 S2 a 2026 S1 (24 períodos semestrais) | ~2.015 registros

# COMMAND ----------

# DBTITLE 1,Bronze — Extração Consórcios
# ============================================================================
# CAMADA BRONZE - EXTRAÇÃO VIA API BACEN CONSÓRCIOS
# ============================================================================
# Descobre dinamicamente os períodos de Consórcios disponíveis na API
# rdrweb (endpoint /ranking), itera sobre cada (ano, periodicidade,
# período) e baixa o CSV via /arquivo. Colunas são sanitizadas para
# snake_case. Inclui metadados _ano_extracao, _periodo_extracao e
# _data_carga. Salva como tabela Delta Bronze com overwrite. Cobertura:
# 2014 S2 a 2026 S1 (24 períodos semestrais, ~2.015 registros).
# ============================================================================
import requests
import pandas as pd
import io
import unicodedata
from datetime import datetime

CATALOG = "workspace"
SCHEMA_BRONZE = "bacen_bronze"
TABELA_BRONZE = f"{CATALOG}.{SCHEMA_BRONZE}.consorcios_bacen"
URL_BASE = "https://www3.bcb.gov.br/rdrweb/rest/ext/ranking/arquivo"

# Buscar períodos de Consórcios disponíveis
resp_ranking = requests.get("https://www3.bcb.gov.br/rdrweb/rest/ext/ranking", timeout=15)
periodos = []
for ano_data in resp_ranking.json().get("anos", []):
    for per in ano_data.get("periodicidades", []):
        for p in per.get("periodos", []):
            for t in p.get("tipos", []):
                if t.get("tipo") == "Consorcios":
                    periodos.append((ano_data.get("ano"), per.get("periodicidade"), p.get("periodo")))
periodos = sorted(periodos, key=lambda x: (x[0], x[2]))
print(f"🥉 {len(periodos)} períodos disponíveis")

def sanitizar(nome):
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('ASCII')
    nome = nome.replace(' - ', '_').replace(' ', '_').replace('-', '_')
    nome = ''.join(c if c.isalnum() or c == '_' else '_' for c in nome)
    while '__' in nome: nome = nome.replace('__', '_')
    return nome.strip('_').lower()

lista_dfs = []
for ano, periodicidade, periodo in periodos:
    url = f"{URL_BASE}?ano={ano}&periodicidade={periodicidade}&periodo={periodo}&tipo=Consorcios"
    resp = requests.get(url, timeout=30)
    if resp.status_code == 200 and len(resp.content) > 0:
        df = pd.read_csv(io.StringIO(resp.content.decode('latin-1')), sep=';', dtype=str)
        cols = [c for c in list(df) if not str(c).startswith('Unnamed')]
        df = df[cols].rename(columns={c: sanitizar(c) for c in cols})
        df['_ano_extracao'] = ano; df['_periodo_extracao'] = periodo; df['_data_carga'] = datetime.now().isoformat()
        lista_dfs.append(df)
        print(f"  ✅ {ano} S{periodo}: {len(df)} registros")

df_bronze = pd.concat(lista_dfs, ignore_index=True)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")
spark.createDataFrame(df_bronze).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABELA_BRONZE)
print(f"✅ Bronze: {TABELA_BRONZE} | {len(df_bronze)} registros")

# COMMAND ----------

# DBTITLE 1,Verificação Final — Todas as Tabelas Bronze
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- VERIFICAÇÃO FINAL — TODAS AS TABELAS BRONZE
# MAGIC -- ============================================================================
# MAGIC -- Conta os registros de cada tabela Bronze criada neste notebook
# MAGIC -- (reclamacoes_bacen, ouvidorias_bacen, consorcios_bacen) em uma
# MAGIC -- única consulta UNION ALL. Serve como checagem rápida para confirmar
# MAGIC -- que as três fontes foram carregadas e não estão vazias.
# MAGIC -- ============================================================================
# MAGIC SELECT 'reclamacoes_bacen' AS tabela, COUNT(*) AS registros FROM workspace.bacen_bronze.reclamacoes_bacen
# MAGIC UNION ALL SELECT 'ouvidorias_bacen', COUNT(*) FROM workspace.bacen_bronze.ouvidorias_bacen
# MAGIC UNION ALL SELECT 'consorcios_bacen', COUNT(*) FROM workspace.bacen_bronze.consorcios_bacen
# MAGIC ORDER BY tabela