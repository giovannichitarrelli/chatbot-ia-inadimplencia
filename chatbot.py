import streamlit as st
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_core.chat_history import InMemoryChatMessageHistory
import httpx
import pandas as pd
from PIL import Image
import time
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from insights import generate_advanced_insights
from insights_projecao import generate_projection_insights
from urllib.parse import quote_plus

load_dotenv()

api_key = os.getenv("API_KEY")
st.set_page_config(page_title="Análise de Inadimplência", page_icon="")

if "app_initialized" not in st.session_state:
    st.session_state.app_initialized = False
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

def get_llm_client():
    return ChatOpenAI(
        api_key=api_key,
        base_url="https://api.deepseek.com",
        model="deepseek-chat",
        http_client=httpx.Client(verify=False)
    )

def connect_to_db():
    try:
        host = os.getenv("SERVER")
        database = os.getenv("DATABASE")
        username = os.getenv("USERNAME")
        password = os.getenv("PASSWORD")
        port = os.getenv("PORT")

        if not all([host, database, username, password, port]):
            raise ValueError("Uma ou mais variáveis de ambiente não estão definidas no .env")
        
        encoded_password = quote_plus(password)
        connection_string = f"postgresql://{username}:{encoded_password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)

        with engine.connect() as connection:
            print("Conexão com o banco de dados estabelecida com sucesso!")
        
        return engine
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

@st.cache_data
def load_insights(_conn):
    """Carrega apenas os insights iniciais leves, sem carregar tabelas completas."""
    try:
        # Carregar uma amostra pequena para gerar insights iniciais
        table = "table_agg_inad_consolidado"
        query_sample = f"SELECT * FROM {table} LIMIT 100"  # Limite pequeno para teste
        df_sample = pd.read_sql(query_sample, _conn)
        insights = generate_advanced_insights(df_sample)

        table_projecao = "projecao_consolidado"
        query_projecao_sample = f"SELECT * FROM {table_projecao} LIMIT 100"
        df_projecao_sample = pd.read_sql(query_projecao_sample, _conn)
        projection_insights = generate_projection_insights(df_projecao_sample)

        combined_insights = f"{insights}\n\nProjeções:\n{projection_insights}"
        return combined_insights
    except Exception as e:
        st.error(f"Erro ao carregar insights iniciais: {str(e)}")
        raise e

def classify_user_intent(prompt, llm):
    intent_prompt = ChatPromptTemplate.from_messages([
        ("system", """
        Analise a pergunta do usuário sobre inadimplência e classifique a intenção em uma das seguintes categorias:
        1. COMPARAÇÃO - Perguntas que comparam diferentes aspectos (ex: "Compare PF e PJ")
        2. RANKING - Perguntas sobre "maior", "menor", "top", etc. (ex: "Qual estado com maior inadimplência?")
        3. ESPECÍFICO - Perguntas sobre um atributo específico (ex: "Valor de inadimplência em São Paulo")
        4. TENDÊNCIA - Perguntas sobre evolução temporal (ex: "Como evoluiu a inadimplência")
        5. GERAL - Perguntas gerais sobre inadimplência
        6. PROJEÇÃO - Perguntas sobre projeção (ex: "Qual projeção de inadimplência para os próximos 5 anos?")	
        
        Responda apenas com o número da categoria mais adequada (1, 2, 3, 4, 5 ou 6).
        """),
        ("human", "{input}")
    ])
    
    intent_chain = intent_prompt | llm
    intent_result = intent_chain.invoke({"input": prompt})
    
    intent_number = ''.join(filter(str.isdigit, intent_result.content[:2]))
    
    intent_mapping = {
        "1": "COMPARAÇÃO",
        "2": "RANKING",
        "3": "ESPECÍFICO",
        "4": "TENDÊNCIA",
        "5": "GERAL",
        "6": "PROJEÇÃO" 
    }
    
    return intent_mapping.get(intent_number, "GERAL")

def generate_dynamic_query(intent, prompt, llm, table_name="table_agg_inad_consolidado"):
    if intent == "PROJEÇÃO":
        query_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""
            Você é um especialista em SQL que transforma perguntas sobre inadimplência em consultas SQL precisas.

            A tabela principal se chama 'projecao_consolidado' e contém as seguintes colunas:
            - ano_mes (ano e mês da projeção)
            - porte (porte do cliente: Pequeno, Médio, Grande)
            - uf (unidade federativa, siglas dos estados brasileiros)
            - cliente (tipo de cliente: PF ou PJ)
            - modalidade (modalidade da operação de crédito)
            - tipo (tipo de cliente: PF ou PJ)
            - soma_ativo_problematico (soma dos ativos problemáticos)
            - soma_carteira_inadimplida_arrastada (soma da carteira inadimplida arrastada)

            Com base na pergunta abaixo, gere uma consulta SQL que retorne os dados necessários.
            Para consultas de PROJEÇÃO, utilize filtros por ano_mes, uf, porte, cliente, modalidade ou tipo, e agregue os valores conforme necessário.

            IMPORTANTE: Retorne APENAS o código SQL, sem explicações ou comentários.
            """),
            ("human", "{input}")
        ])   
    else:
        query_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""
            Você é um especialista em SQL que transforma perguntas sobre inadimplência em consultas SQL precisas.

            A tabela principal se chama '{table_name}' e contém as seguintes colunas:
            - data_base (data de referência dos dados)
            - uf (unidade federativa, siglas dos estados brasileiros)
            - cliente (tipo de cliente: PF ou PJ)
            - ocupacao (ocupações para PF)
            - cnae_secao (setores de atuação para PJ)
            - porte (porte do cliente: Pequeno, Médio, Grande)
            - modalidade (modalidade da operação de crédito)
            - soma_a_vencer_ate_90_dias
            - soma_numero_de_operacoes
            - soma_carteira_ativa
            - soma_carteira_inadimplida_arrastada
            - soma_ativo_problematico
            - media_a_vencer_ate_90_dias
            - media_numero_de_operacoes
            - media_carteira_ativa
            - media_carteira_inadimplida_arrastada
            - media_ativo_problematico
            - min_a_vencer_ate_90_dias
            - min_numero_de_operacoes
            - min_carteira_ativa
            - min_carteira_inadimplida_arrastada
            - min_ativo_problematico
            - max_a_vencer_ate_90_dias
            - max_numero_de_operacoes
            - max_carteira_ativa
            - max_carteira_inadimplida_arrastada
            - max_ativo_problematico

            A intenção do usuário foi classificada como: {intent}

            Com base nesta intenção e na pergunta abaixo, gere uma consulta SQL que retorne os dados necessários.
            Para consultas de RANKING, use ORDER BY e LIMIT.
            Para consultas de COMPARAÇÃO, use GROUP BY para os itens comparados.
            Para consultas ESPECÍFICAS, use filtros WHERE adequados.
            Para consultas de TENDÊNCIA, utilize agrupamento por data_base.

            IMPORTANTE: Retorne APENAS o código SQL, sem explicações ou comentários.
            """),
            ("human", "{input}")
        ])
    query_chain = query_prompt | llm
    sql_result = query_chain.invoke({"input": prompt})
    
    sql_query = sql_result.content.strip()
    if sql_query.startswith("```sql"):
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    
    return sql_query

def process_question_with_insights(prompt, intent, dynamic_query, insights, llm, conn):
    try:
        dynamic_results = pd.read_sql(dynamic_query, conn)
    except Exception as e:
        print(f"Erro ao executar consulta dinâmica: {e}")
        dynamic_results = "Não foi possível gerar resultados dinâmicos específicos."

    processing_prompt = ChatPromptTemplate.from_messages([
        ("system", f"""
        Você é um especialista em análise de inadimplência no Brasil.
        
        A pergunta do usuário foi classificada como: {intent}
        
        Responda à pergunta usando estas duas fontes de informação:
        
        1. INSIGHTS PRÉ-CALCULADOS:
        {insights}
        
        2. RESULTADOS DINÂMICOS DA CONSULTA:
        {dynamic_results}
        
        Priorize os resultados dinâmicos pois são mais relevantes para a pergunta específica.
        Use os insights pré-calculados para complementar sua resposta com contexto adicional.
        
        Formate os valores em reais (R$) com duas casas decimais e separadores de milhar.
        Seja conciso e direto, destacando os pontos mais relevantes para a pergunta do usuário.
        """),
        ("human", "{input}")
    ])
    
    processing_chain = processing_prompt | llm
    response = processing_chain.invoke({"input": prompt})
    
    return response.content

def main():
    st.title("💬 Chatbot Inadimplinha")
    st.caption("🚀 Chatbot Inadimplinha desenvolvido por Grupo de Inadimplência EY")

    conn = connect_to_db()
    if conn is None:
        st.error("Falha na conexão com o banco de dados. Verifique as credenciais.")
        st.stop()
    
    llm = get_llm_client()
    
    # Carregar insights iniciais leves
    if "insights" not in st.session_state:
        st.session_state.insights = load_insights(conn)

    prompt_template = ChatPromptTemplate.from_messages([
        ("system", (
            "Você é um especialista em análise de inadimplência no Brasil. "
            "Responda a pergunta do usuário com base nos dados reais de dezembro de 2024 da tabela 'table_agg_inad_consolidado' "
            "usando os insights detalhados abaixo como fonte principal. "
            "Os insights foram gerados a partir de uma amostra dos dados reais do banco. "
            "Se a pergunta não for respondida pelos insights, informe que mais dados são necessários e sugira verificar a fonte. "
            "Formate os valores em reais (R$) com duas casas decimais e separadores de milhar.\n\n"
            "Insights gerados:\n{insights}"
        )),
        ("human", "{input}")
    ])
    
    chain = prompt_template | llm

    if "chat_history_store" not in st.session_state:
        st.session_state.chat_history_store = InMemoryChatMessageHistory()

    conversation = RunnableWithMessageHistory(
        runnable=chain,
        get_session_history=lambda: st.session_state.chat_history_store,
        input_messages_key="input",
        history_messages_key="chat_history"
    )

    if not st.session_state.app_initialized and not st.session_state.chat_history:
        initial_message = "Como posso te ajudar hoje?"
        st.session_state.chat_history.append({"role": "assistant", "content": initial_message})
        st.session_state.chat_history_store.add_ai_message(initial_message)
        st.session_state.app_initialized = True

    for message in st.session_state.chat_history:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("Faça uma pergunta sobre a inadimplência"):
        with st.chat_message("user"):
            st.markdown(prompt)
        
        st.session_state.chat_history.append({"role": "user", "content": prompt})
        
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            try:
                with st.spinner("Processando..."):
                    intent = classify_user_intent(prompt, llm)
                    print(f"Intenção classificada como: {intent}")
                    
                    dynamic_query = generate_dynamic_query(intent, prompt, llm)
                    print(f"Consulta dinâmica gerada: {dynamic_query}")
                    
                    if intent != "GERAL":
                        response_content = process_question_with_insights(
                            prompt, intent, dynamic_query, st.session_state.insights, llm, conn
                        )
                    else:
                        response = conversation.invoke(
                            {"input": prompt, "insights": st.session_state.insights},
                            config={"configurable": {"session_id": "default"}}
                        )
                        response_content = response.content

                    full_response = ""
                    for i in range(len(response_content)):
                        full_response = response_content[:i+1]
                        message_placeholder.markdown(full_response + "▌")
                        time.sleep(0.01)
                    message_placeholder.markdown(full_response)
                    
                    st.session_state.chat_history.append({"role": "assistant", "content": full_response})
                    st.session_state.chat_history_store.add_ai_message(full_response)
            except Exception as e:
                error_message = f"Erro no processamento: {str(e)}"
                message_placeholder.markdown(error_message)
                st.session_state.chat_history.append({"role": "assistant", "content": error_message})
                st.session_state.chat_history_store.add_ai_message(error_message)

    with st.sidebar:
        ey_logo = Image.open(r"EY_Logo.png")
        ey_logo_resized = ey_logo.resize((100, 100))   
        st.sidebar.image(ey_logo_resized)
        st.sidebar.header("EY Academy | Inadimplência")

        st.sidebar.subheader("🔍 Sugestões de Análise")
        st.sidebar.write("➡️ Qual estado com maior inadimplência e quais os valores devidos?")
        st.sidebar.write("➡️ Qual tipo de cliente apresenta o maior número de operações?")
        st.sidebar.write("➡️ Em qual modalidade existe maior inadimplência?")
        st.sidebar.write("➡️ Compare a inadimplência entre PF e PJ")
        st.sidebar.write("➡️ Qual ocupação entre PF possui maior inadimplência?")
        st.sidebar.write("➡️ Qual o principal porte de cliente com inadimplência entre PF?")
        st.sidebar.write("➡️ Qual região apresenta a maior taxa de inadimplência?")
        st.sidebar.write("➡️ Quais os setores econômicos com maior volume de inadimplência?")
        st.sidebar.write("➡️ Qual a projeção de inadimplência para os próximos 90 dias?")
        st.sidebar.write("➡️ Qual o índice de ativo problemático por tipo de cliente?")
        st.sidebar.write("➡️ Quais as modalidades de crédito com maior risco de inadimplência?")

        if st.button("Limpar Conversa"):
            st.session_state.chat_history_store = InMemoryChatMessageHistory()
            st.session_state.chat_history = []
            st.session_state.app_initialized = False
            st.rerun()

    conn.dispose()

if __name__ == "__main__":
    main()
# import streamlit as st
# from langchain_openai import ChatOpenAI
# from langchain_core.prompts import ChatPromptTemplate
# from langchain_core.runnables.history import RunnableWithMessageHistory
# from langchain_core.chat_history import InMemoryChatMessageHistory
# import httpx
# import pandas as pd
# from PIL import Image
# import time
# import os
# from dotenv import load_dotenv
# from sqlalchemy import create_engine
# from insights import generate_advanced_insights
# from insights_projecao import generate_projection_insights
# from urllib.parse import quote_plus
 
# load_dotenv()

# api_key = os.getenv("API_KEY")
# st.set_page_config(page_title="Análise de Inadimplência", page_icon="")

# if "app_initialized" not in st.session_state:
#     st.session_state.app_initialized = False
# if "chat_history" not in st.session_state:
#     st.session_state.chat_history = []
# if "data_loaded" not in st.session_state:
#     st.session_state.data_loaded = False

# def get_llm_client():
#     return ChatOpenAI(
#         api_key=api_key,
#         base_url="https://api.deepseek.com",
#         model="deepseek-chat",
#         http_client=httpx.Client(verify=False)
#     )

# def connect_to_db():
#     try:
#         # Dados de conexão
#         host = os.getenv("SERVER")
#         database = os.getenv("DATABASE")
#         username = os.getenv("USERNAME")
#         password = os.getenv("PASSWORD")
#         port = os.getenv("PORT")

  
#         if not all([host, database, username, password, port]):
#             raise ValueError("Uma ou mais variáveis de ambiente não estão definidas no .env")
        
#         encoded_password = quote_plus(password)
#         connection_string = f"postgresql://{username}:{encoded_password}@{host}:{port}/{database}"
#         engine = create_engine(connection_string)

#         with engine.connect() as connection:
#             print("Conexão com o banco de dados estabelecida com sucesso!")
        
#         return engine

#     except Exception as e:
#         print(f"Erro ao conectar ao banco de dados: {e}")
#         return None

  
# @st.cache_data
# def load_data(_conn):
#     """Carrega os dados e gera os insights sob demanda."""
#     try:
#         # Carregar dados da tabela principal
#         table = "table_agg_inad_consolidado"
#         query = f"SELECT * FROM {table}"
#         df = pd.read_sql(query, _conn)
#         insights = generate_advanced_insights(df)

#         # Carregar dados da tabela de projeções
#         table_projecao = "projecao_consolidado"
#         query_projecao = f"SELECT * FROM {table_projecao}"
#         df_projecao = pd.read_sql(query_projecao, _conn)
#         projection_insights = generate_projection_insights(df_projecao)

#         # Combinar os insights
#         combined_insights = f"{insights}\n\nProjeções:\n{projection_insights}"
        
#         st.session_state.df = df
#         st.session_state.df_projecao = df_projecao
#         st.session_state.insights = combined_insights
#         st.session_state.data_loaded = True
#         print("Dados carregados com sucesso!")
#     except Exception as e:
#         st.error(f"Erro ao carregar dados: {str(e)}")
#         st.session_state.data_loaded = False
#         raise e

    

# def classify_user_intent(prompt, llm):
#     """
#     Classifica a intenção do usuário para determinar o tipo de consulta necessária
#     """
#     intent_prompt = ChatPromptTemplate.from_messages([
#         ("system", """
#         Analise a pergunta do usuário sobre inadimplência e classifique a intenção em uma das seguintes categorias:
#         1. COMPARAÇÃO - Perguntas que comparam diferentes aspectos (ex: "Compare PF e PJ")
#         2. RANKING - Perguntas sobre "maior", "menor", "top", etc. (ex: "Qual estado com maior inadimplência?")
#         3. ESPECÍFICO - Perguntas sobre um atributo específico (ex: "Valor de inadimplência em São Paulo")
#         4. TENDÊNCIA - Perguntas sobre evolução temporal (ex: "Como evoluiu a inadimplência")
#         5. GERAL - Perguntas gerais sobre inadimplência
#         6. PROJEÇÃO - Perguntas sobre projeção (ex: "Qual projeção de inadimplência para os próximos 5 anos?")	
        
#         Responda apenas com o número da categoria mais adequada (1, 2, 3, 4 ou 5).
#         """),
#         ("human", "{input}")
#     ])
    
#     intent_chain = intent_prompt | llm
#     intent_result = intent_chain.invoke({"input": prompt})
    
#     # Extrair apenas o número da classificação
#     intent_number = ''.join(filter(str.isdigit, intent_result.content[:2]))
    
#     intent_mapping = {
#         "1": "COMPARAÇÃO",
#         "2": "RANKING",
#         "3": "ESPECÍFICO",
#         "4": "TENDÊNCIA",
#         "5": "GERAL",
#         "6": "PROJEÇÃO" 
#     }
    
#     return intent_mapping.get(intent_number, "GERAL")

# def generate_dynamic_query(intent, prompt, llm, table_name="table_agg_inad_consolidado"):
#     """
#     Gera uma consulta SQL dinâmica com base na intenção do usuário e na pergunta
#     """
#     if intent == "PROJEÇÃO":
#         query_prompt = ChatPromptTemplate.from_messages([
#                     ("system", f"""
#                     Você é um especialista em SQL que transforma perguntas sobre inadimplência em consultas SQL precisas.

#                     A tabela principal se chama 'projecao_consolidado' e contém as seguintes colunas:

#                     - ano_mes (ano e mês da projeção)
#                     - porte (porte do cliente: Pequeno, Médio, Grande)
#                     - uf (unidade federativa, siglas dos estados brasileiros)
#                     - cliente (tipo de cliente: PF ou PJ)
#                     - modalidade (modalidade da operação de crédito)
#                     - tipo (tipo de cliente: PF ou PJ)
#                     - soma_ativo_problematico (soma dos ativos problemáticos)
#                     - soma_carteira_inadimplida_arrastada (soma da carteira inadimplida arrastada)

#                     Com base na pergunta abaixo, gere uma consulta SQL que retorne os dados necessários.
#                     Para consultas de PROJEÇÃO, utilize filtros por ano_mes, uf, porte, cliente, modalidade ou tipo, e agregue os valores conforme necessário.

#                     IMPORTANTE: Retorne APENAS o código SQL, sem explicações ou comentários.
#                     """),
#                     ("human", "{input}")
#                 ])   
#     else:
#         query_prompt = ChatPromptTemplate.from_messages([
#             ("system", f"""
#             Você é um especialista em SQL que transforma perguntas sobre inadimplência em consultas SQL precisas.

#             A tabela principal se chama '{table_name}' e contém as seguintes colunas:

#             - data_base (data de referência dos dados)
#             - uf (unidade federativa, siglas dos estados brasileiros)
#             - cliente (tipo de cliente: PF ou PJ)
#             - ocupacao (ocupações para PF)
#             - cnae_secao (setores de atuação para PJ)
#             - porte (porte do cliente: Pequeno, Médio, Grande)
#             - modalidade (modalidade da operação de crédito)
            
#             As colunas a seguir representam agregados estatísticos:
#             - soma_a_vencer_ate_90_dias
#             - soma_numero_de_operacoes
#             - soma_carteira_ativa
#             - soma_carteira_inadimplida_arrastada
#             - soma_ativo_problematico
#             - media_a_vencer_ate_90_dias
#             - media_numero_de_operacoes
#             - media_carteira_ativa
#             - media_carteira_inadimplida_arrastada
#             - media_ativo_problematico
#             - min_a_vencer_ate_90_dias
#             - min_numero_de_operacoes
#             - min_carteira_ativa
#             - min_carteira_inadimplida_arrastada
#             - min_ativo_problematico
#             - max_a_vencer_ate_90_dias
#             - max_numero_de_operacoes
#             - max_carteira_ativa
#             - max_carteira_inadimplida_arrastada
#             - max_ativo_problematico

#             A intenção do usuário foi classificada como: {intent}

#             Com base nesta intenção e na pergunta abaixo, gere uma consulta SQL que retorne os dados necessários.
#             Para consultas de RANKING, use ORDER BY e LIMIT.
#             Para consultas de COMPARAÇÃO, use GROUP BY para os itens comparados.
#             Para consultas ESPECÍFICAS, use filtros WHERE adequados.
#             Para consultas de TENDÊNCIA, utilize agrupamento por data_base.

#             IMPORTANTE: Retorne APENAS o código SQL, sem explicações ou comentários.
#             """),
#             ("human", "{input}")
#         ])
#     query_chain = query_prompt | llm
#     sql_result = query_chain.invoke({"input": prompt})
    
#     # Limpar a resposta para garantir que seja apenas SQL

#     sql_query = sql_result.content.strip()
#     if sql_query.startswith("```sql"):
#         sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    
#     return sql_query

# def process_question_with_insights(prompt, intent, dynamic_query, df, insights, llm, conn, df_projecao):
#     """
#     Processa a pergunta usando insights estáticos e dados dinâmicos da consulta
#     """
#     try:
#         # Verificar se a intenção é sobre projeções
#         if intent == "PROJEÇÃO" and df_projecao is not None:
#             # Use a tabela de projeções
#             dynamic_results = pd.read_sql(dynamic_query, conn)
#         elif hasattr(df, 'con'):
#             dynamic_results = pd.read_sql(dynamic_query, df.con)
#         elif "SELECT" not in dynamic_query.upper():
#             dynamic_results = df.query(dynamic_query)
#         else:
#             dynamic_results = pd.read_sql(dynamic_query, conn)
#     except Exception as e:
#         print(f"Erro ao executar consulta dinâmica: {e}")
#         dynamic_results = "Não foi possível gerar resultados dinâmicos específicos."

#     # Preparar o contexto combinado
#     processing_prompt = ChatPromptTemplate.from_messages([
#         ("system", f"""
#         Você é um especialista em análise de inadimplência no Brasil.
        
#         A pergunta do usuário foi classificada como: {intent}
        
#         Responda à pergunta usando estas duas fontes de informação:
        
#         1. INSIGHTS PRÉ-CALCULADOS:
#         {insights}
        
#         2. RESULTADOS DINÂMICOS DA CONSULTA:
#         {dynamic_results}
        
#         Priorize os resultados dinâmicos pois são mais relevantes para a pergunta específica.
#         Use os insights pré-calculados para complementar sua resposta com contexto adicional.
        
#         Formate os valores em reais (R$) com duas casas decimais e separadores de milhar.
#         Seja conciso e direto, destacando os pontos mais relevantes para a pergunta do usuário.
#         """),
#         ("human", "{input}")
#     ])
    
#     processing_chain = processing_prompt | llm
#     response = processing_chain.invoke({"input": prompt})
    
#     return response.content

# def main():
#     st.title("💬 Chatbot Inadimplinha")
#     st.caption("🚀 Chatbot Inadimplinha desenvolvido por Grupo de Inadimplência EY")

 
#     conn = connect_to_db()
#     if conn is None:
#         st.stop()
    
 
#     llm = get_llm_client()
    
#     # if not st.session_state.data_loaded:
#     #     st.info("Carregando... Isso pode levar alguns segundos na primeira interação.")
    
#     prompt_template = ChatPromptTemplate.from_messages([
#         ("system", (
#             "Você é um especialista em análise de inadimplência no Brasil. "
#             "Responda a pergunta do usuário com base nos dados reais de dezembro de 2024 da tabela 'table_agg_inad_consolidado' sem informar que está usando apenas os dados de dezembro de 2024, "
#             "usando os insights detalhados abaixo como fonte principal. "
#             "Os insights foram gerados a partir dos dados reais do banco e contêm valores totais e análises segmentadas. "
#             "Extraia a resposta diretamente dos insights quando possível, sem inventar valores. "
#             "Se a pergunta não for respondida pelos insights ou se os insights indicarem que não há dados, "
#             "informe que os dados de dezembro de 2024 não estão disponíveis e sugira verificar a fonte. "
#             "Formate os valores em reais (R$) com duas casas decimais e separadores de milhar. "
#             "Inclua informações adicionais relevantes sobre inadimplência quando apropriado.\n\n"
#             "Insights gerados:\n{insights}"
#         )),
#         ("human", "{input}")
#     ])
    
#     chain = prompt_template | llm

  
#     if "chat_history_store" not in st.session_state:
#         st.session_state.chat_history_store = InMemoryChatMessageHistory()

 
#     conversation = RunnableWithMessageHistory(
#         runnable=chain,
#         get_session_history=lambda: st.session_state.chat_history_store,
#         input_messages_key="input",
#         history_messages_key="chat_history"
#     )

 
#     if not st.session_state.app_initialized and not st.session_state.chat_history:
#         initial_message = "Como posso te ajudar hoje?"
#         st.session_state.chat_history.append({"role": "assistant", "content": initial_message})
#         st.session_state.chat_history_store.add_ai_message(initial_message)
#         st.session_state.app_initialized = True

 
#     for message in st.session_state.chat_history:
#         with st.chat_message(message["role"]):
#             st.markdown(message["content"])

#     if prompt := st.chat_input("Faça uma pergunta sobre a inadimplência"):
#         # Adicionar a pergunta do usuário à interface de chat
#         with st.chat_message("user"):
#             st.markdown(prompt)
        
    
#         st.session_state.chat_history.append({"role": "user", "content": prompt})
        
 
#         with st.chat_message("assistant"):
#             message_placeholder = st.empty()
#             try:
#                     with st.spinner("Processando..."):
#                         # Carregar dados na primeira interação, se ainda não carregados
#                         if not st.session_state.data_loaded:
#                             load_data(conn)

#                         # Classificar intenção e processar a pergunta
#                         intent = classify_user_intent(prompt, llm)
#                         print(f"Intenção classificada como: {intent}")
                        
#                         dynamic_query = generate_dynamic_query(intent, prompt, llm)
#                         print(f"Consulta dinâmica gerada: {dynamic_query}")
                        
#                     if intent != "GERAL":
#                         response_content = process_question_with_insights(
#                             prompt, intent, dynamic_query, st.session_state.df,
#                             st.session_state.insights, llm, conn, st.session_state.df_projecao
#                         )
#                     else:
#                         response = conversation.invoke(
#                             {"input": prompt, "insights": st.session_state.insights},
#                             config={"configurable": {"session_id": "default"}}
#                         )
#                         response_content = response.content

#                     # Simular streaming
#                     full_response = ""
#                     for i in range(len(response_content)):
#                         full_response = response_content[:i+1]
#                         message_placeholder.markdown(full_response + "▌")
#                         time.sleep(0.01)
#                     message_placeholder.markdown(full_response)
                    
#                     st.session_state.chat_history.append({"role": "assistant", "content": full_response})
#                     st.session_state.chat_history_store.add_ai_message(full_response)

#             except Exception as e:
#                 error_message = f"Erro no processamento: {str(e)}"
#                 message_placeholder.markdown(error_message)
#                 st.session_state.chat_history.append({"role": "assistant", "content": error_message})
#                 st.session_state.chat_history_store.add_ai_message(error_message)

#     with st.sidebar:
#         ey_logo = Image.open(r"EY_Logo.png")
#         ey_logo_resized = ey_logo.resize((100, 100))   
#         st.sidebar.image(ey_logo_resized)
#         st.sidebar.header("EY Academy | Inadimplência")

#         st.sidebar.subheader("🔍 Sugestões de Análise")
#         st.sidebar.write("➡️ Qual estado com maior inadimplência e quais os valores devidos?")
#         st.sidebar.write("➡️ Qual tipo de cliente apresenta o maior número de operações?")
#         st.sidebar.write("➡️ Em qual modalidade existe maior inadimplência?")
#         st.sidebar.write("➡️ Compare a inadimplência entre PF e PJ")
#         st.sidebar.write("➡️ Qual ocupação entre PF possui maior inadimplência?")
#         st.sidebar.write("➡️ Qual o principal porte de cliente com inadimplência entre PF?")
#         st.sidebar.write("➡️ Qual região apresenta a maior taxa de inadimplência?")
#         st.sidebar.write("➡️ Quais os setores econômicos com maior volume de inadimplência?")
#         st.sidebar.write("➡️ Qual a projeção de inadimplência para os próximos 90 dias?")
#         st.sidebar.write("➡️ Qual o índice de ativo problemático por tipo de cliente?")
#         st.sidebar.write("➡️ Quais as modalidades de crédito com maior risco de inadimplência?")

#         # Botão para limpar histórico de conversa
#         if st.button("Limpar Conversa"):
#             st.session_state.chat_history_store = InMemoryChatMessageHistory()
#             st.session_state.chat_history = []
#             st.session_state.app_initialized = False
#             st.rerun()

#     conn.dispose()   

# if __name__ == "__main__":
#     main()



