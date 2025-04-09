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

# def generate_dynamic_query(intent, prompt, llm):
#     if intent == "PROJEÇÃO":
#         query_prompt = ChatPromptTemplate.from_messages([
#             ("system", f"""
#             Você é um especialista em SQL que transforma perguntas sobre inadimplência em consultas SQL precisas para um banco PostgreSQL.

#             A tabela principal se chama 'projecao_consolidado' e contém as seguintes colunas:
#             - ano_mes (data da projeção, formato 'DD/MM/YYYY', tipo texto)
#             - porte (porte do cliente: Pequeno, Médio, Grande)
#             - uf (unidade federativa, siglas dos estados brasileiros)
#             - cliente (tipo de cliente: PF ou PJ)
#             - modalidade (modalidade da operação de crédito)
#             - tipo (tipo de cliente: PF ou PJ)
#             - soma_ativo_problematico (soma dos ativos problemáticos)
#             - soma_carteira_inadimplida_arrastada (soma da carteira inadimplida arrastada)

#             Para perguntas envolvendo regiões, use este mapeamento de UFs para regiões no SQL com CASE WHEN:
#             - Norte: AC, AM, AP, PA, RO, RR, TO
#             - Nordeste: AL, BA, CE, MA, PB, PE, PI, RN, SE
#             - Centro-Oeste: GO, MT, MS, DF
#             - Sudeste: SP, RJ, MG, ES
#             - Sul: PR, RS, SC

#             Com base na pergunta abaixo, gere uma consulta SQL válida que retorne os dados necessários:
#             - Use TO_DATE(ano_mes, 'DD/MM/YYYY') para converter ano_mes em data.
#             - Use NOW() para a data atual e NOW() + INTERVAL 'X days' para projeções futuras (ex.: '90 days').
#             - Filtre ano_mes para o período solicitado (ex.: próximos 90 dias a partir de hoje).
#             - Agregue valores (ex.: SUM) quando necessário para totais.
#             - Se a pergunta mencionar "região" ou "regiões", agrupe por região usando o mapeamento acima.
#             - Certifique-se de que a consulta seja sintaticamente correta e compatível com PostgreSQL.

#             IMPORTANTE: Retorne APENAS o código SQL, sem explicações ou comentários.
#             """),
#             ("human", "{input}")
#         ])   
#     else:
#         query_prompt = ChatPromptTemplate.from_messages([
#             ("system", f"""
#             Você é um especialista em SQL que transforma perguntas sobre inadimplência em consultas SQL precisas para um banco PostgreSQL.

#             A tabela principal se chama 'table_agg_inad_consolidado' e contém as seguintes colunas:
#             - data_base (data de referência dos dados, formato 'YYYY-MM-DD')
#             - uf (unidade federativa, siglas dos estados brasileiros)
#             - cliente (tipo de cliente: PF ou PJ)
#             - ocupacao (ocupações para PF)
#             - cnae_secao (setores de atuação para PJ)
#             - porte (porte do cliente: Pequeno, Médio, Grande)
#             - modalidade (modalidade da operação de crédito)
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

#             Para perguntas envolvendo regiões, use este mapeamento de UFs para regiões no SQL com CASE WHEN:
#             - Norte: AC, AM, AP, PA, RO, RR, TO
#             - Nordeste: AL, BA, CE, MA, PB, PE, PI, RN, SE
#             - Centro-Oeste: GO, MT, MS, DF
#             - Sudeste: SP, RJ, MG, ES
#             - Sul: PR, RS, SC

#             A intenção do usuário foi classificada como: {intent}

#             Com base nesta intenção e na pergunta abaixo, gere uma consulta SQL válida que retorne os dados necessários:
#             - Para RANKING, use ORDER BY e LIMIT para identificar o maior/menor.
#             - Para COMPARAÇÃO, use GROUP BY para os itens comparados.
#             - Para ESPECÍFICO, use filtros WHERE adequados.
#             - Para TENDÊNCIA, agrupe por data_base e ordene cronologicamente.
#             - Sempre inclua filtros ou agregações (ex.: SUM) para garantir resultados totais e precisos.
#             - Use o formato de data 'YYYY-MM-DD' (ex.: '2024-12-31') para o campo data_base.
#             - Se a pergunta não especificar um período, use apenas dados de '2024-12-31'.
#             - Se a pergunta mencionar "região" ou "regiões", agrupe por região usando o mapeamento acima.
#             - Certifique-se de que a consulta seja sintaticamente correta e compatível com PostgreSQL.

#             IMPORTANTE: Retorne APENAS o código SQL, sem explicações ou comentários.
#             """),
#             ("human", "{input}")
#         ])
    
#     query_chain = query_prompt | llm
#     sql_result = query_chain.invoke({"input": prompt})
    
#     sql_query = sql_result.content.strip()
#     if sql_query.startswith("```sql"):
#         sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    
#     print(f"Consulta SQL gerada: {sql_query}")  # Log para depuração
#     return sql_query

def generate_dynamic_query(intent, prompt, llm, table_name="table_agg_inad_consolidado"):
    if intent == "PROJEÇÃO":
        query_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""
            Você é um especialista em SQL que transforma perguntas sobre inadimplência em consultas SQL precisas para um banco PostgreSQL.

            A tabela principal se chama 'projecao_consolidado' e contém as seguintes colunas:
            - ano_mes (data da projeção, formato 'DD/MM/YYYY', tipo texto)
            - porte (porte do cliente: Pequeno, Médio, Grande)
            - uf (unidade federativa, siglas dos estados brasileiros)
            - cliente (tipo de cliente: PF ou PJ)
            - modalidade (modalidade da operação de crédito)
            - tipo (tipo de cliente: PF ou PJ, ou 'previsão' para projeções)
            - soma_ativo_problematico (soma dos ativos problemáticos)
            - soma_carteira_inadimplida_arrastada (soma da carteira inadimplida arrastada)

            Para perguntas envolvendo regiões, use este mapeamento de UFs para regiões no SQL com CASE WHEN:
            - Norte: AC, AM, AP, PA, RO, RR, TO
            - Nordeste: AL, BA, CE, MA, PB, PE, PI, RN, SE
            - Centro-Oeste: GO, MT, MS, DF
            - Sudeste: SP, RJ, MG, ES
            - Sul: PR, RS, SC

            Com base na pergunta abaixo, gere uma consulta SQL válida que retorne os dados necessários:
            - Use TO_DATE(ano_mes, 'DD/MM/YYYY') para converter ano_mes em data.
            - Use NOW() para a data atual e NOW() + INTERVAL 'X days' para projeções futuras (ex.: '90 days').
            - Se a pergunta mencionar "percentual" ou "%", calcule a porcentagem dividindo o valor específico (ex.: soma_carteira_inadimplida_arrastada para um filtro específico) pelo total geral (ex.: soma_carteira_inadimplida_arrastada sem filtros adicionais além de ano_mes) e multiplique por 100, retornando o resultado como uma coluna chamada "percentual".
            - Filtre ano_mes para o período solicitado (ex.: próximos 90 dias a partir de hoje).
            - Filtre apenas registros onde tipo = 'previsão'.
            - Agregue valores (ex.: SUM) quando necessário para totais.
            - Se a pergunta mencionar "percentual" ou "%", calcule a porcentagem dividindo o valor específico pelo total e multiplicando por 100.
            - Se a pergunta mencionar "região" ou "regiões", agrupe por região usando o mapeamento acima.
            - Certifique-se de que a consulta seja sintaticamente correta e compatível com PostgreSQL.

            IMPORTANTE: Retorne APENAS o código SQL, sem explicações ou comentários.
            """),
            ("human", "{input}")
        ])   
    else:
        query_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""
            Você é um especialista em SQL que transforma perguntas sobre inadimplência em consultas SQL precisas para um banco PostgreSQL.

            A tabela principal se chama '{table_name}' e contém as seguintes colunas:
            - data_base (data de referência dos dados, formato 'YYYY-MM-DD')
            - uf (unidade federativa, siglas dos estados brasileiros)
            - cliente (tipo de cliente: PF ou PJ)
            - ocupacao (ocupações para PF)
            - cnae_secao (setores de atuação para PJ)
            - porte (porte do cliente: Pequeno, Médio, Grande)
            - modalidade (modalidade da operação de crédito)
            - soma_a_vencer_ate_90_dias (soma dos valores a vencer em até 90 dias)
            - soma_numero_de_operacoes (soma do número de operações)
            - soma_carteira_ativa (soma da carteira ativa total)
            - soma_carteira_inadimplida_arrastada (soma da carteira inadimplida arrastada, considerada a dívida)
            - soma_ativo_problematico (soma dos ativos problemáticos)
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

            Para perguntas envolvendo regiões, use este mapeamento de UFs para regiões no SQL com CASE WHEN:
            - Norte: AC, AM, AP, PA, RO, RR, TO
            - Nordeste: AL, BA, CE, MA, PB, PE, PI, RN, SE
            - Centro-Oeste: GO, MT, MS, DF
            - Sudeste: SP, RJ, MG, ES
            - Sul: PR, RS, SC

            A intenção do usuário foi classificada como: {intent}

            Com base nesta intenção e na pergunta abaixo, gere uma consulta SQL válida que retorne os dados necessários:
            - Para RANKING, use ORDER BY e LIMIT para identificar o maior/menor.
            - Para COMPARAÇÃO, use GROUP BY para os itens comparados.
            - Para ESPECÍFICO, use filtros WHERE adequados (ex.: uf='SP', cliente='PJ').
            - Para TENDÊNCIA, agrupe por data_base e ordene cronologicamente.
            - Sempre inclua filtros ou agregações (ex.: SUM) para garantir resultados totais e precisos.
            - Use o formato de data 'YYYY-MM-DD' (ex.: '2021-10-31') para o campo data_base.
            - Se a pergunta fornecer uma data no formato 'MM/YYYY' (ex.: '10/2021'), converta para 'YYYY-MM-DD' assumindo o último dia do mês (ex.: '2021-10-31').
            - Se a pergunta não especificar um período, use apenas dados de '2024-12-31'.
            - Se a pergunta mencionar "percentual" ou "%", calcule a porcentagem dividindo o valor específico (ex.: soma_carteira_inadimplida_arrastada para um filtro específico) pelo total geral (ex.: soma_carteira_inadimplida_arrastada sem filtros adicionais além de data_base) e multiplique por 100, retornando o resultado como uma coluna chamada "percentual".
            - Se a pergunta mencionar "região" ou "regiões", agrupe por região usando o mapeamento acima.
            - Certifique-se de que a consulta seja sintaticamente correta e compatível com PostgreSQL.

            IMPORTANTE: Retorne APENAS o código SQL, sem explicações ou comentários.
            """),
            ("human", "{input}")
        ])
    
    query_chain = query_prompt | llm
    sql_result = query_chain.invoke({"input": prompt})
    
    sql_query = sql_result.content.strip()
    if sql_query.startswith("```sql"):
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    
    print(f"Consulta SQL gerada: {sql_query}")  # Log para depuração
    return sql_query

def process_question(prompt, intent, dynamic_query, llm, conn):
    try:
        dynamic_results = pd.read_sql(dynamic_query, conn)
        print(f"Resultados dinâmicos: {dynamic_results.to_string()}")  # Log para depuração
    except Exception as e:
        print(f"Erro ao executar consulta dinâmica: {e}")
        dynamic_results = "Não foi possível gerar resultados dinâmicos específicos."

    processing_prompt = ChatPromptTemplate.from_messages([
        ("system", f"""
        Você é um especialista em análise de inadimplência no Brasil.
        
        A pergunta do usuário foi classificada como: {intent}
        
        Responda à pergunta usando os resultados da consulta abaixo, que refletem os dados completos das tabelas:
        
        RESULTADOS DA CONSULTA:
        {dynamic_results}
        
        Formate os valores em reais (R$) com duas casas decimais e separadores de milhar.
        Seja conciso e direto, destacando os pontos mais relevantes para a pergunta do usuário.
        Se os dados não forem suficientes ou estiverem ausentes, informe que os dados não estão disponíveis e sugira verificar a fonte.
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

    # Sem pré-carregamento de insights
    prompt_template = ChatPromptTemplate.from_messages([
        ("system", """
        Você é um especialista em análise de inadimplência no Brasil.
        Responda à pergunta do usuário com base nos dados reais das tabelas 'table_agg_inad_consolidado' e 'projecao_consolidado'.
        Se os dados não estiverem disponíveis, informe que não há informações suficientes e sugira verificar a fonte.
        Formate os valores em reais (R$) com duas casas decimais e separadores de milhar.
        """),
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
                    
                    if intent != "GERAL":
                        response_content = process_question(prompt, intent, dynamic_query, llm, conn)
                    else:
                        response = conversation.invoke(
                            {"input": prompt},
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
        st.sidebar.write("➡️ Qual a projeção de inadimplência para os próximos 90 dias?")

        if st.button("Limpar Conversa"):
            st.session_state.chat_history_store = InMemoryChatMessageHistory()
            st.session_state.chat_history = []
            st.session_state.app_initialized = False
            st.rerun()

    conn.dispose()

if __name__ == "__main__":
    main()
 