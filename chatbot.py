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

# def connect_to_db():
#     try:
#         # Dados de conexão
#         host = os.getenv("SERVER")
#         database = os.getenv("DATABASE")
#         username = os.getenv("USERNAME")
#         password = os.getenv("PASSWORD")
#         port = os.getenv("PORT")

#         # Validar os valores
#         if not all([host, database, username, password, port]):
#             raise ValueError("Uma ou mais variáveis de ambiente não estão definidas no .env")

#         # Codificar a senha para lidar com caracteres especiais
#         encoded_password = quote_plus(password)

#         # String de conexão com senha codificada
#         connection_string = f"postgresql://{username}:{encoded_password}@{host}:{port}/{database}"

#         # Criar engine do SQLAlchemy
#         engine = create_engine(connection_string)

#         # Testar a conexão
#         with engine.connect() as connection:
#             print("Conexão com o banco de dados estabelecida com sucesso!")
        
#         return engine

#     except Exception as e:
#         print(f"Erro ao conectar ao banco de dados: {e}")
#         return None

def connect_to_db():
    try:
        # Verificar se está rodando no Streamlit Cloud (usando st.secrets) ou localmente (usando os.getenv)
        if "STREAMLIT_CLOUD" in os.environ:  # Variável fictícia, ajustaremos a lógica
            print("Rodando no Streamlit Cloud, usando st.secrets")
            host = st.secrets["SERVER"]
            database = st.secrets["DATABASE"]
            username = st.secrets["USERNAME"]
            password = st.secrets["PASSWORD"]
            port = st.secrets["PORT"]
        else:
            print("Rodando localmente, usando variáveis do .env")
            host = os.getenv("SERVER")
            database = os.getenv("DATABASE")
            username = os.getenv("USERNAME")
            password = os.getenv("PASSWORD")
            port = os.getenv("PORT")

        # Validar os valores
        if not all([host, database, username, password, port]):
            error_msg = "Uma ou mais variáveis de conexão com o banco não estão definidas"
            print(error_msg)
            if hasattr(st, "error"):
                st.error(error_msg)
            raise ValueError(error_msg)

        # Codificar a senha para lidar com caracteres especiais
        encoded_password = quote_plus(password)

        # String de conexão com senha codificada
        connection_string = f"postgresql+psycopg2://{username}:{encoded_password}@{host}:{port}/{database}"

        # Criar engine do SQLAlchemy
        engine = create_engine(connection_string)

        # Testar a conexão
        with engine.connect() as connection:
            success_msg = "Conexão com o banco de dados estabelecida com sucesso!"
            print(success_msg)
        
        return engine

    except Exception as e:
        error_msg = f"Erro ao conectar ao banco de dados: {e}"
        print(error_msg)
        if hasattr(st, "error"):
            st.error(error_msg)
        return None

def classify_user_intent(prompt, llm):
    """
    Classifica a intenção do usuário para determinar o tipo de consulta necessária
    """
    intent_prompt = ChatPromptTemplate.from_messages([
        ("system", """
        Analise a pergunta do usuário sobre inadimplência e classifique a intenção em uma das seguintes categorias:
        1. COMPARAÇÃO - Perguntas que comparam diferentes aspectos (ex: "Compare PF e PJ")
        2. RANKING - Perguntas sobre "maior", "menor", "top", etc. (ex: "Qual estado com maior inadimplência?")
        3. ESPECÍFICO - Perguntas sobre um atributo específico (ex: "Valor de inadimplência em São Paulo")
        4. TENDÊNCIA - Perguntas sobre evolução temporal (ex: "Como evoluiu a inadimplência")
        5. GERAL - Perguntas gerais sobre inadimplência
        
        Responda apenas com o número da categoria mais adequada (1, 2, 3, 4 ou 5).
        """),
        ("human", "{input}")
    ])
    
    intent_chain = intent_prompt | llm
    intent_result = intent_chain.invoke({"input": prompt})
    
    # Extrair apenas o número da classificação
    intent_number = ''.join(filter(str.isdigit, intent_result.content[:2]))
    
    intent_mapping = {
        "1": "COMPARAÇÃO",
        "2": "RANKING",
        "3": "ESPECÍFICO",
        "4": "TENDÊNCIA",
        "5": "GERAL"
    }
    
    return intent_mapping.get(intent_number, "GERAL")

def generate_dynamic_query(intent, prompt, llm, table_name="table_agg_inad_consolidado"):
    """
    Gera uma consulta SQL dinâmica com base na intenção do usuário e na pergunta
    """
    query_prompt = ChatPromptTemplate.from_messages([
        ("system", f"""
        Você é um especialista em SQL que transforma perguntas sobre inadimplência em consultas SQL precisas.
        
        A tabela principal é '{table_name}' e contém as seguintes colunas:
        - cliente_tipo (PF, PJ)
        - cliente_porte (Pequeno, Médio, Grande)
        - cliente_ocupacao (para PF: várias ocupações)
        - cliente_setor (para PJ: vários setores)
        - estado (siglas dos estados brasileiros)
        - modalidade (tipos de operações de crédito)
        - valor_inadimplencia (valor em reais)
        - num_operacoes (quantidade de operações)
        - data_referencia (mês de referência dos dados)
        
        A intenção do usuário foi classificada como: {intent}
        
        Com base nesta intenção e na pergunta abaixo, gere uma consulta SQL que retorne os dados necessários.
        Para consultas de RANKING, use ORDER BY e LIMIT.
        Para consultas de COMPARAÇÃO, use GROUP BY para os itens comparados.
        Para consultas ESPECÍFICAS, use filtros WHERE adequados.
        Para consultas de TENDÊNCIA, considere agrupamentos por períodos.
        
        IMPORTANTE: Retorne APENAS o código SQL, sem explicações ou comentários.
        """),
        ("human", "{input}")
    ])
    
    query_chain = query_prompt | llm
    sql_result = query_chain.invoke({"input": prompt})
    
    # Limpar a resposta para garantir que seja apenas SQL

    sql_query = sql_result.content.strip()
    if sql_query.startswith("```sql"):
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    
    return sql_query

def process_question_with_insights(prompt, intent, dynamic_query, df, insights, llm):
    """
    Processa a pergunta usando insights estáticos e dados dinâmicos da consulta
    """
    # Executar a consulta dinâmica
    try:
        dynamic_results = pd.read_sql(dynamic_query, df.con) if hasattr(df, 'con') else df.query(dynamic_query) if "SELECT" not in dynamic_query.upper() else pd.read_sql(dynamic_query, create_engine("sqlite:///:memory:"), params={})
    except Exception as e:
        print(f"Erro ao executar consulta dinâmica: {e}")
        # Fallback para insights estáticos
        dynamic_results = "Não foi possível gerar resultados dinâmicos específicos."
    
    # Preparar o contexto combinado
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
    st.title("Chatbot Inadimplinha")
    st.caption("Chatbot Inadimplinha desenvolvido por Grupo de Inadimplência EY")

    # Conectar ao banco de dados
    conn = connect_to_db()
    if conn is None:
        st.stop()
    
    # Inicializar o modelo LLM
    llm = get_llm_client()
    
    # Carregar os dados do banco e gerar insights apenas uma vez
    if "insights" not in st.session_state or "df" not in st.session_state:
        try:
            # Carregar os dados
            table = "table_agg_inad_consolidado"
            query = f"SELECT * FROM {table}"
            df = pd.read_sql(query, conn)
            st.session_state.df = df
            
            # Gerar insights
            st.session_state.insights = generate_advanced_insights(df)
            
            print(f"Total de linhas carregadas do banco: {len(df)}")
            print(f"Primeiras linhas do DataFrame:\n{df.head()}")
        except Exception as e:
            st.error(f"Erro ao carregar dados ou gerar insights: {str(e)}")
            conn.dispose()
            st.stop()
    
    # Criar a cadeia de execução padrão para casos simples
    prompt_template = ChatPromptTemplate.from_messages([
        ("system", (
            "Você é um especialista em análise de inadimplência no Brasil. "
            "Responda a pergunta do usuário com base nos dados reais de dezembro de 2024 da tabela 'table_agg_inad_consolidado', "
            "usando os insights detalhados abaixo como fonte principal. "
            "Os insights foram gerados a partir dos dados reais do banco e contêm valores totais e análises segmentadas. "
            "Extraia a resposta diretamente dos insights quando possível, sem inventar valores. "
            "Se a pergunta não for respondida pelos insights ou se os insights indicarem que não há dados, "
            "informe que os dados de dezembro de 2024 não estão disponíveis e sugira verificar a fonte. "
            "Formate os valores em reais (R$) com duas casas decimais e separadores de milhar. "
            "Inclua informações adicionais relevantes sobre inadimplência quando apropriado.\n\n"
            "Insights gerados:\n{insights}"
        )),
        ("human", "{input}")
    ])
    
    chain = prompt_template | llm

    # Inicializar o histórico de mensagens
    if "chat_history_store" not in st.session_state:
        st.session_state.chat_history_store = InMemoryChatMessageHistory()

    # Envolver a cadeia com histórico de mensagens
    conversation = RunnableWithMessageHistory(
        runnable=chain,
        get_session_history=lambda: st.session_state.chat_history_store,
        input_messages_key="input",
        history_messages_key="chat_history"
    )

    # Adicionar mensagem inicial apenas uma vez
    if not st.session_state.app_initialized and not st.session_state.chat_history:
        initial_message = "Como posso te ajudar hoje?"
        st.session_state.chat_history.append({"role": "assistant", "content": initial_message})
        st.session_state.chat_history_store.add_ai_message(initial_message)
        st.session_state.app_initialized = True

    # Exibir histórico de chat para o usuário
    for message in st.session_state.chat_history:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("Faça uma pergunta sobre a inadimplência"):
        # Adicionar a pergunta do usuário à interface de chat
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Adicionar à exibição do histórico
        st.session_state.chat_history.append({"role": "user", "content": prompt})
        
        # Processar a resposta
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            
            try:
                with st.spinner(""):
                    # Classificar a intenção do usuário
                    intent = classify_user_intent(prompt, llm)
                    print(f"Intenção classificada como: {intent}")
                    
                    # Gerar consulta dinâmica baseada na intenção
                    dynamic_query = generate_dynamic_query(intent, prompt, llm)
                    print(f"Consulta dinâmica gerada: {dynamic_query}")
                    
                    # Processar a pergunta com insights e resultados dinâmicos
                    if intent != "GERAL":
                        response_content = process_question_with_insights(
                            prompt, 
                            intent, 
                            dynamic_query, 
                            st.session_state.df, 
                            st.session_state.insights,
                            llm
                        )
                    else:
                        # Para perguntas gerais, usar o fluxo padrão
                        response = conversation.invoke(
                            {"input": prompt, "insights": st.session_state.insights},
                            config={"configurable": {"session_id": "default"}}
                        )
                        response_content = response.content
                    
                    # Simulando streaming para melhor UX
                    full_response = ""
                    for i in range(len(response_content)):
                        full_response = response_content[:i+1]
                        message_placeholder.markdown(full_response + "▌")
                        time.sleep(0.01)
                    message_placeholder.markdown(full_response)
                    
                    # Adicionar à exibição do histórico
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
        
        # Botão para limpar histórico de conversa
        if st.button("Limpar Conversa"):
            st.session_state.chat_history_store = InMemoryChatMessageHistory()
            st.session_state.chat_history = []
            st.session_state.app_initialized = False
            st.rerun()

    conn.dispose()  # Fechar o engine ao final da execução

if __name__ == "__main__":
    main()


