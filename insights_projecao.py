import pandas as pd

def generate_projection_insights(df_projecao):
    """
    Gera insights detalhados sobre projeção da inadimplência a partir de dados consolidados.
    Params:
        df_projecao: DataFrame com insights de projeção
    Returns:
        String com insights formatados
    """
    # Filtrar apenas dados de previsão
    df_projecao = df_projecao[df_projecao['tipo'].str.upper() == 'PREVISAO']
    
    if df_projecao.empty:
        return "Nenhum dado disponível para projeções de inadimplência."

    insights = "\n## PROJEÇÕES DE INADIMPLÊNCIA\n\n"

    # 1. Projeção por Ano e Porte
    insights += "### Projeção por Ano e Porte:\n"
    projecao_agrupada = df_projecao.groupby(['ano_mes', 'porte']).agg({
        'soma_ativo_problematico': 'sum',
        'soma_carteira_inadimplida_arrastada': 'sum'
    }).reset_index()

    for _, row in projecao_agrupada.iterrows():
        insights += (
            f"- **{row['ano_mes']} | {row['porte']}**: "
            f"Ativo Problemático: R$ {row['soma_ativo_problematico']:,.2f}, "
            f"Inadimplência Arrastada: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f}\n"
        )

    # 2. Projeção por Estado e Modalidade
    insights += "\n### Projeção por Estado e Modalidade:\n"
    projecao_estado_modalidade = df_projecao.groupby(['uf', 'modalidade']).agg({
        'soma_ativo_problematico': 'sum',
        'soma_carteira_inadimplida_arrastada': 'sum'
    }).reset_index()

    for _, row in projecao_estado_modalidade.sort_values('soma_carteira_inadimplida_arrastada', ascending=False).head(10).iterrows():
        insights += (
            f"- **{row['uf']} | {row['modalidade']}**: "
            f"Ativo Problemático: R$ {row['soma_ativo_problematico']:,.2f}, "
            f"Inadimplência Arrastada: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f}\n"
        )

    # 3. Projeção por Tipo de Cliente
    insights += "\n### Projeção por Tipo de Cliente:\n"
    projecao_cliente = df_projecao.groupby(['cliente']).agg({
        'soma_ativo_problematico': 'sum',
        'soma_carteira_inadimplida_arrastada': 'sum'
    }).reset_index()

    for _, row in projecao_cliente.iterrows():
        insights += (
            f"- **{row['cliente']}**: "
            f"Ativo Problemático: R$ {row['soma_ativo_problematico']:,.2f}, "
            f"Inadimplência Arrastada: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f}\n"
        )

    # 4. Destaques de Projeção
    insights += "\n### Destaques de Projeção:\n"
    total_ativo_problematico = df_projecao['soma_ativo_problematico'].sum()
    total_inadimplencia = df_projecao['soma_carteira_inadimplida_arrastada'].sum()

    insights += f"- **Total Ativo Problemático Previsto**: R$ {total_ativo_problematico:,.2f}\n"
    insights += f"- **Total Inadimplência Prevista**: R$ {total_inadimplencia:,.2f}\n"

    return insights


def projection_by_client(df_projecao, cliente, anos):
    """
    Gera projeção da dívida para um tipo específico de cliente (PF ou PJ) nos próximos anos.
    Params:
        df_projecao: DataFrame com insights de projeção
        cliente: Tipo de cliente (ex: 'PF', 'PJ')
        anos: Número de anos para projeção
    Returns:
        String com projeção formatada
    """
    df_filtered = df_projecao[df_projecao['cliente'] == cliente].copy()
    if df_filtered.empty:
        return f"Nenhum dado disponível para o tipo de cliente '{cliente}'."
    
    df_filtered['ano'] = pd.to_datetime(df_filtered['ano_mes'], format='%Y-%m').dt.year
    df_grouped = df_filtered.groupby('ano').agg({
        'soma_ativo_problematico': 'sum',
        'soma_carteira_inadimplida_arrastada': 'sum'
    }).reset_index()

    insights = f"\n## PROJEÇÃO DE DÍVIDA PARA {cliente.upper()} NOS PRÓXIMOS {anos} ANOS\n\n"
    for ano in range(df_grouped['ano'].min(), df_grouped['ano'].min() + anos):
        row = df_grouped[df_grouped['ano'] == ano]
        if not row.empty:
            insights += (
                f"- **Ano {ano}**: "
                f"Ativo Problemático: R$ {row['soma_ativo_problematico'].values[0]:,.2f}, "
                f"Inadimplência Arrastada: R$ {row['soma_carteira_inadimplida_arrastada'].values[0]:,.2f}\n"
            )
        else:
            insights += f"- **Ano {ano}**: Sem dados disponíveis.\n"

    return insights


def projection_by_state(df_projecao, uf):
    """
    Gera projeção da dívida para um estado específico (UF).
    Params:
        df_projecao: DataFrame com insights de projeção
        uf: Unidade Federativa (ex: 'SP', 'RJ')
    Returns:
        String com projeção formatada
    """
    df_filtered = df_projecao[df_projecao['uf'] == uf].copy()
    if df_filtered.empty:
        return f"Nenhum dado disponível para o estado '{uf}'."
    
    df_grouped = df_filtered.groupby('ano_mes').agg({
        'soma_ativo_problematico': 'sum',
        'soma_carteira_inadimplida_arrastada': 'sum'
    }).reset_index()

    insights = f"\n## PROJEÇÃO DE DÍVIDA PARA O ESTADO {uf.upper()}\n\n"
    for _, row in df_grouped.iterrows():
        insights += (
            f"- **{row['ano_mes']}**: "
            f"Ativo Problemático: R$ {row['soma_ativo_problematico']:,.2f}, "
            f"Inadimplência Arrastada: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f}\n"
        )

    return insights


def projection_by_port(df_projecao, porte):
    """
    Gera projeção da dívida para um porte específico de cliente.
    Params:
        df_projecao: DataFrame com insights de projeção
        porte: Porte do cliente (ex: 'Pequeno', 'Médio', 'Grande')
    Returns:
        String com projeção formatada
    """
    df_filtered = df_projecao[df_projecao['porte'] == porte].copy()
    if df_filtered.empty:
        return f"Nenhum dado disponível para o porte '{porte}'."
    
    df_grouped = df_filtered.groupby('ano_mes').agg({
        'soma_ativo_problematico': 'sum',
        'soma_carteira_inadimplida_arrastada': 'sum'
    }).reset_index()

    insights = f"\n## PROJEÇÃO DE DÍVIDA PARA CLIENTES DE PORTE {porte.upper()}\n\n"
    for _, row in df_grouped.iterrows():
        insights += (
            f"- **{row['ano_mes']}**: "
            f"Ativo Problemático: R$ {row['soma_ativo_problematico']:,.2f}, "
            f"Inadimplência Arrastada: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f}\n"
        )

    return insights