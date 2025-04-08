import pandas as pd
import numpy as np

def generate_advanced_insights(df):
    """
    Gera insights detalhados sobre inadimplência a partir de dados consolidados de dezembro de 2024
    
    Params:
        df: DataFrame com dados consolidados de inadimplência
    
    Returns:
        String com insights formatados
    """
    # Filtrar apenas dados de dezembro de 2024
    df['data_base'] = pd.to_datetime(df['data_base'], format='%d/%m/%Y', errors='coerce')
    df = df[(df['data_base'].dt.month == 12) & (df['data_base'].dt.year == 2024)].copy()
    
    if df.empty:
        return "Nenhum dado disponível para dezembro de 2024."

    # Preparar dados - mapear regiões
    df['regiao'] = df['uf'].map({
        'AC': 'Norte', 'AM': 'Norte', 'AP': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
        'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste', 
        'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
        'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste', 'DF': 'Centro-Oeste',
        'SP': 'Sudeste', 'RJ': 'Sudeste', 'MG': 'Sudeste', 'ES': 'Sudeste',
        'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
    })
    
    # Calcular taxa de inadimplência
    df['taxa_inadimplencia'] = (df['soma_carteira_inadimplida_arrastada'] / df['soma_carteira_ativa'] * 100).fillna(0)
    
    # Calcular índice de ativo problemático
    df['indice_ativo_problematico'] = (df['soma_ativo_problematico'] / df['soma_carteira_ativa'] * 100).fillna(0)
    
    # Calcular projeção de inadimplência em 90 dias
    df['projecao_inadimplencia_90d'] = np.where(
        df['soma_carteira_ativa'] > 0,
        df['soma_a_vencer_ate_90_dias'] * (df['soma_carteira_inadimplida_arrastada'] / df['soma_carteira_ativa']),
        0
    )
    
    # Calcular indicador de reestruturação
    df['indicador_reestruturacao'] = df['soma_ativo_problematico'] - df['soma_carteira_inadimplida_arrastada']
    
    # Determinar tipo de cliente
    df['tipo_cliente'] = df['cliente'].apply(lambda x: 'PF' if 'Física' in str(x) else 'PJ')
    
    # Preparar insights detalhados para dezembro de 2024
    insights = "# ANÁLISE ESTRATÉGICA DE INADIMPLÊNCIA BANCÁRIA - DEZEMBRO 2024\n\n"
    
    # 1. VISÃO GERAL
    insights += "## 1. VISÃO GERAL DO CENÁRIO DE INADIMPLÊNCIA (DEZ/2024)\n\n"
    
    total_inadimplencia = df['soma_carteira_inadimplida_arrastada'].sum()
    total_ativo_problematico = df['soma_ativo_problematico'].sum()
    total_carteira = df['soma_carteira_ativa'].sum()
    taxa_global = (total_inadimplencia / total_carteira * 100) if total_carteira > 0 else 0
    
    insights += f"- **Carteira Total**: R$ {total_carteira:,.2f}\n"
    insights += f"- **Total Inadimplido**: R$ {total_inadimplencia:,.2f} ({taxa_global:.2f}% da carteira total)\n"
    insights += f"- **Ativos Problemáticos**: R$ {total_ativo_problematico:,.2f}\n"
    insights += f"- **Total de Operações**: {df['soma_numero_de_operacoes'].sum():,.0f}\n"
    
    # 2. ANÁLISE REGIONAL
    insights += "\n## 2. PANORAMA REGIONAL DE INADIMPLÊNCIA (DEZ/2024)\n\n"
    
    region_summary = df.groupby('regiao').agg({
        'soma_carteira_inadimplida_arrastada': 'sum',
        'soma_carteira_ativa': 'sum',
        'soma_numero_de_operacoes': 'sum'
    }).reset_index()
    
    region_summary['percentual_inadimplencia'] = region_summary['soma_carteira_inadimplida_arrastada'] / total_inadimplencia * 100
    region_summary['taxa_inadimplencia'] = region_summary['soma_carteira_inadimplida_arrastada'] / region_summary['soma_carteira_ativa'] * 100
    
    for _, row in region_summary.sort_values('soma_carteira_inadimplida_arrastada', ascending=False).iterrows():
        insights += f"### {row['regiao']}:\n"
        insights += f"- **Inadimplência**: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f} "
        insights += f"({row['percentual_inadimplencia']:.2f}% do total inadimplido)\n"
        insights += f"- **Taxa de Inadimplência**: {row['taxa_inadimplencia']:.2f}%\n"
        insights += f"- **Número de Operações**: {row['soma_numero_de_operacoes']:,.0f}\n\n"
    
    # 3. ANÁLISE POR ESTADO
    insights += "\n## 3. ESTADOS COM MAIOR ÍNDICE DE INADIMPLÊNCIA (DEZ/2024)\n\n"
    
    state_summary = df.groupby('uf').agg({
        'soma_carteira_inadimplida_arrastada': 'sum',
        'soma_carteira_ativa': 'sum'
    }).reset_index()
    
    state_summary['percentual_total'] = state_summary['soma_carteira_inadimplida_arrastada'] / total_inadimplencia * 100
    state_summary['taxa_inadimplencia'] = state_summary['soma_carteira_inadimplida_arrastada'] / state_summary['soma_carteira_ativa'] * 100
    
    insights += "### Top 5 Estados em Volume de Inadimplência:\n"
    for _, row in state_summary.sort_values('soma_carteira_inadimplida_arrastada', ascending=False).head(5).iterrows():
        insights += f"- **{row['uf']}**: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f} "
        insights += f"({row['percentual_total']:.2f}% do total, Taxa: {row['taxa_inadimplencia']:.2f}%)\n"
    
    insights += "\n### Top 5 Estados em Taxa de Inadimplência:\n"
    for _, row in state_summary[state_summary['soma_carteira_ativa'] > 1000000].sort_values('taxa_inadimplencia', ascending=False).head(5).iterrows():
        insights += f"- **{row['uf']}**: {row['taxa_inadimplencia']:.2f}% "
        insights += f"(R$ {row['soma_carteira_inadimplida_arrastada']:,.2f})\n"
    
    # 4. ANÁLISE SETORIAL (CNAE)
    insights += "\n## 4. SETORES ECONÔMICOS E INADIMPLÊNCIA (DEZ/2024)\n\n"
    
    cnae_summary = df.groupby('cnae_secao').agg({
        'soma_carteira_inadimplida_arrastada': 'sum',
        'soma_carteira_ativa': 'sum',
        'soma_numero_de_operacoes': 'sum'
    }).reset_index()
    
    cnae_summary['percentual_total'] = cnae_summary['soma_carteira_inadimplida_arrastada'] / total_inadimplencia * 100
    cnae_summary['taxa_inadimplencia'] = cnae_summary['soma_carteira_inadimplida_arrastada'] / cnae_summary['soma_carteira_ativa'] * 100
    
    insights += "### Setores com Maior Volume de Inadimplência:\n"
    for _, row in cnae_summary.sort_values('soma_carteira_inadimplida_arrastada', ascending=False).head(5).iterrows():
        insights += f"- **{row['cnae_secao']}**: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f} "
        insights += f"({row['percentual_total']:.2f}% do total, Taxa: {row['taxa_inadimplencia']:.2f}%)\n"
    
    insights += "\n### Setores com Maior Taxa de Inadimplência:\n"
    for _, row in cnae_summary[cnae_summary['soma_carteira_ativa'] > 1000000].sort_values('taxa_inadimplencia', ascending=False).head(5).iterrows():
        insights += f"- **{row['cnae_secao']}**: {row['taxa_inadimplencia']:.2f}% "
        insights += f"(R$ {row['soma_carteira_inadimplida_arrastada']:,.2f})\n"
    
    # 5. COMPARATIVO PESSOA FÍSICA VS PESSOA JURÍDICA (DEZ/2024)
    insights += "\n## 5. COMPARATIVO PESSOA FÍSICA VS PESSOA JURÍDICA (DEZ/2024)\n\n"
    
    client_type_summary = df.groupby('tipo_cliente').agg({
        'soma_carteira_inadimplida_arrastada': 'sum',
        'soma_carteira_ativa': 'sum',
        'soma_numero_de_operacoes': 'sum',
        'soma_ativo_problematico': 'sum',
        'soma_a_vencer_ate_90_dias': 'sum',
        'projecao_inadimplencia_90d': 'sum'
    }).reset_index()
    
    client_type_summary['taxa_inadimplencia'] = (client_type_summary['soma_carteira_inadimplida_arrastada'] / client_type_summary['soma_carteira_ativa'] * 100).fillna(0)
    client_type_summary['media_por_operacao'] = (client_type_summary['soma_carteira_inadimplida_arrastada'] / client_type_summary['soma_numero_de_operacoes']).fillna(0)
    client_type_summary['percentual_inadimplencia'] = (client_type_summary['soma_carteira_inadimplida_arrastada'] / total_inadimplencia * 100).fillna(0)
    client_type_summary['risco_90d_percentual'] = (client_type_summary['projecao_inadimplencia_90d'] / client_type_summary['soma_a_vencer_ate_90_dias'] * 100).fillna(0)
    
    insights += "### Visão Geral PF vs PJ:\n"
    for _, row in client_type_summary.iterrows():
        insights += f"#### {row['tipo_cliente']}:\n"
        insights += f"- **Inadimplência Total**: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f} ({row['percentual_inadimplencia']:.2f}% do total)\n"
        insights += f"- **Taxa de Inadimplência**: {row['taxa_inadimplencia']:.2f}%\n"
        insights += f"- **Ativos Problemáticos**: R$ {row['soma_ativo_problematico']:,.2f}\n"
        insights += f"- **Número de Operações**: {row['soma_numero_de_operacoes']:,.0f}\n"
        insights += f"- **Média por Operação**: R$ {row['media_por_operacao']:,.2f}\n"
        insights += f"- **Projeção Inadimplência 90 Dias**: R$ {row['projecao_inadimplencia_90d']:,.2f} (Risco: {row['risco_90d_percentual']:.2f}%)\n\n"
    
    # 5.1 Distribuição por Porte
    insights += "### Distribuição por Porte:\n"
    size_summary = df.groupby(['tipo_cliente', 'porte']).agg({
        'soma_carteira_inadimplida_arrastada': 'sum',
        'soma_carteira_ativa': 'sum',
        'soma_ativo_problematico': 'sum',
        'soma_numero_de_operacoes': 'sum'
    }).reset_index()
    
    size_summary['taxa_inadimplencia'] = (size_summary['soma_carteira_inadimplida_arrastada'] / size_summary['soma_carteira_ativa'] * 100).fillna(0)
    size_summary['indice_problematico'] = (size_summary['soma_ativo_problematico'] / size_summary['soma_carteira_ativa'] * 100).fillna(0)
    
    for tipo in ['PF', 'PJ']:
        insights += f"#### {tipo}:\n"
        for _, row in size_summary[size_summary['tipo_cliente'] == tipo].sort_values('soma_carteira_inadimplida_arrastada', ascending=False).iterrows():
            insights += f"- **{row['porte']}**: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f} "
            insights += f"(Taxa: {row['taxa_inadimplencia']:.2f}%, Índice Problemático: {row['indice_problematico']:.2f}%)\n"
        insights += "\n"
    
    # 5.2 Modalidades de Crédito por Tipo de Cliente
    insights += "### Modalidades de Crédito com Maior Inadimplência:\n"
    modality_summary_client = df.groupby(['tipo_cliente', 'modalidade']).agg({
        'soma_carteira_inadimplida_arrastada': 'sum',
        'soma_carteira_ativa': 'sum',
        'soma_numero_de_operacoes': 'sum'
    }).reset_index()
    
    modality_summary_client['taxa_inadimplencia'] = (modality_summary_client['soma_carteira_inadimplida_arrastada'] / modality_summary_client['soma_carteira_ativa'] * 100).fillna(0)
    modality_summary_client['percentual_inadimplencia'] = (modality_summary_client['soma_carteira_inadimplida_arrastada'] / total_inadimplencia * 100).fillna(0)
    
    for tipo in ['PF', 'PJ']:
        insights += f"#### {tipo}:\n"
        insights += f"- **Top Modalidades por Volume de Inadimplência**:\n"
        for _, row in modality_summary_client[modality_summary_client['tipo_cliente'] == tipo].sort_values('soma_carteira_inadimplida_arrastada', ascending=False).head(3).iterrows():
            insights += f"  - **{row['modalidade']}**: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f} "
            insights += f"({row['percentual_inadimplencia']:.2f}% do total, Taxa: {row['taxa_inadimplencia']:.2f}%)\n"
        insights += f"- **Top Modalidades por Taxa de Inadimplência**:\n"
        for _, row in modality_summary_client[(modality_summary_client['tipo_cliente'] == tipo) & (modality_summary_client['soma_carteira_ativa'] > 1000000)].sort_values('taxa_inadimplencia', ascending=False).head(3).iterrows():
            insights += f"  - **{row['modalidade']}**: {row['taxa_inadimplencia']:.2f}% "
            insights += f"(R$ {row['soma_carteira_inadimplida_arrastada']:,.2f})\n"
        insights += "\n"
    
    # 6. ANÁLISE POR MODALIDADE GERAL
    insights += "\n## 6. MODALIDADES DE CRÉDITO E INADIMPLÊNCIA (DEZ/2024)\n\n"
    
    modality_summary = df.groupby('modalidade').agg({
        'soma_carteira_inadimplida_arrastada': 'sum',
        'soma_carteira_ativa': 'sum',
        'soma_numero_de_operacoes': 'sum'
    }).reset_index()
    
    modality_summary['taxa_inadimplencia'] = modality_summary['soma_carteira_inadimplida_arrastada'] / modality_summary['soma_carteira_ativa'] * 100
    modality_summary['percentual_total'] = modality_summary['soma_carteira_inadimplida_arrastada'] / total_inadimplencia * 100
    
    insights += "### Top Modalidades por Volume de Inadimplência:\n"
    for _, row in modality_summary.sort_values('soma_carteira_inadimplida_arrastada', ascending=False).head(6).iterrows():
        insights += f"- **{row['modalidade']}**: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f} "
        insights += f"({row['percentual_total']:.2f}% do total, Taxa: {row['taxa_inadimplencia']:.2f}%)\n"
    
    insights += "\n### Top Modalidades por Taxa de Inadimplência:\n"
    for _, row in modality_summary[modality_summary['soma_carteira_ativa'] > 1000000].sort_values('taxa_inadimplencia', ascending=False).head(5).iterrows():
        insights += f"- **{row['modalidade']}**: {row['taxa_inadimplencia']:.2f}% "
        insights += f"(R$ {row['soma_carteira_inadimplida_arrastada']:,.2f})\n"
    
    # 7. ANÁLISE POR OCUPAÇÃO (PF)
    insights += "\n## 7. INADIMPLÊNCIA POR OCUPAÇÃO - PESSOA FÍSICA (DEZ/2024)\n\n"
    
    occupation_summary = df[df['tipo_cliente'] == 'PF'].groupby('ocupacao').agg({
        'soma_carteira_inadimplida_arrastada': 'sum',
        'soma_carteira_ativa': 'sum',
        'soma_numero_de_operacoes': 'sum'
    }).reset_index()
    
    occupation_summary['taxa_inadimplencia'] = occupation_summary['soma_carteira_inadimplida_arrastada'] / occupation_summary['soma_carteira_ativa'] * 100
    occupation_summary['media_por_operacao'] = occupation_summary['soma_carteira_inadimplida_arrastada'] / occupation_summary['soma_numero_de_operacoes']
    
    insights += "### Ocupações com Maior Volume de Inadimplência:\n"
    for _, row in occupation_summary.sort_values('soma_carteira_inadimplida_arrastada', ascending=False).head(5).iterrows():
        insights += f"- **{row['ocupacao']}**: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f} "
        insights += f"(Taxa: {row['taxa_inadimplencia']:.2f}%, Média: R$ {row['media_por_operacao']:,.2f})\n"
    
    insights += "\n### Ocupações com Maior Taxa de Inadimplência:\n"
    valid_occupations = occupation_summary[occupation_summary['soma_carteira_ativa'] > 500000]
    for _, row in valid_occupations.sort_values('taxa_inadimplencia', ascending=False).head(5).iterrows():
        insights += f"- **{row['ocupacao']}**: {row['taxa_inadimplencia']:.2f}% "
        insights += f"(Volume: R$ {row['soma_carteira_inadimplida_arrastada']:,.2f})\n"
    
    # 8. PROJEÇÕES E RISCO FUTURO
    insights += "\n## 8. PROJEÇÃO DE INADIMPLÊNCIA EM 90 DIAS (DEZ/2024)\n\n"
    
    projection_summary = df.groupby(['tipo_cliente', 'porte']).agg({
        'projecao_inadimplencia_90d': 'sum',
        'soma_a_vencer_ate_90_dias': 'sum',
        'soma_carteira_inadimplida_arrastada': 'sum'
    }).reset_index()
    
    projection_summary['risco_percentual'] = projection_summary['projecao_inadimplencia_90d'] / projection_summary['soma_a_vencer_ate_90_dias'] * 100
    projection_summary['aumento_previsto'] = projection_summary['projecao_inadimplencia_90d'] / projection_summary['soma_carteira_inadimplida_arrastada'] * 100
    
    insights += "### Projeção por Tipo e Porte de Cliente:\n"
    for _, row in projection_summary.sort_values('projecao_inadimplencia_90d', ascending=False).head(8).iterrows():
        insights += f"- **{row['tipo_cliente']} - {row['porte']}**: R$ {row['projecao_inadimplencia_90d']:,.2f} "
        insights += f"(Risco: {row['risco_percentual']:.2f}%, Aumento Previsto: {row['aumento_previsto']:.2f}%)\n"
    
    # 9. REESTRUTURAÇÃO DE DÍVIDAS
    insights += "\n## 9. ANÁLISE DE REESTRUTURAÇÃO DE DÍVIDAS (DEZ/2024)\n\n"
    
    restructuring_summary = df.groupby(['tipo_cliente', 'porte']).agg({
        'indicador_reestruturacao': 'sum',
        'soma_ativo_problematico': 'sum',
        'soma_carteira_inadimplida_arrastada': 'sum'
    }).reset_index()
    
    restructuring_summary['percentual_reestruturacao'] = restructuring_summary['indicador_reestruturacao'] / restructuring_summary['soma_ativo_problematico'] * 100
    
    insights += "### Indicadores de Reestruturação por Segmento:\n"
    for _, row in restructuring_summary.sort_values('indicador_reestruturacao', ascending=False).head(6).iterrows():
        if row['soma_ativo_problematico'] > 0:
            insights += f"- **{row['tipo_cliente']} - {row['porte']}**: R$ {row['indicador_reestruturacao']:,.2f} "
            insights += f"({row['percentual_reestruturacao']:.2f}% dos ativos problemáticos)\n"
    
    # 10. RECOMENDAÇÕES ESTRATÉGICAS
    insights += "\n## 10. RECOMENDAÇÕES ESTRATÉGICAS (DEZ/2024)\n\n"
    
    insights += "### Ações Recomendadas por Segmento de Risco:\n"
    
    top_cnae_risk = cnae_summary.sort_values('taxa_inadimplencia', ascending=False).head(3)
    insights += "#### Setores Econômicos de Alto Risco:\n"
    for _, row in top_cnae_risk.iterrows():
        insights += f"- **{row['cnae_secao']}**: Implementar monitoramento especial e revisar políticas de crédito\n"
    
    top_region_risk = region_summary.sort_values('taxa_inadimplencia', ascending=False).head(2)
    insights += "\n#### Regiões Críticas:\n"
    for _, row in top_region_risk.iterrows():
        insights += f"- **{row['regiao']}**: Considerar condições macroeconômicas regionais e ajustar estratégias de cobrança\n"
    
    top_modality_risk = modality_summary.sort_values('taxa_inadimplencia', ascending=False).head(3)
    insights += "\n#### Modalidades de Alto Risco:\n"
    for _, row in top_modality_risk.iterrows():
        insights += f"- **{row['modalidade']}**: Revisar critérios de aprovação e limites de crédito\n"
    
    # Conclusão
    insights += "\n## CONCLUSÃO EXECUTIVA (DEZ/2024)\n\n"
    insights += f"- A taxa global de inadimplência em dezembro de 2024 está em **{taxa_global:.2f}%** da carteira total\n"
    insights += "- Aproximadamente **{:.2f}%** do volume inadimplido está concentrado na região {}\n".format(
        region_summary.iloc[0]['percentual_inadimplencia'], 
        region_summary.iloc[0]['regiao']
    )
    insights += "- O setor **{}** apresenta a maior concentração de inadimplência ({:.2f}%)\n".format(
        cnae_summary.iloc[0]['cnae_secao'],
        cnae_summary.iloc[0]['percentual_total']
    )
    insights += "- A modalidade **{}** apresenta a maior taxa de inadimplência ({:.2f}%)\n".format(
        modality_summary.sort_values('taxa_inadimplencia', ascending=False).iloc[0]['modalidade'],
        modality_summary.sort_values('taxa_inadimplencia', ascending=False).iloc[0]['taxa_inadimplencia']
    )
    insights += "- Projeção de inadimplência para os próximos 90 dias indica potencial aumento de até **{:.2f}%**\n".format(
        projection_summary['aumento_previsto'].mean()
    )
    
    insights += "\n### Próximos Passos Recomendados:\n"
    insights += "1. Revisar políticas de crédito para os setores e modalidades de maior risco\n"
    insights += "2. Monitorar de perto as regiões com altas taxas de inadimplência\n"
    insights += "3. Avaliar estratégias de reestruturação para os segmentos com ativos problemáticos elevados\n"
    insights += "4. Implementar alertas precoces baseados nas projeções de 90 dias\n"
    
    return insights