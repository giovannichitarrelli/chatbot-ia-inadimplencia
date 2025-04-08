import pandas as pd
 
def generate_projection_insights( df_projecao):
    """
    Gera insights detalhados sobre projecao da inadimplência a partir de dados consolidados de dezembro de 2024
    Params:
        df: DataFrame com insights de projecao
    Returns:
        String com insights formatados
    """
    # Filtrar apenas dados de dezembro de 2024
    df_projecao['data_base'] = pd.to_datetime(df_projecao['data_base'], format='%d/%m/%Y', errors='coerce')
    df_projecao = df_projecao[(df_projecao['data_base'].dt.month == 12) & (df_projecao['data_base'].dt.year == 2024)].copy()
    
    if df_projecao.empty:
        return "Nenhum dado disponível para dezembro de 2024."
  
    if df_projecao:
        insights += "\n## PROJEÇÕES DE INADIMPLÊNCIA\n\n"
        projecao_agrupada = df_projecao.groupby(['ano_mes', 'porte', 'tipo']).agg({
            'soma_ativo_problematico': 'sum',
            'soma_carteira_inadimplencia_arrastada': 'sum'
        }).reset_index()

        for _, row in projecao_agrupada.iterrows():
            insights += (
                f"- **{row['ano_mes']} | {row['porte']} | {row['tipo']}**: "
                f"Ativo Problemático: R$ {row['soma_ativo_problematico']:,.2f}, "
                f"Inadimplência Arrastada: R$ {row['soma_carteira_inadimplencia_arrastada']:,.2f}\n"
            )

    return insights