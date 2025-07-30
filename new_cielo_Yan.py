"""
Script de Conciliação de Dados Cielo x ERP
Autor: Yan Fernandes
Descrição: codigo de concliação de dados entre Cielo e ERP, com formatação e exportação para Excel.
"""
#========================================
# Importação das bibliotecas necessárias:
#========================================

import pandas as pd
import streamlit as st
import os
import sys
import logging
import numpy as np
from pandas import ExcelWriter
from rapidfuzz import process, fuzz
from openpyxl import load_workbook
from datetime import datetime
import io

#================================
# Configuração do sistema de logs
#================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('conciliacao_erros.log'),
        logging.StreamHandler()
    ]
)

def resource_path(relative_path):
    
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

#===========================================
# === FUNÇÕES: CARREGAMENTO DE PLANILHAS ===
#===========================================

def carregar_planilha(caminho):
    try:
        logging.info(f"Tentando carregar planilha: {getattr(caminho, 'name', caminho)}")
        if caminho.name.endswith(".csv"):
            df = pd.read_csv(
                caminho, 
                sep=";", 
                encoding="latin1", 
                dtype={
                    "NSU": str,
                    "NSU Concentrador": str
                }
            )
            logging.info(f"Planilha CSV carregada com sucesso: {df.shape}")
            return df
        else:
            df = pd.read_excel(caminho, dtype={"NSU/DOC": str})
            logging.info(f"Planilha Excel carregada com sucesso: {df.shape}")
            return df
    except Exception as e:
        logging.error(f"Erro ao carregar arquivo {getattr(caminho, 'name', caminho)}: {str(e)}", exc_info=True)
        raise


#=============================================
# =========== FORMATAÇÂO CIELO ===============
#=============================================

def limpar_cielo(df):
    try:
        logging.debug("Iniciando limpeza da planilha Cielo")
        # Remove cabeçalhos e linhas inválidas
        df = df.iloc[8:].reset_index(drop=True)
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)
        df.dropna(axis='columns', how='all', inplace=True)
        
        # Normalizar nomes das colunas (minúsculas e sem espaços)
        df.columns = df.columns.str.strip().str.lower()

        # Selecionar e garantir colunas necessárias
        colunas_cielo = [
            "data de pagamento",
            "data do lançamento",
            "estabelecimento",
            "tipo de lançamento",
            "bandeira",
            "valor bruto",
            "taxa/tarifa",
            "valor líquido",
            "data da venda",
            "data prevista de pagamento",
            "código da autorização",
            "nsu/doc",
            "número da parcela",
            "quantidade total de parcelas",
            "valor total da transação",
        ]

        # Verificar colunas faltantes
        colunas_faltantes = set([col.lower() for col in colunas_cielo]) - set(df.columns)
        if colunas_faltantes:
            raise ValueError(f"Colunas faltantes: {colunas_faltantes}")
        df = df[[col.lower() for col in colunas_cielo]]

        # Converter valores numéricos (tratamento robusto)
        # Converter valores numéricos (mantendo padrão em reais)
        for col in ["valor bruto", "taxa/tarifa", "valor líquido", "valor total da transação"]:
            df[col] = (
                df[col].astype(str)
                    .str.replace(',', '.', regex=False)      # Converte vírgula decimal
                    .astype(float)
            )


        # Converter datas com tratamento de erros
        for col in ["data de pagamento", "data do lançamento", "data da venda", "data prevista de pagamento"]:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
            nat_count = df[col].isna().sum()
            if nat_count > 0:
                logging.warning(f"{nat_count} datas inválidas na coluna {col}")

        # Colunas para inteiro
        for col in ["número da parcela", "quantidade total de parcelas"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        # Renomear colunas
        df = df.rename(columns={
            "valor bruto": "VALOR DA PARCELA",
            "valor líquido": "VALOR LÍQUIDO",
            "número da parcela": "PARCELA",
            "quantidade total de parcelas": "TOTAL_PARCELAS",
            "código da autorização": "AUTORIZAÇÃO",
            "nsu/doc": "NSU/DOC",
            "data da venda": "DATA DA VENDA",
            "data prevista de pagamento": "DATA DE VENCIMENTO",
            "bandeira": "BANDEIRA / MODALIDADE",
            "tipo de lançamento": "Tipo de lançamento"
        })


        logging.info("Limpeza da planilha Cielo concluída com sucesso")
        return df
    except Exception as e:
        logging.error(f"Erro crítico em limpar_cielo: {str(e)}", exc_info=True)
        raise
    
    


#=============================================
# =========== FORMATAÇÂO ERP =================
#=============================================


def limpar_erp(df):
    try:
        logging.debug("Iniciando limpeza da planilha ERP")
        # Remover colunas irrelevantes
        cols_to_drop = ["Nome do Cliente", "Tipo", "Carteira", "Caracterização da Venda", "1o. Agrupamento"]
        for col in cols_to_drop:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)
        # Converter coluna 'Emissão' para data
        df["Emissão"] = pd.to_datetime(df["Emissão"], dayfirst=True, errors="coerce")
        # Extrair 'Numero da Parcela' e 'Total Parcelas' da coluna 'Numero' (formato 'id-X/Y')
        parcela_info = df["Numero"].str.extract(r'-(\d+)/(\d+)')
        df["Numero da Parcela"] = parcela_info[0].astype(int)
        df["Total Parcelas"]    = parcela_info[1].astype(int)
        # Formatação numérica de valores
        # Formatação numérica de valores (mantendo padrão em reais)
        for col in ["Valor", "Vr Corrigido", "Taxa"]:
            df[col] = (
            df[col].astype(str)
                .str.replace(',', '.', regex=False)     # Converte vírgula decimal
                .astype(float)
        )

        logging.info("Limpeza da planilha ERP concluída com sucesso")
        return df
    except Exception as e:
        logging.error(f"Erro ao preparar dados ERP: {str(e)}", exc_info=True)
        raise






#=============================================
# === FUNÇÃO PRINCIPAL: CONCILIADOR ==========
#=============================================


# === FUNÇÕES: CONCILIAÇÃO POR PONTUAÇÃO ===

def selecionar_melhor_por_pontuacao(row, df_erp_base, tolerancia_dias=3, tolerancia_valor=0.20, incluir_detalhes=False):
    try:
        logging.debug(f"[MATCH] Iniciando para linha: {row.to_dict()}")

        # --- NOVO: checar NSU e Autorização 100% iguais ---
        nsu_cielo = str(row["NSU/DOC"])
        aut_cielo = str(row["AUTORIZAÇÃO"])
        match = df_erp_base[
            (df_erp_base["NSU"].astype(str) == nsu_cielo) &
            (df_erp_base["Autorização"].astype(str) == aut_cielo)
        ]
        if not match.empty:
            linha = match.iloc[0]
            return pd.Series([linha["Autorização"], linha["NSU"], linha["Chave"], linha["Valor"], "Conciliado", 0])

        # --- Lógica existente ---
        mask = (
            (abs((df_erp_base["Emissão"] - row["DATA DA VENDA"]).dt.days) <= tolerancia_dias) &
            (abs(df_erp_base["Valor"] - row["VALOR DA PARCELA"]) <= tolerancia_valor) &
            (df_erp_base["Numero da Parcela"] == row["PARCELA"]) &
            (df_erp_base["Total Parcelas"] == row["TOTAL_PARCELAS"])
        )
        candidatos = df_erp_base[mask]

        melhor_resultado = None
        menor_pontuacao = float("inf")

        for _, linha in candidatos.iterrows():
            dias_dif = abs((linha["Emissão"] - row["DATA DA VENDA"]).days)
            valor_dif = abs(linha["Valor"] - row["VALOR DA PARCELA"])
            sim_autorizacao = fuzz.ratio(aut_cielo, linha["Autorização"])
            sim_nsu = fuzz.ratio(nsu_cielo, linha["NSU"])
            pontuacao = dias_dif * 10 + valor_dif * 100 + (100 - sim_nsu) + (100 - sim_autorizacao)
            status = []
            if dias_dif > tolerancia_dias:
                status.append("Divergência de Data")
            if valor_dif > tolerancia_valor:
                status.append("Divergência de Valor")
            if row["PARCELA"] != linha["Numero da Parcela"]:
                status.append("Divergência de Parcela")
            if row["TOTAL_PARCELAS"] != linha["Total Parcelas"]:
                status.append("Divergência de Total de Parcelas")
            status_final = " e ".join(status) if status else "Conciliado"
            if pontuacao < menor_pontuacao:
                menor_pontuacao = pontuacao
                melhor_resultado = (
                    linha["Autorização"], linha["NSU"], linha["Chave"], linha["Valor"], status_final, round(pontuacao, 2)
                )

        return pd.Series(melhor_resultado) if melhor_resultado else pd.Series([None, None, None, None, "Não Conciliado", 999])
    except Exception as e:
        logging.error(f"Erro em selecionar_melhor_por_pontuacao: {str(e)}", exc_info=True)
        return pd.Series([None, None, None, None, "Erro na Conciliação", 999])



def conciliar_por_nsu(row, df_erp_base, tolerancia_dias=1, tolerancia_valor=0.15):
    try:
        logging.debug(f"[NSU] Tentando conciliar por NSU: {row['NSU/DOC']}")

        nsu_cielo = str(row['NSU/DOC'])
        aut_cielo = str(row['AUTORIZAÇÃO'])

        # --- NOVO: checar NSU e Autorização 100% iguais ---
        match = df_erp_base[
            (df_erp_base["NSU"].astype(str) == nsu_cielo) &
            (df_erp_base["Autorização"].astype(str) == aut_cielo)
        ]
        if not match.empty:
            linha = match.iloc[0]
            return pd.Series([linha["Autorização"], linha["NSU"], linha["Chave"], linha["Valor"], "Conciliado", 0])

        # --- Busca fuzzy original ---
        nsus_erp = df_erp_base['NSU'].astype(str)
        correspondencias = process.extract(nsu_cielo, nsus_erp, scorer=fuzz.ratio, limit=5)

        correspondencias_validas = [(texto, score, idx) for texto, score, idx in correspondencias if score >= 80]
        if not correspondencias_validas:
            return pd.Series([None, None, None, None, "Não Conciliado", 999])

        melhor_resultado = None
        menor_pontuacao = float("inf")

        for _, score, idx in correspondencias_validas:
            linha = df_erp_base.iloc[idx]
            dias_dif = abs((linha["Emissão"] - row["DATA DA VENDA"]).days)
            valor_dif = abs(linha["Valor"] - row["VALOR DA PARCELA"])
            pontuacao = dias_dif * 10 + valor_dif * 100 + (100 - score)
            status = []
            if dias_dif > 1:
                status.append("Divergência de Data")
            if valor_dif > 0.10:
                status.append("Divergência de Valor")
            status_final = " e ".join(status) if status else "Conciliado"
            if pontuacao < menor_pontuacao:
                menor_pontuacao = pontuacao
                melhor_resultado = (
                    linha["Autorização"], linha["NSU"], linha["Chave"], linha["Valor"], status_final, round(pontuacao, 2)
                )

        return pd.Series(melhor_resultado)
    except Exception as e:
        logging.error(f"Erro em conciliar_por_nsu: {str(e)}", exc_info=True)
        return pd.Series([None, None, None, None, "Erro na Conciliação NSU", 999])




def conciliar_por_autorizacao(row, df_erp_base, tolerancia_dias=2, tolerancia_valor=0.30):
    try:
        aut_cielo = str(row['AUTORIZAÇÃO'])
        nsu_cielo = str(row['NSU/DOC'])

        # --- NOVO: checar NSU e Autorização 100% iguais ---
        match = df_erp_base[
            (df_erp_base["Autorização"].astype(str) == aut_cielo) &
            (df_erp_base["NSU"].astype(str) == nsu_cielo)
        ]
        if not match.empty:
            linha = match.iloc[0]
            return pd.Series([linha["Autorização"], linha["NSU"], linha["Chave"], linha["Valor"], "Conciliado", 0])

        # --- Busca fuzzy original ---
        autorizacoes_erp = df_erp_base['Autorização'].astype(str)
        correspondencias = process.extract(aut_cielo, autorizacoes_erp, scorer=fuzz.ratio, limit=5)

        correspondencias_validas = [(texto, score, idx) for texto, score, idx in correspondencias if score >= 80]
        if not correspondencias_validas:
            return pd.Series([None, None, None, None, "Não Conciliado", 999])

        melhor_resultado = None
        menor_pontuacao = float("inf")

        for _, score, idx in correspondencias_validas:
            linha = df_erp_base.iloc[idx]
            dias_dif = abs((linha["Emissão"] - row["DATA DA VENDA"]).days)
            valor_dif = abs(linha["Valor"] - row["VALOR DA PARCELA"])
            pontuacao = dias_dif * 10 + valor_dif * 100 + (100 - score)
            status = []
            if dias_dif > tolerancia_dias:
                status.append("Divergência de Data")
            if valor_dif > tolerancia_valor:
                status.append("Divergência de Valor")
            status_final = " | ".join(status) if status else "Conciliado por Autorização"
            if pontuacao < menor_pontuacao:
                menor_pontuacao = pontuacao
                melhor_resultado = (
                    linha["Autorização"], linha["NSU"], linha["Chave"], linha["Valor"], status_final, round(pontuacao, 2)
                )

        return pd.Series(melhor_resultado) if melhor_resultado else pd.Series([None]*5 + ["Não Conciliado", 999])
    except Exception as e:
        logging.error(f"Erro em conciliar_por_autorizacao: {str(e)}", exc_info=True)
        return pd.Series([None, None, None, None, "Erro na Conciliação", 999])




def marcar_duplicados_com_pior_score(df):
    """
    Marca duplicados na coluna 'Chave ERP' com a pior pontuação (998).
    """
    if "Chave ERP" not in df.columns:
        return df
    duplicados = df.duplicated(subset=["Chave ERP"], keep=False)
    df.loc[duplicados, "Pontuação"] = 998
    return df

def marcar_e_filtrar_chaves_utilizadas(df_erp, df_conciliado):
    """
    Marca as chaves já utilizadas e retorna o ERP disponível para novas conciliações.
    """
    if "Chave ERP" not in df_conciliado.columns or "Chave" not in df_erp.columns:
        return df_erp, df_erp
    chaves_usadas = df_conciliado["Chave ERP"].dropna().unique()
    df_erp_disponivel = df_erp[~df_erp["Chave"].astype(str).isin(chaves_usadas.astype(str))]
    return df_erp, df_erp_disponivel

def gerar_relatorio_df_formatado(df_conciliado, df_nao_conciliado, df_cancelamento_venda, df_tarifas, df_aluguel):
    """
    Gera um DataFrame resumo para o relatório final.
    """
    resumo = {
        "Tipo": ["Conciliados", "Não Conciliados", "Cancelamentos", "Tarifas", "Aluguel"],
        "Quantidade": [
            len(df_conciliado),
            len(df_nao_conciliado),
            len(df_cancelamento_venda),
            len(df_tarifas),
            len(df_aluguel)
        ],
        "Valor Líquido": [
            df_conciliado["VALOR LÍQUIDO"].sum() if "VALOR LÍQUIDO" in df_conciliado else 0,
            df_nao_conciliado["VALOR LÍQUIDO"].sum() if "VALOR LÍQUIDO" in df_nao_conciliado else 0,
            df_cancelamento_venda["VALOR LÍQUIDO"].sum() if "VALOR LÍQUIDO" in df_cancelamento_venda else 0,
            df_tarifas["VALOR LÍQUIDO"].sum() if "VALOR LÍQUIDO" in df_tarifas else 0,
            df_aluguel["VALOR LÍQUIDO"].sum() if "VALOR LÍQUIDO" in df_aluguel else 0,
        ]
    }
    return pd.DataFrame(resumo)


#=============================================
# ===  INTERFACE STREAMLIT ===
#=============================================

def main():
    try:
        # --- BARRA LATERAL ---
        with st.sidebar:
            st.markdown("# App Conciliação Bancária")
            st.markdown("### Carregar planilhas")
            caminho_erp = st.file_uploader("ERP (CSV)", type=["csv"], key="erp_uploader")
            caminho_cielo = st.file_uploader("Cielo (XLSX)", type=["xlsx"], key="cielo_uploader")

        # --- ÁREA PRINCIPAL ---
        if caminho_erp is None or caminho_cielo is None:
            st.subheader("Bem-vindo ao Sistema de Conciliação")
            st.markdown("""...""")  # Mantenha seu HTML original
            st.warning("⚠️ Por favor, faça upload de ambos os arquivos para iniciar a conciliação")
            return

        # --- CARREGAMENTO DOS DADOS ---
        try:
            with st.spinner('📂 Carregando planilhas...'):
                df_erp = carregar_planilha(caminho_erp)
                df_cielo = carregar_planilha(caminho_cielo)
        except Exception as e:
            logging.error(f"Erro ao carregar planilhas: {str(e)}", exc_info=True)
            st.error(f"❌ Erro ao carregar arquivos: {str(e)}")
            return

        # --- PROCESSAMENTO INICIAL ---
        with st.spinner('🔧 Processando dados...'):
            try:
                df_cielo = limpar_cielo(df_cielo)
                df_erp = limpar_erp(df_erp)

                # Filtros iniciais
                df_cancelamento = df_cielo[df_cielo["Tipo de lançamento"] == "Cancelamento"].copy()
                df_tarifas = df_cielo[df_cielo["Tipo de lançamento"] == "Tarifa"].copy()
                df_aluguel = df_cielo[df_cielo["Tipo de lançamento"] == "Aluguel"].copy()
                
                df_cielo_principal = df_cielo[~df_cielo["Tipo de lançamento"].isin(
                    ["Cancelamento", "Tarifa", "Pagamento Realizado", "Saldo Anterior", "Aluguel"]
                )].copy()

            except Exception as e:
                logging.error(f"Erro no processamento inicial: {str(e)}", exc_info=True)
                st.error(f"❌ Erro no processamento: {str(e)}")
                return

        # --- CONCILIAÇÃO PRINCIPAL ---
        with st.spinner('🔁 Realizando conciliação...'):
            try:
                # 1ª Rodada: Conciliação por dados básicos
                df_cielo_principal[["Autorização ERP", "NSU ERP", "Chave ERP", "Valor ERP", "Status", "Pontuação"]] = df_cielo_principal.apply(
                    lambda row: selecionar_melhor_por_pontuacao(row, df_erp, tolerancia_dias=1, tolerancia_valor=0.10),
                    axis=1
                )

                # Separa conciliados e não conciliados
                df_conciliados = df_cielo_principal[df_cielo_principal["Pontuação"] != 999].copy()
                df_nao_conciliados = df_cielo_principal[df_cielo_principal["Pontuação"] == 999].copy()

                # Filtra ERP disponível
                df_erp, df_erp_disponivel = marcar_e_filtrar_chaves_utilizadas(df_erp, df_conciliados)

                # 2ª Rodada: Conciliação por NSU
                if not df_nao_conciliados.empty:
                    df_nao_conciliados[["Autorização ERP", "NSU ERP", "Chave ERP", "Valor ERP", "Status", "Pontuação"]] = df_nao_conciliados.apply(
                        lambda row: conciliar_por_nsu(row, df_erp_disponivel, tolerancia_dias=5, tolerancia_valor=0.30),
                        axis=1
                    )

                    # Atualiza listas
                    novos_conciliados = df_nao_conciliados[df_nao_conciliados["Pontuação"] != 999].copy()
                    df_conciliados = pd.concat([df_conciliados, novos_conciliados])
                    df_nao_conciliados = df_nao_conciliados[df_nao_conciliados["Pontuação"] == 999].copy()

                    # Atualiza ERP disponível
                    df_erp, df_erp_disponivel = marcar_e_filtrar_chaves_utilizadas(df_erp, df_conciliados)

                # 3ª Rodada: Conciliação por Autorização
                if not df_nao_conciliados.empty:
                    df_nao_conciliados[["Autorização ERP", "NSU ERP", "Chave ERP", "Valor ERP", "Status", "Pontuação"]] = df_nao_conciliados.apply(
                        lambda row: conciliar_por_autorizacao(row, df_erp_disponivel),
                        axis=1
                    )

                    # Atualiza listas
                    novos_conciliados = df_nao_conciliados[df_nao_conciliados["Pontuação"] != 999].copy()
                    df_conciliados = pd.concat([df_conciliados, novos_conciliados])
                    df_nao_conciliados = df_nao_conciliados[df_nao_conciliados["Pontuação"] == 999].copy()

                # Marcar duplicados finais
                df_conciliados = marcar_duplicados_com_pior_score(df_conciliados)

            except Exception as e:
                logging.error(f"Erro na conciliação: {str(e)}", exc_info=True)
                st.error(f"❌ Erro na conciliação: {str(e)}")
                return

# --- RELATÓRIO FINAL ---
        # --- RELATÓRIO FINAL ---
        with st.spinner('📊 Gerando relatórios...'):

            # === DEBUG: mostrar dtypes das colunas financeiras antes de formatar ===
            cols_formatar = [
                "VALOR DA PARCELA",
                "VALOR LÍQUIDO",
                "valor total da transação",
                "taxa/tarifa",
                "Valor ERP"
            ]

            try:
                # Cálculo dos totais
                total_conciliado = df_conciliados["VALOR LÍQUIDO"].sum()
                total_nao_conciliado = (
                    df_nao_conciliados["VALOR LÍQUIDO"].sum()
                    if not df_nao_conciliados.empty else 0
                )
                total_cancelamentos = df_cancelamento["VALOR LÍQUIDO"].sum()

                # Exibição dos resultados
                st.header("Resultados da Conciliação")
                col1, col2, col3 = st.columns(3)
                col1.metric("✅ Conciliados", f"R$ {total_conciliado:,.2f}", f"{len(df_conciliados)} registros")
                col2.metric("⚠ Não Conciliados", f"R$ {total_nao_conciliado:,.2f}", f"{len(df_nao_conciliados)} registros")
                col3.metric("❌ Cancelamentos", f"R$ {total_cancelamentos:,.2f}", f"{len(df_cancelamento)} registros")

                # Geração do Excel com formatação brasileira
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:

                    def formatar(df):
                        for col in cols_formatar:
                            if col in df.columns:
                                # garante que é float antes de formatar
                                df[col] = pd.to_numeric(df[col], errors='coerce')
                                df[col] = df[col].apply(
                                    lambda x: f"{x:.2f}".replace('.', ',') if pd.notnull(x) else ""
                                )
                        return df

                    formatar(df_conciliados).to_excel(writer, sheet_name='Conciliados', index=False)
                    formatar(df_nao_conciliados).to_excel(writer, sheet_name='Não Conciliados', index=False)
                    formatar(df_cancelamento).to_excel(writer, sheet_name='Cancelamentos', index=False)
                    formatar(df_tarifas).to_excel(writer, sheet_name='Tarifas', index=False)
                    formatar(df_aluguel).to_excel(writer, sheet_name='Aluguel', index=False)

                st.download_button(
                    label="📥 Baixar Relatório Completo",
                    data=output.getvalue(),
                    file_name="Relatorio_Conciliação_Cielo.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            except Exception as e:
                logging.error(f"Erro ao gerar relatório: {str(e)}", exc_info=True)
                st.error(f"❌ Erro ao gerar relatório: {str(e)}")

                return


    except Exception as e:
        logging.error(f"Erro inesperado: {str(e)}", exc_info=True)
        st.error(f"❌ Erro crítico: {str(e)}")

if __name__ == "__main__":
    main()