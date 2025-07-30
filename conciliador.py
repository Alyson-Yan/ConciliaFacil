from rapidfuzz import fuzz
import pandas as pd
import logging

def conciliar_cielo_erp(df_cielo, df_erp, tolerancia_dias=5, tolerancia_valor=0.20):
    df_cielo = df_cielo.copy()
    df_erp = df_erp.copy()

    # Normalizar chaves
    df_erp["Chave"] = pd.to_numeric(df_erp["Chave"], errors="coerce").astype("Int64")
    df_erp["Usada"] = False

    # Adiciona colunas de resultado na df_cielo
    df_cielo["Autorização ERP"] = None
    df_cielo["NSU ERP"] = None
    df_cielo["Chave ERP"] = None
    df_cielo["Valor ERP"] = None
    df_cielo["Emissão ERP"] = None
    df_cielo["Parcela ERP"] = None
    df_cielo["Total Parcelas ERP"] = None
    df_cielo["Status"] = "Não conciliado"
    df_cielo["Pontuação"] = 999

    for i, row in df_cielo.iterrows():
        if pd.isna(row["AUTORIZAÇÃO"]) or pd.isna(row["NSU/DOC"]):
            logging.warning(f"⚠️ Linha {i} ignorada por dados ausentes.")
            continue

        logging.debug(f"🔍 Linha {i} - Aut: {row['AUTORIZAÇÃO']}, NSU: {row['NSU/DOC']}, Parcela: {row['PARCELA']}")

        candidatos = df_erp[
            (~df_erp["Usada"]) &
            (abs((df_erp["Emissão"] - row["DATA DA VENDA"]).dt.days) <= tolerancia_dias) &
            (abs(df_erp["Valor"] - row["VALOR DA PARCELA"]) <= tolerancia_valor) &
            (df_erp["Numero da Parcela"] == row["PARCELA"]) &
            (df_erp["Total Parcelas"] == row["TOTAL_PARCELAS"])
        ]

        logging.debug(f"🔎 {len(candidatos)} candidatos encontrados para a linha {i} da Cielo.")

        melhor = None
        menor_pontuacao = float("inf")

        for _, linha in candidatos.iterrows():
            dias_dif = abs((linha["Emissão"] - row["DATA DA VENDA"]).days)
            valor_dif = abs(linha["Valor"] - row["VALOR DA PARCELA"])
            sim_aut = fuzz.ratio(str(linha["Autorização"]), str(row["AUTORIZAÇÃO"]))
            sim_nsu = fuzz.ratio(str(linha["NSU"]), str(row["NSU/DOC"]))

            pontuacao = dias_dif * 10 + valor_dif * 100 + (100 - sim_aut) + (100 - sim_nsu)

            logging.debug(f"➡️ Testando Chave {linha['Chave']} | Dias: {dias_dif}, Valor: {valor_dif}, Aut: {sim_aut}, NSU: {sim_nsu}, Pontuação: {pontuacao:.2f}")

            if pontuacao < menor_pontuacao:
                menor_pontuacao = pontuacao
                melhor = linha

        if melhor is not None:
            idx_erp = df_erp.index[df_erp["Chave"] == melhor["Chave"]].tolist()
            if idx_erp:
                df_erp.at[idx_erp[0], "Usada"] = True

            df_cielo.at[i, "Autorização ERP"] = melhor["Autorização"]
            df_cielo.at[i, "NSU ERP"] = melhor["NSU"]
            df_cielo.at[i, "Chave ERP"] = melhor["Chave"]
            df_cielo.at[i, "Valor ERP"] = melhor["Valor"]
            df_cielo.at[i, "Emissão ERP"] = melhor["Emissão"]
            df_cielo.at[i, "Parcela ERP"] = melhor["Numero da Parcela"]
            df_cielo.at[i, "Total Parcelas ERP"] = melhor["Total Parcelas"]
            df_cielo.at[i, "Status"] = "Conciliado"
            df_cielo.at[i, "Pontuação"] = round(menor_pontuacao, 0)
            logging.info(f"✅ Linha {i} conciliada com chave {melhor['Chave']} (Pontuação: {round(menor_pontuacao, 0)})")
        else:
            logging.info(f"❌ Linha {i} não conciliada (sem candidatos adequados)")

    return df_cielo, df_erp