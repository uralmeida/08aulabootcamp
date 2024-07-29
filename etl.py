import pandas # type: ignore
import os
import glob


# função de extract que le e consolida


def extrair_dados_e_consolidar(pasta: str) -> pandas.DataFrame:
    arquivos_json = glob.glob(os.path.join(pasta, '*.json'))
    df_list = [pandas.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pandas.concat(df_list, ignore_index=True)
    return df_total

# função que transforma

def calcular_kpi_total_de_vendas(df:pandas.DataFrame) -> pandas.DataFrame:
    df["Total"] = df["Quantidade"] * df["Venda"]
    return df



# função que da load em csv ou parquet

def carregar_dados_cs_ou_parquet(df:pandas.DataFrame, format_saida:list):
    for formato in format_saida:
        if formato == 'csv':
            df.to_csv("dados.csv", index=False)
        if formato == 'parquet':
            df.to_parquet("dados.parquet", index=False)

def pipeline_calcular_kpi_de_vendas_consolidado (pasta: str, formato_de_saida:list):
    data_frame = extrair_dados_e_consolidar(pasta)
    data_frame_calculado = calcular_kpi_total_de_vendas(data_frame)
    carregar_dados_cs_ou_parquet(data_frame_calculado, formato_de_saida)