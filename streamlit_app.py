import streamlit as st
import pandas as pd
import numpy as np
import google.generativeai as genai
import folium
from streamlit_folium import folium_static
from sklearn.linear_model import LinearRegression

# -----------------------------
# 1) SETUP
# -----------------------------
st.set_page_config(page_title="Market Lens Intelligence", layout="wide")

if "GOOGLE_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
    model = genai.GenerativeModel("gemini-1.5-flash")
else:
    st.error("🔑 API Key não configurada nos Secrets!")
    st.stop()

# -----------------------------
# 2) SYNONYMS
# -----------------------------
SYNONYMS = {
    "Price": ["Current Price_num", "Current Price", "Price", "List Price"],
    "SqFt": ["Heated Area_num", "SqFt", "Living Area", "Lot Size Square Footage_num"],
    "Beds": ["Beds_num", "Beds", "Bedrooms"],
    "Baths": ["Full Baths_num", "Full Baths", "Bathrooms"],
    "Address": ["Address", "Full Address", "Street Address"],
    "Zip": ["Zip", "Zip Code", "PostalCode"],
    "Zoning": ["Zoning", "Zoning Code", "Land Use"],
}

ESSENTIAL_COLS = ["Price", "SqFt", "Beds", "Address", "Zip"]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nomes de colunas e remove 'Unnamed'."""
    df = df.copy()

    # Garantir que são strings e tirar espaços
    df.columns = [str(c).strip() for c in df.columns]

    # Remover colunas "Unnamed: ..."
    df = df.loc[:, ~pd.Series(df.columns).str.startswith("Unnamed")].copy()

    # Se ainda houver colunas duplicadas por nome (originais), remove repetidas mantendo a 1ª
    df = df.loc[:, ~df.columns.duplicated()].copy()

    return df


def _rename_by_synonyms(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renomeia qualquer coluna que bater com o dicionário.
    Atenção: isso pode criar duplicatas (ex: List Price e Price -> Price).
    """
    df = df.copy()

    rename_map = {}
    for target, syns in SYNONYMS.items():
        for s in syns:
            if s in df.columns:
                rename_map[s] = target

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def _coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Se existir mais de uma coluna com o mesmo nome (ex.: duas 'Price'),
    cria uma coluna única pegando o primeiro valor não-nulo na linha,
    e remove as duplicadas.
    """
    df = df.copy()

    # nomes duplicados após rename
    cols = pd.Index(df.columns)
    dup_names = cols[cols.duplicated()].unique().tolist()

    if not dup_names:
        return df

    for name in dup_names:
        # pega todas as colunas com esse mesmo nome
        block = df.loc[:, df.columns == name]

        # coalesce: primeiro valor não-nulo horizontalmente
        merged = block.bfill(axis=1).iloc[:, 0]

        # remove todas as colunas duplicadas e mantém só a mesclada
        df = df.drop(columns=[c for c in df.columns if c == name])
        df[name] = merged

    # Reordenar colunas: mantém as essenciais na frente (se existirem)
    front = [c for c in ESSENTIAL_COLS if c in df.columns]
    rest = [c for c in df.columns if c not in front]
    df = df[front + rest]

    return df


def robust_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina InvalidIndexError e normaliza os dados com segurança."""
    df = _normalize_columns(df)
    df = _rename_by_synonyms(df)

    # CRÍTICO: aqui é onde seu código original falhava
    # (duplicatas criadas pelo rename)
    df = _coalesce_duplicate_columns(df)

    # Garantia de colunas essenciais
    for col in ESSENTIAL_COLS:
        if col not in df.columns:
            df[col] = np.nan

    # Reset index para evitar qualquer resquício de índice estranho
    df = df.reset_index(drop=True)

    # Tipos numéricos
    df["Price"] = pd.to_numeric(
        df["Price"].astype(str).str.replace(r"[$,]", "", regex=True),
        errors="coerce",
    )
    df["SqFt"] = pd.to_numeric(df["SqFt"], errors="coerce")
    df["Beds"] = pd.to_numeric(df["Beds"], errors="coerce")

    # Zip pode vir como float/num -> padroniza para string sem .0
    df["Zip"] = (
        df["Zip"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .replace({"nan": np.nan, "None": np.nan, "": np.nan})
    )

    # Address string
    df["Address"] = (
        df["Address"]
        .astype(str)
        .str.strip()
        .replace({"nan": np.nan, "None": np.nan, "": np.nan})
    )

    # Blindagem final: colunas únicas garantidas
    df = df.loc[:, ~df.columns.duplicated()].copy()

    return df


def read_uploaded_file(f) -> pd.DataFrame:
    """Leitura robusta de CSV/XLSX para Streamlit uploader."""
    name = f.name.lower()
    f.seek(0)

    if name.endswith(".csv"):
        return pd.read_csv(f, low_memory=False)
    elif name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(f)
    else:
        raise ValueError("Formato não suportado. Use CSV ou XLSX.")


# -----------------------------
# 3) UI
# -----------------------------
st.title("🏙️ Global Real Estate Strategic Engine")
st.markdown("---")

files = st.file_uploader("Upload MLS Files (CSV/XLSX)", accept_multiple_files=True)

if files:
    all_dfs = []
    for f in files:
        try:
            df_raw = read_uploaded_file(f)
            df_clean = robust_cleaning(df_raw)
            all_dfs.append(df_clean)

            # Debug útil no sidebar
            st.sidebar.success(f"✅ {f.name} processado ({len(df_clean):,} linhas)")
        except Exception as e:
            st.error(f"Erro no ficheiro {f.name}: {e}")

    if all_dfs:
        # CONCATENAÇÃO SEGURA (agora não quebra por colunas duplicadas)
        try:
            main_df = pd.concat(all_dfs, ignore_index=True, sort=False)
        except Exception as e:
            # diagnóstico extra (se algum caso raríssimo ainda aparecer)
            st.error(f"Falha ao concatenar: {e}")
            for i, df in enumerate(all_dfs):
                if not df.columns.is_unique:
                    st.write(f"DF #{i} ainda tem colunas duplicadas:", df.columns[df.columns.duplicated()].tolist())
            st.stop()

        tab_map, tab_analytics, tab_gemini = st.tabs(
            ["📍 Mapa de Ativos", "📈 Arbitragem & ROI", "🤖 Consultoria Gemini"]
        )

        with tab_map:
            st.subheader("Geolocalização e Contexto de Vizinhança")
            m = folium.Map(location=[27.05, -82.25], zoom_start=11)

            plot_df = main_df.dropna(subset=["Address", "Price"]).head(150)

            for _, row in plot_df.iterrows():
                addr_url = str(row["Address"]).replace(" ", "+")
                gmaps_link = f"https://www.google.com/maps/search/?api=1&query={addr_url}+FL"

                popup_html = f"""
                <div style='width:220px'>
                    <b>{row['Address']}</b><br>
                    Preço: ${row['Price']:,.0f}<br>
                    <a href='{gmaps_link}' target='_blank'>🔗 Abrir no Google Maps</a>
                </div>
                """

                folium.Marker(
                    location=[
                        27.05 + np.random.uniform(-0.04, 0.04),
                        -82.25 + np.random.uniform(-0.04, 0.04),
                    ],
                    popup=folium.Popup(popup_html, max_width=300),
                    icon=folium.Icon(color="blue", icon="home"),
                ).add_to(m)

            folium_static(m)

        with tab_analytics:
            st.subheader("Análise Preditiva de Arbitragem")

            model_df = main_df.dropna(subset=["Price", "SqFt", "Beds"]).copy()

            if len(model_df) > 5:
                X = model_df[["SqFt", "Beds"]].fillna(0)
                y = model_df["Price"]

                reg = LinearRegression().fit(X, y)

                main_df["Fair_Value"] = reg.predict(main_df[["SqFt", "Beds"]].fillna(0))

                # evita divisão por zero
                main_df["Arbitrage_%"] = np.where(
                    main_df["Price"] > 0,
                    ((main_df["Fair_Value"] - main_df["Price"]) / main_df["Price"]) * 100,
                    np.nan,
                )

                st.write("### 💎 Top Oportunidades Subvalorizadas")
                st.dataframe(
                    main_df[main_df["Arbitrage_%"] > 5]
                    .sort_values(by="Arbitrage_%", ascending=False)
                    .head(15)
                )
            else:
                st.warning("Dados insuficientes para análise estatística.")

        with tab_gemini:
            st.subheader("🤖 Assistente de Investimento (McKinsey Style)")
            user_query = st.text_input(
                "Ex: Qual o potencial de construir uma Guest House (ADU) nestes terrenos?"
            )

            if user_query:
                with st.spinner("A analisar o mercado..."):
                    summary = main_df.describe(include="all").to_string()
                    prompt = f"""
Dados Reais do Mercado:
{summary}

Pergunta do Investidor:
{user_query}

Aja como um analista sénior. Considere zoneamento, preço por SqFt e tendências 2025.
"""
                    response = model.generate_content(prompt)
                    st.markdown(response.text)

else:
    st.info("💡 Aguardando ficheiros para ativar o Command Center.")
