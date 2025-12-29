import re
import pandas as pd
import streamlit as st
from sqlalchemy import text, inspect
from db import get_engine

# =========================
# CONFIG (TEM QUE SER PRIMEIRO)
# =========================
st.set_page_config(page_title="Market Lens", layout="wide")

st.title("📊 Market Lens — Upload de Dados (Staging)")

# =========================
# HELPERS
# =========================

MAX_FILES = 12

def guess_category_from_filename(filename: str) -> str:
    """
    Tenta identificar a categoria a partir do nome do arquivo.
    Você pode melhorar as regras depois.
    """
    name = filename.lower()

    if "land" in name:
        return "Land"
    if "rental" in name or "rent" in name:
        return "Rental"
    if "pending" in name or "pendings" in name or "pnd" in name:
        return "Pendings"
    if "sold" in name or "sld" in name:
        return "Sold"
    if "listing" in name or "listings" in name or "act" in name:
        return "Listings"
    if "propriedade" in name or "propriedades" in name:
        # normalmente esse arquivo é um mix — você pode escolher como tratar
        # aqui vamos mandar pra Listings por padrão, mas você pode mudar.
        return "Listings"

    return "Listings"


def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza nomes de colunas vindas do Excel.
    """
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace("-", "_")
        .str.replace("/", "_")
        .str.replace(r"[^a-z0-9_]+", "", regex=True)
    )
    return df


def map_common_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mapeia colunas comuns de MLS -> schema do stg_mls.
    Se uma coluna não existir, tudo bem.
    """
    df = df.copy()

    # mapeamentos comuns (adicione mais quando quiser)
    rename_map = {
        "#": "row_no",
        "ml_number": "mls_id",
        "mls_number": "mls_id",
        "mls": "mls_id",
        "listing_number": "mls_id",
        "address": "address",
        "street_name": "street",
        "street": "street",
        "city": "city",
        "state": "state",
        "zip": "zipcode",
        "zip_code": "zipcode",
        "zipcode": "zipcode",
        "county": "county",
        "legal_subdivision_name": "subdivision",
        "subdivision": "subdivision",
        "subdivision_condo_name": "subdivision",
        "heated_area": "sqft",
        "living_area": "sqft",
        "sq_ft": "sqft",
        "sqft": "sqft",
        "lot_size": "lot_sqft",
        "lot_sqft": "lot_sqft",
        "total_acreage": "total_acreage",  # se não existir no banco, será filtrado depois
        "current_price": "list_price",
        "list_price": "list_price",
        "sold_price": "sold_price",
        "close_price": "sold_price",
        "beds": "beds",
        "bedrooms": "beds",
        "full_baths": "baths",
        "baths_full": "baths",
        "baths": "baths",
        "garage": "garage",
        "pool": "pool",
        "year_built": "year_built",
        "dom": "dom",
        "adom": "adom",
        "cdom": "cdom",
        "list_date": "list_date",
        "pending_date": "pending_date",
        "sold_date": "sold_date",
        "sold_terms": "financing",
        "financing": "financing",
        "list_agent": "list_agent",
        "listing_agent": "list_agent",
        "sell_agent": "sell_agent",
        "selling_agent": "sell_agent",
        "latitude": "latitude",
        "longitude": "longitude",
        "lat": "latitude",
        "lng": "longitude",
    }

    for c in list(df.columns):
        if c in rename_map and rename_map[c] != c:
            df = df.rename(columns={c: rename_map[c]})

    return df


def ensure_month_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria month_key = YYYY-MM-01 para análises MoM/YoY depois.
    Regra simples:
      - Sold -> usa sold_date
      - Pending -> usa pending_date
      - Listings -> usa list_date
      - fallback: hoje
    """
    df = df.copy()

    # tenta converter datas se existirem
    for col in ["list_date", "pending_date", "sold_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # cria month_key se faltar
    if "month_key" not in df.columns:
        df["month_key"] = pd.NaT

    # prioriza sold_date, depois pending_date, depois list_date
    if "sold_date" in df.columns:
        sold_dt = pd.to_datetime(df["sold_date"], errors="coerce")
        df.loc[df["month_key"].isna(), "month_key"] = sold_dt.dt.to_period("M").dt.to_timestamp().dt.date

    if "pending_date" in df.columns:
        pnd_dt = pd.to_datetime(df["pending_date"], errors="coerce")
        df.loc[df["month_key"].isna(), "month_key"] = pnd_dt.dt.to_period("M").dt.to_timestamp().dt.date

    if "list_date" in df.columns:
        lst_dt = pd.to_datetime(df["list_date"], errors="coerce")
        df.loc[df["month_key"].isna(), "month_key"] = lst_dt.dt.to_period("M").dt.to_timestamp().dt.date

    # fallback: mês atual
    today = pd.Timestamp.today()
    df.loc[df["month_key"].isna(), "month_key"] = today.to_period("M").to_timestamp().date()

    return df


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte tipos básicos sem quebrar.
    """
    df = df.copy()

    numeric_cols = [
        "beds", "baths", "garage", "sqft", "lot_sqft",
        "list_price", "sold_price", "dom", "adom", "cdom",
        "latitude", "longitude", "sp_lp", "ppsqft", "total_acreage"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "pool" in df.columns:
        # aceita True/False, Y/N, Yes/No, 1/0
        df["pool"] = df["pool"].astype(str).str.strip().str.lower().map(
            {"true": True, "false": False, "y": True, "n": False, "yes": True, "no": False, "1": True, "0": False}
        )

    if "year_built" in df.columns:
        df["year_built"] = pd.to_numeric(df["year_built"], errors="coerce").astype("Int64")

    # calcula sp_lp e ppsqft quando possível
    if "sold_price" in df.columns and "list_price" in df.columns:
        df["sp_lp"] = df["sold_price"] / df["list_price"]

    if "sqft" in df.columns:
        if "sold_price" in df.columns:
            df["ppsqft"] = df["sold_price"] / df["sqft"]
        elif "list_price" in df.columns:
            df["ppsqft"] = df["list_price"] / df["sqft"]

    return df


def filter_to_db_columns(df: pd.DataFrame, engine) -> pd.DataFrame:
    """
    NÃO deixa o insert quebrar:
    mantém somente colunas que existem na tabela stg_mls.
    """
    insp = inspect(engine)
    db_cols = set([c["name"] for c in insp.get_columns("stg_mls")])

    keep = [c for c in df.columns if c in db_cols]
    dropped = [c for c in df.columns if c not in db_cols]

    return df[keep], dropped


def delete_previous_category(engine, project_id: str, category: str):
    """
    Evita duplicação: apaga registros antigos do mesmo project + category.
    """
    with engine.begin() as conn:
        conn.execute(
            text("""
                DELETE FROM stg_mls
                WHERE project_id = :project_id
                  AND category = :category
            """),
            {"project_id": project_id, "category": category}
        )


def count_rows_by_category(engine, project_id: str):
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT category, COUNT(*) as n
                FROM stg_mls
                WHERE project_id = :project_id
                GROUP BY category
                ORDER BY category
            """),
            {"project_id": project_id}
        ).fetchall()
    return rows


# =========================
# UI
# =========================

st.subheader("1) Upload (até 12 arquivos)")
uploaded_files = st.file_uploader(
    "Selecione um ou mais arquivos Excel (.xlsx)",
    type=["xlsx"],
    accept_multiple_files=True
)

st.subheader("2) Identificação do projeto")
project_id = st.text_input("Project ID", value="default_project")

st.subheader("3) Categoria (opcional)")
st.caption("Você pode escolher uma categoria fixa, ou deixar o sistema detectar pelo nome do arquivo.")
mode = st.radio("Como definir categoria?", ["Detectar pelo nome do arquivo (recomendado)", "Escolher manualmente"], index=0)

manual_category = None
if mode == "Escolher manualmente":
    manual_category = st.selectbox("Categoria fixa para TODOS os arquivos", ["Listings", "Pendings", "Sold", "Land", "Rental"])

st.divider()

# =========================
# RUN IMPORT
# =========================
if uploaded_files:
    if len(uploaded_files) > MAX_FILES:
        st.error(f"Você selecionou {len(uploaded_files)} arquivos. O máximo é {MAX_FILES}.")
        st.stop()

    # conecta no banco
    try:
        engine = get_engine()
        st.success("Banco conectado com sucesso ✅")
    except Exception as e:
        st.error("Erro ao conectar no banco ❌")
        st.code(str(e))
        st.stop()

    if st.button("📥 Importar TODOS para o banco (stg_mls)"):
        for f in uploaded_files:
            category = manual_category if manual_category else guess_category_from_filename(f.name)

            st.write("")
            st.markdown(f"### 📄 Processando: `{f.name}`  → **{category}**")

            try:
                df = pd.read_excel(f, engine="openpyxl")
                st.info(f"Linhas lidas do Excel: **{len(df)}**")

                df = sanitize_columns(df)
                df = map_common_columns(df)
                df = ensure_month_key(df)
                df = cast_types(df)

                # adiciona campos obrigatórios
                df["project_id"] = project_id
                df["category"] = category

                # evita duplicação: limpa project+category antes
                delete_previous_category(engine, project_id, category)

                # filtra colunas para não quebrar no insert
                df_to_insert, dropped = filter_to_db_columns(df, engine)

                if len(df_to_insert.columns) == 0:
                    st.error("Nenhuma coluna do Excel corresponde ao schema do banco (stg_mls).")
                    st.write("Colunas do arquivo:", list(df.columns))
                    continue

                # insere
                df_to_insert.to_sql(
                    "stg_mls",
                    engine,
                    if_exists="append",
                    index=False,
                    method="multi"
                )

                st.success(f"✅ Importado com sucesso: {f.name}")
                st.write(f"Colunas inseridas: {len(df_to_insert.columns)}")
                if dropped:
                    st.caption(f"Colunas ignoradas (não existem no banco): {', '.join(dropped[:25])}" + (" ..." if len(dropped) > 25 else ""))

            except Exception as e:
                st.error(f"❌ Erro ao importar {f.name}")
                st.code(str(e))

        st.divider()
        st.subheader("Resumo no banco (stg_mls) por categoria")
        rows = count_rows_by_category(engine, project_id)
        if rows:
            st.dataframe(pd.DataFrame(rows, columns=["category", "rows"]))
        else:
            st.info("Nenhum registro encontrado para este project_id.")

else:
    st.info("Envie arquivos para começar. Depois clique em **Importar TODOS**.")
