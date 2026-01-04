import streamlit as st

st.set_page_config(page_title="Market Lens — DIAGNOSTIC", layout="centered")

st.title("Market Lens — Diagnostic Mode")

st.write("✅ Streamlit iniciou")

# -------------------------------------------------
# Teste 1 — imports básicos
# -------------------------------------------------
try:
    from pathlib import Path
    st.write("✅ pathlib OK")
except Exception as e:
    st.error("❌ erro em pathlib")
    st.exception(e)
    st.stop()

# -------------------------------------------------
# Teste 2 — backend import
# -------------------------------------------------
try:
    from backend.etl import run_etl
    st.write("✅ backend.etl importado")
except Exception as e:
    st.error("❌ ERRO AO IMPORTAR backend.etl")
    st.exception(e)
    st.stop()

# -------------------------------------------------
# Teste 3 — contrato existe
# -------------------------------------------------
contract_path = Path("backend/contracts/mls_contract.yml")
st.write("📄 Caminho do contrato:", str(contract_path))

if not contract_path.exists():
    st.error("❌ CONTRATO NÃO EXISTE")
    st.stop()

st.write("✅ Contrato encontrado")

# -------------------------------------------------
# UI mínima
# -------------------------------------------------
st.divider()
st.success("🎯 Streamlit está funcionando corretamente")

st.write("Se você está vendo esta mensagem, o problema NÃO é Streamlit.")
