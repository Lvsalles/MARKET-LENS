import streamlit as st

st.set_page_config(page_title="Market Lens – Safe Mode", layout="wide")

st.title("🟢 Market Lens – Safe Mode")
st.success("App carregou sem acessar banco")

if st.button("Testar próximo passo"):
    st.write("Botão funcionando")
