try:
    result = run_etl(
        xlsx_path=uploaded,
        contract_path=str(CONTRACT_PATH),
        snapshot_date=snapshot,
    )

    st.success("ETL finished successfully!")
    st.json(result)   # result é dict → OK

except Exception as e:
    st.error("Erro ao executar ETL")
    st.exception(e)   # NÃO use st.json aqui
