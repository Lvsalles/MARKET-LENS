import streamlit as st
import psycopg2

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
st.set_page_config(
    page_title="MARKET LENS — DB Test",
    layout="centered"
)

st.title("MARKET LENS — Database Connection Test")

# --------------------------------------------------
# DATABASE CONNECTION
# --------------------------------------------------
def get_db_conn():
    """
    Creates a PostgreSQL connection using Supabase credentials
    stored in Streamlit secrets.
    """
    if "DATABASE_URL" not in st.secrets:
        raise RuntimeError("DATABASE_URL not found in Streamlit secrets")

    db_url = st.secrets["DATABASE_URL"]

    try:
        conn = psycopg2.connect(
            dbname=db_url.split("/")[-1],
            user=db_url.split("//")[1].split(":")[0],
            password=db_url.split(":")[2].split("@")[0],
            host=db_url.split("@")[1].split(":")[0],
            port=db_url.split(":")[-1],
            sslmode="require",
        )
        return conn

    except Exception as e:
        raise RuntimeError(f"Database connection failed: {e}")


# --------------------------------------------------
# UI
# --------------------------------------------------
st.subheader("🔌 Database Connection Test")

if st.button("Test Database Connection"):
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT NOW();")
        now = cur.fetchone()[0]

        st.success("✅ Connected successfully!")
        st.write("Server time:", now)

        cur.close()
        conn.close()

    except Exception as e:
        st.error("❌ Connection failed")
        st.exception(e)
