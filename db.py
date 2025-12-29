import streamlit as st
from sqlalchemy import create_engine
from urllib.parse import urlparse
import socket


def _validate_db_url(db_url: str) -> None:
    if not db_url or not isinstance(db_url, str):
        raise RuntimeError("database.url inválida (vazia ou não-string).")

    parsed = urlparse(db_url)
    host = parsed.hostname or ""
    port = parsed.port or 5432

    # Bloqueia pooler (causava Tenant not found)
    if "pooler.supabase.com" in host or host.startswith("aws-"):
        raise RuntimeError(
            "URL inválida: você está usando o POOLER do Supabase (aws-*.pooler...:6543).\n"
            "Use o Direct Connection: db.<id>.supabase.co:5432"
        )

    if port == 6543:
        raise RuntimeError(
            "Porta inválida (6543). Isso é pooler.\n"
            "Use a porta 5432 (Direct Connection)."
        )

    if not host.startswith("db.") or not host.endswith(".supabase.co"):
        raise RuntimeError(
            f"Host inválido: {host}\n"
            "O host deve ser do tipo: db.<id>.supabase.co"
        )

    # Teste DNS para falhar rápido
    try:
        socket.gethostbyname(host)
    except Exception:
        raise RuntimeError(
            f"DNS não resolve o host: {host}\n"
            "Copie o host real em Supabase > Settings > Database > Direct connection."
        )


def get_engine():
    """
    Cria engine SQLAlchemy usando Streamlit Secrets:
      st.secrets['database']['url']
    """
    if "database" not in st.secrets or "url" not in st.secrets["database"]:
        raise RuntimeError(
            "Missing Streamlit Secrets.\n"
            "Configure no Streamlit Cloud > Secrets:\n"
            "[database]\n"
            "url = \"postgresql://postgres:...@db.<id>.supabase.co:5432/postgres\""
        )

    db_url = st.secrets["database"]["url"]
    _validate_db_url(db_url)

    # connect_timeout evita travar “rodando para sempre”
    engine = create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=3,
        max_overflow=6,
        connect_args={"connect_timeout": 5},
    )
    return engine
