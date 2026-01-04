"""
Database connection — Market Lens

Responsabilidade:
- Criar e expor uma engine SQLAlchemy
- Nenhuma lógica de negócio
"""

from __future__ import annotations

import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_database_url() -> str:
    """
    Obtém a DATABASE_URL do ambiente.
    Exemplo (Supabase / Postgres):
    postgresql://user:password@host:5432/dbname
    """
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        raise RuntimeError(
            "DATABASE_URL não definida no ambiente."
        )

    return db_url


def get_engine(echo: bool = False) -> Engine:
    """
    Retorna uma engine SQLAlchemy reutilizável.
    """
    return create_engine(
        get_database_url(),
        echo=echo,
        future=True,
        pool_pre_ping=True,
    )
