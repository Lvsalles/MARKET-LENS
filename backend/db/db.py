"""
Database connection — Market Lens (Cloud-first)

Responsabilidade:
- Criar e fornecer a engine SQLAlchemy
- Ler DATABASE_URL do ambiente
- Nenhuma lógica de negócio
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_engine() -> Engine:
    """
    Retorna uma engine SQLAlchemy usando DATABASE_URL.

    Espera no ambiente algo como:
    postgresql://user:password@host:5432/dbname
    """

    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise RuntimeError(
            "DATABASE_URL não está definida no ambiente."
        )

    return create_engine(
        database_url,
        pool_pre_ping=True,
        future=True,
    )
