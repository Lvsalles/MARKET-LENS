import psycopg2

conn = psycopg2.connect(
    "postgresql://postgres:SENHA_AQUI@aws-0-us-east-1.pooler.supabase.com:6543/postgres"
)

print("🔥 CONECTADO COM SUCESSO")
conn.close()
