import psycopg2

DB_URL = "postgresql://postgres:K%40tha%23030105%24@aws-0-us-east-1.pooler.supabase.com:6543/postgres"

try:
    conn = psycopg2.connect(DB_URL, connect_timeout=10)
    print("✅ CONEXÃO OK COM O BANCO!")
    conn.close()
except Exception as e:
    print("❌ FALHA NA CONEXÃO:")
    print(e)
