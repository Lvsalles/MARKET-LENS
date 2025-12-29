import psycopg2

try:
    conn = psycopg2.connect(
        host="db.kzkxqsivqymdtmtyzmqh.supabase.co",
        port=5432,
        database="postgres",
        user="postgres",
        password="SUA_SENHA_AQUI",
        connect_timeout=10
    )

    print("✅ CONEXÃO OK!")
    cur = conn.cursor()
    cur.execute("SELECT NOW();")
    print("DB Time:", cur.fetchone())
    conn.close()

except Exception as e:
    print("❌ ERRO DE CONEXÃO")
    print(e)
