def insert_upload(conn, filename, filetype, dataset_type, row_count, col_count, stored_path):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO uploads
            (filename, filetype, dataset_type, row_count, col_count, stored_path)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING upload_id;
            """,
            (filename, filetype, dataset_type, row_count, col_count, stored_path),
        )
        upload_id = cur.fetchone()[0]
    conn.commit()
    return upload_id
