import psycopg2
import pytest

def test_postgres_connection():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="changeme",
            host="postgres",
            port="5432"
        )
        cur = conn.cursor()

        cur.execute("SELECT * FROM public.product;")
        rows = cur.fetchall()

        assert len(rows) > 0  # Check if rows are returned

        for row in rows:
            print(row)

    except psycopg2.Error as e:
        pytest.fail(f"Error: {e}")

    finally:
        cur.close()
        conn.close()
