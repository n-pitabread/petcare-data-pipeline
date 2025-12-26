"""
Consume consultations from file for testing purposes.
"""

import psycopg2
import json
from datetime import datetime

def insert_to_db(conn, data):
    with conn.cursor() as cursor:
        cursor.execute("""
                    INSERT INTO dbt_db.public.raw_stream 
                    (timestamp, topic, message)
                    VALUES (%s, %s, %s)
                    """, (datetime.now(), 'consultations', json.dumps(data)))
        conn.commit()
    
if __name__ == "__main__":
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='dbt_db',
        user='dbt_user',
        password='dbt_pass'
    )
    
    with open('data/consultations.json', 'r') as f:
        json_lines = f.readlines()
        for line in json_lines:
            data = json.loads(line)
            print(data)
            insert_to_db(conn, data)
    conn.close()