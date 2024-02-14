import psycopg2, os
from dotenv import load_dotenv

def pg_connect():
    load_dotenv()
    connection=psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        database=os.getenv('POSTGRES_DB')
        )
    return connection