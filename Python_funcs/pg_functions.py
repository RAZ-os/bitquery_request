import json
from db_connect import pg_connect
from datetime import datetime


def insert_token_db(since, till, res):
    connection = pg_connect()
    cur = connection.cursor()
    connection.autocommit = True
    since_date = datetime.strptime(since, "%Y-%m-%d")
    till_date = datetime.strptime(till, "%Y-%m-%d")
    qry = "select tokens_insert('{0}', '{1}', '{2}');"
    query_db = qry.format(since_date, till_date, json.dumps(res))
    cur.execute(query_db)
    all_data = cur.fetchone()
    if all_data[0] is None:
        all_data = []
    if connection:
        connection.close()
    return all_data

