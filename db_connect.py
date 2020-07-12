#DB Connection Tools

import mysql.connector

def connect_sql_gcp(db_name):
    conn = mysql.connector.connect(
            user='user',
            password='password',
            host='host', #
            port=3306,
            database = db_name,
            charset='utf8mb4'
            )
    cur = conn.cursor()
    return conn, cur
