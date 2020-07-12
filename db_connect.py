#DB Connection Tools

import mysql.connector

def connect_sql_gcp(db_name):
    conn = mysql.connector.connect(
            user='lailai',
            password='dsTVBS84305300tvbs',
            host='10.33.0.3', #
            port=3306,
            database = db_name,
            charset='utf8mb4'
            )
    cur = conn.cursor()
    return conn, cur