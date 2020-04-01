# Sparkify - Data Engineer Nanodegree program Cloud Datawarehouse Project
# By JGEL
# March 2020

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
	"""
    Processing log data from AWS S3 in json format files
    :param cur -> Cursor Object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
	"""
    Processing song data from AWS S3 in json format files
    :param cur -> Cursor Object
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():

	# Reading Config File
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
	# Connection string
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()