import yaml
import psycopg2
import pandas

def get_config(dbase):
    with open('config.yml', 'r') as f:
        config = yaml.load(f)
    return config[dbase]

def get_connection(config):
    return psycopg2.connect(dbname=config['dbname'], 
                            host=config['host'], 
                            port=config['port'],
                            user=config['user'], 
                            password=config['password'])

def query_db(dbase, query):
    config = get_config(dbase)
    conn = get_connection(config)
    df = pandas.read_sql(query,conn)
    conn.close()
    return df

def query_rs(query):
    return query_db('rs', query)

def query_pg(query):
    return query_db('pg', query)