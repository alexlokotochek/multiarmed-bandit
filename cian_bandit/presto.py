"""
Этот код вмёрджен в py_common, но работает там только
под python2. Нужно разобраться с импортом hive_conf
to be deleted/
"""
from contextlib import closing
from pyhive import presto
import pandas as pd
#from py_common.hive_conf import presto_config
# want to run locally without py_common

presto_config = {
    'host': 'presto.dmonline.ru',
    'port': 8080,
    'username': 'alaktionov',
    'catalog': 'hive',
}


def set_username(username):
    presto_config["username"] = username


def get_connection():
    return presto.connect(**presto_config)


def run_sql(sql_body, rs=True):
    with closing(get_connection()) as presto_conn:
        with closing(presto_conn.cursor()) as presto_cursor:
            presto_cursor.execute(sql_body)
            if rs:
                return presto_cursor.fetchall()

"""
/to be deleted
and replaced with

from py_common.presto import set_username, run_sql
"""
