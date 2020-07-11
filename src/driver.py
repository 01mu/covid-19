#
# covid-19
# github.com/01mu
#

import sys

from conn import make_conn
from ago import human
from covid_19 import COVID

conn = make_conn('../res/credentials')

def cases(conn):
    cur = conn.cursor()

    try:
        p = {'-r': 'recovered, new_recovered', '-d': 'deaths, new_deaths',
            '-c': 'confirmed, new_confirmed'}.get(sys.argv[3])
    except:
        p = 'confirmed, new_confirmed'

    cur.execute('SELECT ' + p + ' FROM cases WHERE country = %s \
        ORDER BY timestamp DESC LIMIT 1', (sys.argv[2], ))
    res = cur.fetchone()

    try:
        print({'-t': str(res[0]), '-n': str(res[1])}.get(sys.argv[4]))
    except:
         print(str(res[0]))

def ago(conn):
    cur = conn.cursor()
    cur.execute('SELECT input_value FROM key_values WHERE input_key = \
        \'last_update\'')
    print(human(cur.fetchone()[0]))

def init(conn):
    covid = COVID(conn)
    covid.init_cases()

def update(conn):
    covid = COVID(conn)
    covid.update_cases()

{'cases': cases, 'ago': ago, 'init': init,
    'update': update}.get(sys.argv[1])(conn)
