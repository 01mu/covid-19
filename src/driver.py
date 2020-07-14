#
# covid-19
# github.com/01mu
#

import sys
from ago import human
from conn import make_conn
from covid_19 import COVID

def main():
    conn = make_conn('../res/credentials')
    arg = sys.argv[1]

    {'cases': cases, 'ago': ago, 'init': init, 'update': update}.get(arg)(conn)

def cases(conn):
    p = {'-r': 'recovered, new_recovered', '-d': 'deaths, new_deaths',
        '-c': 'confirmed, new_confirmed'}.get(sys.argv[2],
        'confirmed, new_confirmed')
    cur = conn.cursor()
    cur.execute('SELECT ' + p + ' FROM cases WHERE country = %s \
        ORDER BY timestamp DESC LIMIT 1', (sys.argv[4], ))
    res = cur.fetchone()

    print({'-t': str(res[0]), '-n': str(res[1])}.get(sys.argv[3], str(res[0])))

def ago(conn):
    cur = conn.cursor()
    cur.execute('SELECT input_value FROM key_values WHERE input_key = \
        \'last_update\'')
    print(human(cur.fetchone()[0]))

def init(conn):
    covid = COVID(conn)
    covid.init_cases('countries')
    covid.init_cases('states')

def update(conn):
    covid = COVID(conn)
    covid.update_cases('countries')
    covid.update_cases('states')

if __name__ == '__main__':
    main()
