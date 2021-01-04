#
# covid-19
# github.com/01mu
#

import sys
import json
import urllib
from ago import human
from conn import make_conn
from covid_19 import COVID

def main():
    conn = make_conn('../res/credentials')
    arg = sys.argv[1]

    {'cases': cases, 'ago': ago, 'init': init, 'del': delete,
        'update': update, 'population': population}.get(arg)(conn)

def read_json(page):
    return json.loads(urllib.urlopen(page).read())

def read_file(file_name):
    return map(str.strip, open(file_name, 'r').readlines())

def population(conn):
    cur = conn.cursor()

    fix = {'United Kingdom of Great Britain and Northern Ireland':
        'United Kingdom',
        'Myanmar': 'Burma',
        'Syrian Arab Republic': 'Syria',
        'Venezuela (Bolivarian Republic of)': 'Venezuela',
        'Swaziland': 'Eswatini',
        'Republic of Kosovo': 'Kosovo',
        'Moldova (Republic of)': 'Moldova',
        'Czech Republic': 'Czechia',
        'Iran (Islamic Republic of)': 'Iran',
        'Brunei Darussalam': 'Brunei',
        'Viet Nam': 'Vietnam',
        'United States of America': 'United States',
        'Russian Federation': 'Russia',
        'Lao People\'s Democratic Republic': 'Laos',
        'Korea (Republic of)': 'South Korea',
        'Congo': 'Congo (Brazzaville)',
        'Macedonia (the former Yugoslav Republic of)': 'North Macedonia',
        'Tanzania, United Republic of': 'Tanzania',
        'Congo (Democratic Republic of the)': 'Congo (Kinshasa)',
        'Bolivia (Plurinational State of)': 'Bolivia'}

    countries = read_json('https://restcountries.eu/rest/v2/all')

    for country in countries:
        country_ins = country['name']

        if country['name'] in fix:
            country_ins = fix[country_ins]

        q = 'INSERT INTO population (place, type, population) VALUES \
            (%s, %s, %s)'
        cur.execute(q, (country_ins, 0, country['population']))

    q = 'INSERT INTO population (place, type, population) VALUES \
        ("West Bank and Gaza", 0, 2747943)'
    cur.execute(q)

    q = 'INSERT INTO population (place, type, population) VALUES \
        ("Global", 0, 7713468100)'
    cur.execute(q)

    for state in read_file('../res/states'):
        split = state.split(',')

        q = 'INSERT INTO population (place, type, population) VALUES \
            (%s, %s, %s)'
        cur.execute(q, (split[0], 1, int(split[1])))

    conn.commit()

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

def delete(conn):
    cur = conn.cursor()
    cur.execute('DELETE FROM cases')
    cur.execute('DELETE FROM cases_us')
    cur.execute('DELETE FROM daily')
    cur.execute('DELETE FROM key_values')
    conn.commit()

if __name__ == '__main__':
    main()
