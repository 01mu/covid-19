#!/usr/bin/env python

#
# covid-19
# github.com/01mu
#

import sys
import csv
import requests
import datetime
import time
import psycopg2
import MySQLdb

class CountryData:
    confirmed = deaths = recovered = []

def main():
    conn = make_conn('credentials')
    conn.set_character_set('utf8')

    if sys.argv[1] == 'clear-cases':
        conn.cursor().execute('DELETE from cases')
        conn.commit()

        print 'cases cleared'

    if sys.argv[1] == 'update-cases':
        update_cases(conn)

    if sys.argv[1] == 'create-tables':
        create_tables(conn)

def update_cases(conn):
    cur = conn.cursor()

    url = ('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/' +
        'csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-')

    cases = {}
    dates = []

    confirmed = deaths = recovered = new_cases = 0
    confirmed_total = deaths_total = recovered_total = new_cases_recent = 0

    for t in ['Confirmed', 'Deaths', 'Recovered']:
        get_data(url, t, cases, dates)

    for key, value in cases.items():
        print key

        prev = 0
        i = 0

        for i in range(len(value.confirmed)):
            d = dates[i].split('/')
            dt = datetime.datetime(int(d[2]) + 2000, int(d[0]), int(d[1]), 0, 0)

            timestamp = int(time.mktime(dt.timetuple()))
            confirmed = value.confirmed[i]
            deaths = value.deaths[i]
            recovered = value.recovered[i]
            new_cases = confirmed - prev

            try:
                cfr = deaths / (float(confirmed)) * 100
            except:
                cfr = 0

            print (str(timestamp) + ' ' + str(confirmed) + ' ' + str(deaths) +
                ' ' + str(recovered) + ' ' + str(cfr) + ' ' + str(new_cases))

            cur.execute('INSERT INTO cases (timestamp, confirmed, deaths, \
                recovered, cfr, new_cases, country, instance) VALUES (%s, %s, \
                %s, %s, %s, %s, %s, 1)', (timestamp, confirmed, deaths,
                recovered, cfr, new_cases, key))

            prev = confirmed

        confirmed_total += confirmed
        deaths_total += deaths
        recovered_total += recovered
        new_cases_recent += new_cases

    cur.execute('DELETE FROM cases WHERE instance = 0')
    cur.execute('UPDATE cases SET instance = 0 WHERE instance = 1')

    fix = [['Taiwan', 'Taiwan*'], ['United States', 'US'],
        ['Korea, South', 'South Korea']]

    for i in fix:
        cur.execute('UPDATE cases SET country = "' + i[0] +'" \
            WHERE country = "' + i[1] +'"')

    insert_value(cur, 'confirmed_total', confirmed_total)
    insert_value(cur, 'deaths_total', deaths_total)
    insert_value(cur, 'recovered_total', recovered_total)
    insert_value(cur, 'new_cases_recent', new_cases_recent)

    insert_value(cur, 'last_update_cases', int(time.time()))

    conn.commit()

def get_dates(a, dates):
    i = 4

    while i < len(a[0]):
        dates.append(a[0][i])
        i += 1

def get_data(url, stat, cases, dates):
    open('data', 'wb').write(requests.get(url + stat + '.csv').content)

    with open('data') as csvfile:
        a = list(csv.reader(csvfile, delimiter=',', quotechar='"'))
        i = 1
        prev_v = 0

        if stat == 'Confirmed':
            get_dates(a, dates)

        while i < len(a):
            b = list(a[i])
            lb = len(b)
            c = a[i][1]
            j = 4
            z = 0

            if c not in cases:
                cases[c] = CountryData()
                cases[c].confirmed = [0] * (lb-4)
                cases[c].deaths = [0] * (lb-4)
                cases[c].recovered = [0] * (lb-4)

            while j < lb:
                try:
                    v = int(b[j])
                    prev_v = v
                except:
                    v = prev_v

                if stat == 'Confirmed':
                    cases[c].confirmed[z] += v
                elif stat == 'Deaths':
                    cases[c].deaths[z] += v
                else:
                    cases[c].recovered[z] += v

                j += 1
                z += 1

            i += 1

def make_conn(cred_file):
    creds = map(str.strip, open(cred_file, 'r').readlines())

    if creds[0] == 'mysql':
        if len(creds) == 7:
            return MySQLdb.connect(db = creds[1],
                user = creds[2],
                passwd = creds[3],
                unix_socket = creds[6])
        else:
            return MySQLdb.connect(db = creds[1],
                user = creds[2],
                passwd = creds[3])
    else:
        return psycopg2.connect(database = creds[1],
            user = creds[2],
            password = creds[3],
            host = creds[4],
            port = creds[5])

def read_file(file_name):
    return

def create_tables(conn):
    cur = conn.cursor()

    cmds = ["CREATE TABLE cases(id SERIAL PRIMARY KEY)",
            "ALTER TABLE cases ADD COLUMN country TEXT",
            "ALTER TABLE cases ADD COLUMN timestamp INT",
            "ALTER TABLE cases ADD COLUMN confirmed INT",
            "ALTER TABLE cases ADD COLUMN deaths INT",
            "ALTER TABLE cases ADD COLUMN recovered INT",
            "ALTER TABLE cases ADD COLUMN new_cases INT",
            "ALTER TABLE cases ADD COLUMN cfr FLOAT",
            "ALTER TABLE cases ADD COLUMN instance INT",

            "CREATE TABLE key_values(id SERIAL PRIMARY KEY)",
            "ALTER TABLE key_values ADD COLUMN input_key TEXT",
            "ALTER TABLE key_values ADD COLUMN input_value TEXT",]

    for cmd in cmds:
        try:
            print cmd
            cur.execute(cmd)
        except Exception as e:
            conn.rollback()
            print e
            continue

    conn.commit()

def insert_value(cur, key, value):
    cur.execute('SELECT id FROM key_values WHERE input_key = %s', (key,))

    if cur.fetchone() == None:
        q = 'INSERT INTO key_values (input_key, input_value) VALUES (%s, %s)'
        vals = (key, value)

        print 'insert: ' + str(vals)
    else:
        q = 'UPDATE key_values SET input_value = %s WHERE input_key = %s'
        vals = (value, key)

        print 'update: ' + str(vals)

    cur.execute(q, vals)

if __name__ == '__main__':
    main()
