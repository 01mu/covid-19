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

    if sys.argv[1] == 'update-cases':
        url = ('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/' +
            'master/csse_covid_19_data/csse_covid_19_time_series/' +
            'time_series_19-covid-')

        cases = {}
        dates = []

        for t in ['Confirmed', 'Deaths', 'Recovered']:
            get_data(url, t, cases, dates)

        update_cases(cases, dates, conn)

    if sys.argv[1] == 'create-tables':
        create_tables(conn)

    if sys.argv[1] == 'clear-tables':
        for v in ['cases', 'daily', 'key_values']:
            conn.cursor().execute('DELETE FROM ' + v)

            print v + ' cleared'

        conn.commit()

def update_cases(cases, dates, conn):
    cur = conn.cursor()

    confirmed = deaths = recovered = 0
    confirmed_total = deaths_total = recovered_total = 0

    days = len(cases['China'].confirmed)

    inc_confirmed = [0] * days
    inc_deaths = [0] * days
    inc_recovered = [0] * days

    tot_confirmed = [0] * days
    tot_deaths = [0] * days
    tot_recovered = [0] * days

    for key, value in cases.items():
        print 'inserting ' + str(days) + ' values for ' + key

        prev_confirmed = prev_deaths =  prev_recovered = 0

        for i in range(len(value.confirmed)):
            timestamp = dates[i]

            confirmed = value.confirmed[i]
            deaths = value.deaths[i]
            recovered = value.recovered[i]

            new_confirmed = confirmed - prev_confirmed
            new_deaths = deaths - prev_deaths
            new_recovered = recovered - prev_recovered

            inc_confirmed[i] += new_confirmed
            inc_deaths[i] += new_deaths
            inc_recovered[i] += new_recovered

            tot_confirmed[i] += confirmed
            tot_deaths[i] += deaths
            tot_recovered[i] += recovered

            try:
                cfr = deaths / float(confirmed) * 100
            except:
                cfr = 0

            cur.execute('INSERT INTO cases (timestamp, confirmed, deaths, \
                recovered, cfr, new_confirmed, new_deaths, new_recovered, \
                country, instance) VALUES (%s, %s, %s, %s, \
                %s, %s, %s, %s, %s, 1)', (timestamp, confirmed, deaths,
                recovered, cfr, new_confirmed, new_deaths, new_recovered, key))

            prev_confirmed = confirmed
            prev_deaths = deaths
            prev_recovered = recovered

    for i in range(days):
        d = dates[i]

        for j in [  ['inc_confirmed', inc_confirmed[i]],
                    ['inc_deaths', inc_deaths[i]],
                    ['inc_recovered', inc_recovered[i]],
                    ['tot_confirmed', tot_confirmed[i]],
                    ['tot_deaths', tot_deaths[i]],
                    ['tot_recovered', tot_recovered[i]]]:
            cur.execute('INSERT INTO daily(timestamp, type, value, \
            instance) VALUES (%s, %s, %s, 1)', (d, j[0], j[1],))

    for v in [  'DELETE FROM daily WHERE instance = 0',
                'UPDATE daily SET instance = 0 WHERE instance = 1',
                'DELETE FROM cases WHERE instance = 0',
                'UPDATE cases SET instance = 0 WHERE instance = 1']:
        cur.execute(v)

    for i in [  ['confirmed_total', tot_confirmed[days - 1]],
                ['deaths_total', tot_deaths[days - 1]],
                ['recovered_total', tot_recovered[days - 1]],
                ['confirmed_latest', inc_confirmed[days - 1]],
                ['deaths_latest', inc_deaths[days - 1]],
                ['recovered_latest', inc_recovered[days - 1]],
                ['cfr_total', tot_deaths[days - 1] /
                    float(tot_confirmed[days - 1]) * 100],
                ['last_update', int(time.time())]]:
        insert_value(cur, i[0], i[1])

    for i in [  ['Taiwan', 'Taiwan*'],
                ['United States', 'US'],
                ['Korea, South', 'South Korea']]:
        cur.execute("UPDATE cases SET country = '" + i[0] + " \
            ' WHERE country = '" + i[1] + "'")

    conn.commit()

def get_dates(a, dates):
    i = 4

    while i < len(a[0]):
        d = a[0][i].split('/')
        dt = datetime.datetime(int(d[2]) + 2000, int(d[0]), int(d[1]), 0, 0)
        timestamp = int(time.mktime(dt.timetuple()))

        dates.append(timestamp)
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
        try:
            c = MySQLdb.connect(db = creds[1],
                user = creds[2],
                passwd = creds[3],
                unix_socket = creds[6])
        except:
            c = MySQLdb.connect(db = creds[1],
                user = creds[2],
                passwd = creds[3])

        c.set_character_set('utf8')

        return c
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
            "ALTER TABLE cases ADD COLUMN new_confirmed INT",
            "ALTER TABLE cases ADD COLUMN new_deaths INT",
            "ALTER TABLE cases ADD COLUMN new_recovered INT",
            "ALTER TABLE cases ADD COLUMN cfr FLOAT",
            "ALTER TABLE cases ADD COLUMN instance INT",

            "CREATE TABLE daily(id SERIAL PRIMARY KEY)",
            "ALTER TABLE daily ADD COLUMN timestamp INT",
            "ALTER TABLE daily ADD COLUMN type TEXT",
            "ALTER TABLE daily ADD COLUMN value INT",
            "ALTER TABLE daily ADD COLUMN instance INT",

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
