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
from ago import human

class CountryData:
    confirmed = deaths = recovered = []

def main():
    cases = {}
    dates = []

    conn = make_conn('credentials')

    if sys.argv[1] == 'cases':
        cur = conn.cursor()

        try:
            if sys.argv[3] == '-recovered':
                p = 'recovered, new_recovered'
            elif sys.argv[3] == '-deaths':
                p = 'deaths, new_deaths'
            elif sys.argv[3] == '-confirmed':
                p = 'confirmed, new_confirmed'
        except:
            p = 'confirmed, new_confirmed'

        cur.execute('SELECT ' + p + ' FROM cases WHERE country = %s \
            ORDER BY timestamp DESC LIMIT 1', (sys.argv[2],))

        res = cur.fetchone()

        try:
            if sys.argv[4] == '-total':
                print(str(res[0]))
            elif sys.argv[4] == '-new':
                print(str(res[1]))
        except:
             print(str(res[0]))

    if sys.argv[1] == 'ago':
        cur = conn.cursor()

        cur.execute('SELECT input_value FROM key_values WHERE input_key = \
            \'last_update\'')

        print(human(cur.fetchone()[0]))

    if sys.argv[1] == 'update-cases':

        for stat in ['confirmed', 'deaths', 'recovered']:
            if get_update_data(conn, stat, cases, dates) == False:
                print('No new data')
                return

        update_cases(cases, dates, conn)

    if sys.argv[1] == 'init-cases':

        for t in ['confirmed', 'deaths', 'recovered']:
            get_data(t, cases, dates)

        init_cases(cases, dates, conn)

    if sys.argv[1] == 'create-tables':
        create_tables(conn)

    if sys.argv[1] == 'clear-tables':
        for v in ['cases', 'daily', 'key_values']:
            conn.cursor().execute('DELETE FROM ' + v)

            print(v + ' cleared')

        conn.commit()

def get_update_data(conn, stat, cases, dates):
    cur = conn.cursor()

    open('data', 'wb').write(requests.get((
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/' +
        'master/csse_covid_19_data/csse_covid_19_time_series/' +
        'time_series_covid19_') + stat + '_global.csv').content)

    with open('data') as csvfile:
        a = list(csv.reader(csvfile, delimiter=',', quotechar='"'))
        i = 1

        if stat == 'confirmed':
            get_dates(a, dates)

            cur.execute('SELECT timestamp FROM cases ORDER BY timestamp \
                DESC LIMIT 1')

            if dates[-1] == cur.fetchone()[0]:
                return False

        while i < len(a):
            country = a[i][1]
            v = int(a[i][-1])

            if country not in cases:
                cases[country] = CountryData()
                cases[country].confirmed = [0] * 1
                cases[country].deaths = [0] * 1
                cases[country].recovered = [0] * 1

            if stat == 'confirmed':
                cases[country].confirmed[0] += v
            elif stat == 'deaths':
                cases[country].deaths[0] += v
            else:
                cases[country].recovered[0] += v

            i += 1

    return True

def update_cases(cases, dates, conn):
    cur = conn.cursor()

    cur.execute('SELECT timestamp FROM cases ORDER BY timestamp \
        DESC LIMIT 1')

    last_timestamp = cur.fetchone()[0]
    new_timestamp = dates[-1]

    # Get total stats from database (excluding new update data)
    cur.execute('SELECT SUM(confirmed), SUM(deaths), SUM(recovered) \
        FROM cases WHERE timestamp = %s', (last_timestamp,))

    res = cur.fetchall()[0]

    db_confirmed_sum = res[0]
    db_deaths_sum = res[1]
    db_recovered_sum = res[2]

    # Get total stats from the latest update (excluding database data)
    recent_confirmed_sum = recent_deaths_sum = recent_recovered_sum = 0

    for key, value in cases.items():
        recent_confirmed_sum += value.confirmed[0]
        recent_deaths_sum += value.deaths[0]
        recent_recovered_sum += value.recovered[0]

    # New total stats for day (all countries)
    all_confirmed_new = recent_confirmed_sum - db_confirmed_sum
    all_deaths_new = recent_deaths_sum - db_deaths_sum
    all_recovered_new = recent_recovered_sum - db_recovered_sum

    # Get country specific data
    for key, value in cases.items():
        print('Inserting update for ' + key)

        cur.execute('SELECT confirmed, deaths, recovered FROM cases WHERE \
            country = %s AND timestamp = %s', (key, last_timestamp,))

        # Data from the day before the update
        res = cur.fetchall()[0]

        # Data from the latest update
        recent_confirmed = value.confirmed[0]
        recent_deaths = value.deaths[0]
        recent_recovered = value.recovered[0]

        # New data
        new_confirmed = recent_confirmed - res[0]
        new_deaths = recent_deaths - res[1]
        new_recovered = recent_recovered - res[2]

        # Percentage specific
        deaths_per = zero_exp(recent_deaths, recent_deaths_sum)
        new_deaths_per = zero_exp(new_deaths, all_deaths_new)

        confirmed_per = zero_exp(recent_confirmed, recent_confirmed_sum)
        new_confirmed_per = zero_exp(new_confirmed, all_confirmed_new)

        recovered_per = zero_exp(recent_recovered, recent_recovered_sum)
        new_recovered_per = zero_exp(new_recovered, all_recovered_new)

        cfr = zero_exp(recent_deaths, recent_confirmed)

        cur.execute('INSERT INTO cases (timestamp, country, confirmed, \
            deaths, recovered, new_confirmed, new_deaths, new_recovered, \
            confirmed_per, deaths_per, recovered_per, new_confirmed_per, \
            new_deaths_per, new_recovered_per, cfr) VALUES (%s, %s, %s, \
            %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, \
            %s, %s, %s)', (new_timestamp, key,
            recent_confirmed, recent_deaths, recent_recovered, new_confirmed,
            new_deaths, new_recovered, confirmed_per, deaths_per, recovered_per,
            new_recovered_per, new_deaths_per, new_recovered_per, cfr))

    print('Inserting update for Global')

    cfr = zero_exp(recent_deaths_sum, recent_confirmed_sum)

    q = 'UPDATE cases SET confirmed = %s, deaths = %s, recovered = %s, \
        new_confirmed = %s, new_deaths = %s, new_recovered = %s, cfr = %s \
        WHERE country = \'Global\''

    cur.execute(q, (recent_confirmed_sum, recent_deaths_sum,
        recent_recovered_sum, all_confirmed_new, all_deaths_new,
        all_recovered_new, cfr))

    for j in [  ['inc_confirmed', all_confirmed_new],
                ['inc_deaths', all_deaths_new],
                ['inc_recovered', all_recovered_new],
                ['tot_confirmed', recent_confirmed_sum],
                ['tot_deaths', recent_deaths_sum],
                ['tot_recovered', recent_recovered_sum]]:
        cur.execute('INSERT INTO daily(timestamp, type, value) \
            VALUES (%s, %s, %s)', (new_timestamp, j[0], j[1],))

    for i in [  ['confirmed_total', recent_confirmed_sum],
                ['deaths_total', recent_deaths_sum],
                ['recovered_total', recent_recovered_sum],
                ['confirmed_latest', all_confirmed_new],
                ['deaths_latest', all_deaths_new],
                ['recovered_latest', all_recovered_new],
                ['cfr_total', cfr],
                ['last_update', int(time.time())]]:
        insert_value(cur, i[0], i[1])

    conn.commit()

def zero_exp(a, b):
    try:
        val = a / float(b) * 100
    except:
        val = 0

    return val

def init_cases(cases, dates, conn):
    cur = conn.cursor()

    confirmed = deaths = recovered = 0
    confirmed_total = deaths_total = recovered_total = 0

    days = len(cases['China'].confirmed)
    li = days - 1

    inc_confirmed = [0] * days
    inc_deaths = [0] * days
    inc_recovered = [0] * days

    tot_confirmed = [0] * days
    tot_deaths = [0] * days
    tot_recovered = [0] * days

    for key, value in cases.items():
        prev_confirmed = prev_deaths =  prev_recovered = 0

        for i in range(len(value.confirmed)):
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

            prev_confirmed = confirmed
            prev_deaths = deaths
            prev_recovered = recovered

    for key, value in cases.items():
        print('Inserting ' + str(days) + ' values for ' + key)

        prev_confirmed = prev_deaths =  prev_recovered = 0

        for i in range(len(value.confirmed)):
            timestamp = dates[i]

            confirmed = value.confirmed[i]
            deaths = value.deaths[i]
            recovered = value.recovered[i]

            new_confirmed = confirmed - prev_confirmed
            new_deaths = deaths - prev_deaths
            new_recovered = recovered - prev_recovered

            confirmed_per = zero_exp(confirmed, tot_confirmed[i])
            deaths_per = zero_exp(deaths, tot_deaths[i])
            recovered_per = zero_exp(recovered, tot_recovered[i])

            new_confirmed_per = zero_exp(new_confirmed, inc_confirmed[i])
            new_deaths_per = zero_exp(new_deaths, inc_deaths[i])
            new_recovered_per = zero_exp(new_recovered, inc_recovered[i])

            cfr = zero_exp(deaths, confirmed)

            cur.execute('INSERT INTO cases (timestamp, confirmed, deaths, \
                cfr, new_confirmed, new_deaths, \
                country, recovered, new_recovered, \
                confirmed_per, deaths_per, recovered_per, \
                new_confirmed_per, new_deaths_per, new_recovered_per) \
                VALUES (%s, %s, \
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (timestamp, confirmed, deaths, cfr, new_confirmed, new_deaths,
                key, recovered, new_recovered, confirmed_per,
                deaths_per, recovered_per, new_confirmed_per,
                new_deaths_per, new_recovered_per))

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
            cur.execute('INSERT INTO daily(timestamp, type, value) \
                VALUES (%s, %s, %s)', (d, j[0], j[1],))

    cfr_total = tot_deaths[li] / float(tot_confirmed[li]) * 100

    cur.execute('INSERT INTO cases (timestamp, confirmed, deaths, \
        cfr, new_confirmed, new_deaths, \
        country, recovered, new_recovered, confirmed_per, \
        deaths_per, recovered_per, new_confirmed_per, new_deaths_per, \
        new_recovered_per) VALUES (%s, %s, \
        %s, %s, %s, %s, \'Global\', %s, %s, 100, 100, 100, 100, 100, 100)',
        (dates[li], tot_confirmed[li],
        tot_deaths[li], cfr_total, inc_confirmed[li],
        inc_deaths[li], tot_recovered[li], inc_recovered[li]))

    for i in [  ['confirmed_total', tot_confirmed[li]],
                ['deaths_total', tot_deaths[li]],
                ['recovered_total', tot_recovered[li]],
                ['confirmed_latest', inc_confirmed[li]],
                ['deaths_latest', inc_deaths[li]],
                ['recovered_latest', inc_recovered[li]],
                ['cfr_total', cfr_total],
                ['last_update', int(time.time())]]:
        insert_value(cur, i[0], i[1])

    '''for i in [  ['Taiwan', 'Taiwan*'],
                ['United States', 'US'],
                ['South Korea', 'Korea, South']]:
        cur.execute("UPDATE cases SET country = '" + i[0] + " \
            ' WHERE country = '" + i[1] + "'")'''

    conn.commit()

def get_dates(a, dates):
    i = 4

    while i < len(a[0]):
        d = a[0][i].split('/')
        dt = datetime.datetime(int(d[2]) + 2000, int(d[0]), int(d[1]), 0, 0)

        dates.append(int(time.mktime(dt.timetuple())))

        i += 1

def get_data(stat, cases, dates):
    open('data', 'wb').write(requests.get((
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/' +
        'master/csse_covid_19_data/csse_covid_19_time_series/' +
        'time_series_covid19_') + stat + '_global.csv').content)

    with open('data') as csvfile:
        a = list(csv.reader(csvfile, delimiter=',', quotechar='"'))
        i = 1

        if stat == 'confirmed':
            get_dates(a, dates)

        while i < len(a):
            b = list(a[i])
            lb = len(b)
            country = a[i][1]
            j = 4
            z = 0

            if country not in cases:
                cases[country] = CountryData()
                cases[country].confirmed = [0] * (lb-4)
                cases[country].deaths = [0] * (lb-4)
                cases[country].recovered = [0] * (lb-4)

            while j < lb:
                v = int(b[j])

                if stat == 'confirmed':
                    cases[country].confirmed[z] += v
                elif stat == 'deaths':
                    cases[country].deaths[z] += v
                else:
                    cases[country].recovered[z] += v

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

    cmds = ["CREATE TABLE cases(country TEXT, timestamp INT, \
                confirmed INT, deaths INT, recovered INT, \
                new_confirmed INT, new_deaths INT, new_recovered INT, \
                confirmed_per FLOAT, deaths_per FLOAT, recovered_per FLOAT, \
                new_confirmed_per FLOAT, new_deaths_per FLOAT, \
                new_recovered_per FLOAT, cfr FLOAT);",
            "CREATE TABLE daily(timestamp INT, type TEXT, value INT);",
            "CREATE TABLE key_values(input_key TEXT, input_value TEXT);"]

    for cmd in cmds:
        try:
            print(cmd)
            cur.execute(cmd)
        except Exception as e:
            conn.rollback()
            print(e)
            continue

    conn.commit()

def insert_value(cur, key, value):
    cur.execute('SELECT * FROM key_values WHERE input_key = %s', (key,))

    if cur.fetchone() == None:
        q = 'INSERT INTO key_values (input_key, input_value) VALUES (%s, %s)'
        vals = (key, value)

        print('Insert: ' + str(vals))
    else:
        q = 'UPDATE key_values SET input_value = %s WHERE input_key = %s'
        vals = (value, key)

        print('Update: ' + str(vals))

    cur.execute(q, vals)

if __name__ == '__main__':
    main()
