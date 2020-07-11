#
# covid-19
# github.com/01mu
#

import requests
import csv
import datetime
import time

class COVID:
    class CountryData:
        confirmed = deaths = recovered = []

    def __init__(self, conn):
        self.conn = conn
        self.cases = {}
        self.dates = []

        for t in ['confirmed', 'deaths', 'recovered']:
            self.get_data(t)

        for v in [  ['United States', 'US'],
                    ['Taiwan', 'Taiwan*'],
                    ['South Korea', 'Korea, South']]:
            self.cases[v[0]] = self.cases.pop(v[1])

    def get_data(self, stat):
        open('data', 'wb').write(requests.get((
            'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/' +
            'master/csse_covid_19_data/csse_covid_19_time_series/' +
            'time_series_covid19_') + stat + '_global.csv').content)

        with open('data') as csvfile:
            a = list(csv.reader(csvfile, delimiter=',', quotechar='"'))
            i = 1

            if stat == 'confirmed':
                k = 4

                while k < len(a[0]):
                    d = a[0][k].split('/')
                    dt = datetime.datetime(int(d[2]) + 2000, int(d[0]),
                        int(d[1]), 0, 0)
                    self.dates.append(int(time.mktime(dt.timetuple())))
                    k += 1

            while i < len(a):
                b = list(a[i])
                lb = len(b)
                country = a[i][1]
                j = 4
                z = 0

                if country not in self.cases:
                    self.cases[country] = self.CountryData()
                    self.cases[country].confirmed = [0] * (lb-4)
                    self.cases[country].deaths = [0] * (lb-4)
                    self.cases[country].recovered = [0] * (lb-4)

                while j < lb:
                    v = int(b[j])

                    if stat == 'confirmed':
                        self.cases[country].confirmed[z] += v
                    elif stat == 'deaths':
                        self.cases[country].deaths[z] += v
                    else:
                        self.cases[country].recovered[z] += v

                    j += 1
                    z += 1

                i += 1

    def update_cases(self):
        cur = self.conn.cursor()

        cur.execute('SELECT timestamp FROM cases ORDER BY timestamp \
            DESC LIMIT 1')

        last_timestamp = cur.fetchone()[0]
        new_timestamp = self.dates[-1]

        cur.execute('SELECT SUM(confirmed), SUM(deaths), SUM(recovered) \
            FROM cases WHERE timestamp = %s', (last_timestamp,))

        res = cur.fetchall()[0]

        db_confirmed_sum = res[0]
        db_deaths_sum = res[1]
        db_recovered_sum = res[2]

        recent_confirmed_sum = recent_deaths_sum = recent_recovered_sum = 0

        for key, value in self.cases.items():
            recent_confirmed_sum += value.confirmed[-1]
            recent_deaths_sum += value.deaths[-1]
            recent_recovered_sum += value.recovered[-1]

        all_confirmed_new = recent_confirmed_sum - db_confirmed_sum
        all_deaths_new = recent_deaths_sum - db_deaths_sum
        all_recovered_new = recent_recovered_sum - db_recovered_sum

        for key, value in self.cases.items():
            print('Inserting update for ' + key)

            cur.execute('SELECT confirmed, deaths, recovered FROM cases WHERE \
                country = %s AND timestamp = %s', (key, last_timestamp,))

            res = cur.fetchall()[0]

            recent_confirmed = value.confirmed[-1]
            recent_deaths = value.deaths[-1]
            recent_recovered = value.recovered[-1]

            new_confirmed = recent_confirmed - res[0]
            new_deaths = recent_deaths - res[1]
            new_recovered = recent_recovered - res[2]

            v = (new_timestamp, key,
                recent_confirmed, recent_deaths, recent_recovered,
                new_confirmed, new_deaths, new_recovered,
                self.perc(recent_confirmed, recent_confirmed_sum),
                self.perc(recent_deaths, recent_deaths_sum),
                self.perc(recent_recovered, recent_recovered_sum),
                self.perc(new_confirmed, all_confirmed_new),
                self.perc(new_deaths, all_deaths_new),
                self.perc(new_recovered, all_recovered_new),
                self.perc(recent_deaths, recent_confirmed))

            cur.execute('INSERT INTO cases (timestamp, country, confirmed, \
                deaths, recovered, new_confirmed, new_deaths, new_recovered, \
                confirmed_per, deaths_per, recovered_per, new_confirmed_per, \
                new_deaths_per, new_recovered_per, cfr) VALUES (%s, %s, %s, \
                %s, %s, %s, %s, %s, \
                %s, %s, %s, %s, \
                %s, %s, %s)', v)

        print('Inserting update for Global')

        cfr = self.perc(recent_deaths_sum, recent_confirmed_sum)

        q = 'UPDATE cases SET confirmed = %s, deaths = %s, recovered = %s, \
            new_confirmed = %s, new_deaths = %s, new_recovered = %s, cfr = %s, \
            timestamp = %s WHERE country = \'Global\''

        cur.execute(q, (recent_confirmed_sum, recent_deaths_sum,
            recent_recovered_sum, all_confirmed_new, all_deaths_new,
            all_recovered_new, cfr, new_timestamp))

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
            self.insert_value(cur, i[0], i[1])

        self.conn.commit()

    def init_cases(self):
        cur = self.conn.cursor()
        confirmed = deaths = recovered = 0
        confirmed_total = deaths_total = recovered_total = 0
        days = len(self.cases['China'].confirmed)
        li = days - 1
        inc_confirmed = [0] * days
        inc_deaths = [0] * days
        inc_recovered = [0] * days
        tot_confirmed = [0] * days
        tot_deaths = [0] * days
        tot_recovered = [0] * days

        for key, value in self.cases.items():
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

        for key, value in self.cases.items():
            print('Inserting ' + str(days) + ' values for ' + key)
            prev_confirmed = prev_deaths =  prev_recovered = 0

            for i in range(len(value.confirmed)):
                timestamp = self.dates[i]
                confirmed = value.confirmed[i]
                deaths = value.deaths[i]
                recovered = value.recovered[i]
                new_confirmed = confirmed - prev_confirmed
                new_deaths = deaths - prev_deaths
                new_recovered = recovered - prev_recovered
                confirmed_per = self.perc(confirmed, tot_confirmed[i])
                deaths_per = self.perc(deaths, tot_deaths[i])
                recovered_per = self.perc(recovered, tot_recovered[i])
                new_confirmed_per = self.perc(new_confirmed, inc_confirmed[i])
                new_deaths_per = self.perc(new_deaths, inc_deaths[i])
                new_recovered_per = self.perc(new_recovered, inc_recovered[i])
                cfr = self.perc(deaths, confirmed)

                cur.execute('INSERT INTO cases (timestamp, confirmed, deaths, \
                    cfr, new_confirmed, new_deaths, \
                    country, recovered, new_recovered, \
                    confirmed_per, deaths_per, recovered_per, \
                    new_confirmed_per, new_deaths_per, new_recovered_per) \
                    VALUES (%s, %s, \
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (timestamp, confirmed, deaths, cfr, new_confirmed,
                    new_deaths, key, recovered, new_recovered, confirmed_per,
                    deaths_per, recovered_per, new_confirmed_per,
                    new_deaths_per, new_recovered_per))

                prev_confirmed = confirmed
                prev_deaths = deaths
                prev_recovered = recovered

        for i in range(days):
            d = self.dates[i]

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
            (self.dates[li], tot_confirmed[li],
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
            self.insert_value(cur, i[0], i[1])

        self.conn.commit()

    def perc(self, a, b):
        try:
            val = a / float(b) * 100
        except:
            val = 0

        return val

    def insert_value(self, cur, key, value):
        cur.execute('SELECT * FROM key_values WHERE input_key = %s', (key,))

        if cur.fetchone() == None:
            q = 'INSERT INTO key_values (input_key, input_value) \
                VALUES (%s, %s)'
            vals = (key, value)
        else:
            q = 'UPDATE key_values SET input_value = %s WHERE input_key = %s'
            vals = (value, key)

        cur.execute(q, vals)
