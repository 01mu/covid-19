#!/usr/bin/env python

#
# covid-19
# github.com/01mu
#

import csv
import requests
import datetime
import time

class CountryData:
    confirmed = deaths = recovered = []

url = ('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/' +
    'csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-')

cases = {}
dates = []

def get_dates(a, dates):
    i = 4

    while i < len(a[0]):
        dates.append(a[0][i])
        i = i + 1

def get_data(url, stat, cases, dates):
    open('data', 'wb').write(requests.get(url + stat + '.csv').content)

    with open('data') as csvfile:
        a = list(csv.reader(csvfile, delimiter=',', quotechar='"'))
        i = 1

        if stat == 'Confirmed':
            get_dates(a, dates)

        while i < len(a):
            b = list(a[i])
            j = 4
            z = 0
            lb = len(b)
            country = a[i][1]

            if country not in cases:
                cases[country] = CountryData()
                cases[country].confirmed = [0] * (lb-4)
                cases[country].deaths = [0] * (lb-4)
                cases[country].recovered = [0] * (lb-4)

            while j < lb:
                c = int(b[j])

                if stat == 'Confirmed':
                    cases[country].confirmed[z] = (cases[country].confirmed[z]
                        + c)
                elif stat == 'Deaths':
                    cases[country].deaths[z] = (cases[country].deaths[z]
                        + c)
                else:
                    cases[country].recovered[z] = (cases[country].recovered[z]
                        + c)

                j = j + 1
                z = z + 1

            i = i + 1

for t in ['Confirmed', 'Deaths', 'Recovered']:
    get_data(url, t, cases, dates)

for key, value in cases.items() :
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

        # worldometers.info/coronavirus/coronavirus-death-rate/#correct
        # vox.com/2020/3/5/21165973/coronavirus-death-rate-explained

        try:
            cfr = deaths / (float(confirmed)) * 100
        except:
            cfr = 0

        print (str(timestamp) + ' ' + str(confirmed) + ' ' + str(deaths) +
            ' ' + str(recovered) + ' ' + str(cfr) + ' ' + str(new_cases))

        prev = confirmed
