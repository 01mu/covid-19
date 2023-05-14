import time
import datetime
import pymysql
import requests
import csv
import json
import urllib
import sys
import arrow

NEWS_API = 'https://newsapi.org/v2'
GIT_BASE = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data'
GIT_DIR_STATES = f'{GIT_BASE}/csse_covid_19_daily_reports_us'
GIT_DIR_COUNTRIES = f'{GIT_BASE}/csse_covid_19_daily_reports'

def open_csv(csvfile):
    return list(csv.reader(csvfile, delimiter = ',', quotechar = '"'))[1:]

def percent(a, b):
    return a / float(b) * 100 if b > 0 else 0

def get_dates(place_type):
    start = datetime.date(2020, 4, 12) if place_type == 'states' else datetime.date(2021, 1, 21)
    end = datetime.date(2023, 3, 9) if place_type == 'states' else datetime.date(2023, 3, 9)
    dates = [start + datetime.timedelta(days=i) for i in range((end - start).days + 1)]

    return [str(date.month).zfill(2) + '-' + str(date.day).zfill(2) + '-' + str(date.year) for date in dates]

def insert_place_ids(conn, place_type):
    cur = conn.cursor()
    places = set()

    print(f'Inserting {place_type} place IDs')

    file = (f'{GIT_DIR_STATES}/03-09-2023.csv') if place_type == 'states' else (f'{GIT_DIR_COUNTRIES}/01-21-2021.csv')
    open('data', 'wb').write(requests.get(file).content)

    with open('data') as csvfile:
        for v in open_csv(csvfile):
            places.add(v[0 if place_type == 'states' else 3])

    for place in places:
        q = 'INSERT INTO places (place, place_type) VALUES (%s, %s)'
        cur.execute(q, (place, 'us' if place_type == 'states' else 'country'))

    q = 'INSERT INTO places (place, place_type) VALUES (%s, %s)'
    v = (('United States' if place_type == 'states' else 'Global'), ('us' if place_type == 'states' else 'country'))
    cur.execute(q, v)

    conn.commit()

def set_current_percent(data, set_for, total):
    for p in data.keys():
        data[p]['current'][f'{set_for}_per'] = percent(data[p]['current'][set_for], total)

def set_new_percent(data, set_for, total):
    for p in data.keys():
        diff = data[p]['current'][set_for] - data[p]['previous'][set_for]
        data[p]['current'][f'new_{set_for}_per'] = percent(diff, total)

def insert_into_cases(cur, values):
    cur.execute('INSERT INTO cases (confirmed, deaths, recovered, new_confirmed, new_deaths, new_recovered, \
        confirmed_per, deaths_per, recovered_per, cfr, place_id, timestamp, new_confirmed_per, new_deaths_per, \
        new_recovered_per) VALUES (%s, %s, 0, %s, %s, 0, %s, %s, 0, 0, %s, %s, %s, %s, 0)', values)

def insert_into_place_list(cur, values):
    cur.execute('INSERT INTO place_list (confirmed, deaths, recovered, new_confirmed, new_deaths, new_recovered, \
        confirmed_per, deaths_per, recovered_per, cfr, place_id, new_confirmed_per, new_deaths_per, new_recovered_per) \
        VALUES (%s, %s, 0, %s, %s, 0, %s, %s, 0, 0, %s, %s, %s, 0)', values)

def insert_cases(conn, place_type):
    cur = conn.cursor()
    data = {}

    dates = get_dates(place_type)

    q = 'SELECT id FROM places WHERE place = %s and place_type = %s'
    v = ('United States' if place_type == 'states' else 'Global', 'us' if place_type == 'states' else 'country')
    cur.execute(q, v)

    aggregate_place_id = cur.fetchone()[0]

    for date_idx, date in enumerate(dates):
        timestamp = time.mktime(datetime.datetime.strptime(date, "%m-%d-%Y").timetuple())

        print(f'Inserting {place_type} case data for {date}')

        file = (f'{GIT_DIR_STATES}/{date}.csv') if place_type == 'states' else (f'{GIT_DIR_COUNTRIES}/{date}.csv')
        open('data', 'wb').write(requests.get(file).content)

        with open('data') as csvfile:
            for v in open_csv(csvfile):
                place = v[0 if place_type == 'states' else 3]

                if place == 'Recovered':
                    continue

                if place not in data:
                    data[place] = {}
                    data[place]['current'] = {'confirmed': 0, 'deaths': 0}
                    data[place]['previous'] = {'confirmed': 0, 'deaths': 0}

                data[place]['current']['confirmed'] += int(v[5 if place_type == 'states' else 7])
                data[place]['current']['deaths'] += int(v[6 if place_type == 'states' else 8])

            total_confirmed = sum([data[p]['current']['confirmed'] for p in data.keys()])
            total_deaths = sum([data[p]['current']['deaths'] for p in data.keys()])

            new_confirmed_total = sum([data[place]['current']['confirmed'] - data[place]['previous']['confirmed']
                for place in data.keys()])

            new_deaths_total = sum([data[place]['current']['deaths'] - data[place]['previous']['deaths']
                for place in data.keys()])

            set_current_percent(data, 'confirmed', total_confirmed)
            set_current_percent(data, 'deaths', total_deaths)
            set_new_percent(data, 'confirmed', new_confirmed_total)
            set_new_percent(data, 'deaths', new_deaths_total)

            for place in data.keys():
                previous_confirmed = data[place]['previous']['confirmed']
                previous_deaths = data[place]['previous']['deaths']

                confirmed = data[place]['current']['confirmed']
                deaths = data[place]['current']['deaths']

                new_confirmed = confirmed - previous_confirmed
                new_deaths = deaths - previous_deaths

                confirmed_per = data[place]['current']['confirmed_per']
                deaths_per = data[place]['current']['deaths_per']

                new_confirmed_per = data[place]['current']['new_confirmed_per']
                new_deaths_per = data[place]['current']['new_deaths_per']

                q = 'SELECT id FROM places WHERE place = %s and place_type = %s'
                cur.execute(q, (place, 'us' if place_type == 'states' else 'country'))

                place_id = cur.fetchone()[0]

                values = (confirmed, deaths, new_confirmed, new_deaths, confirmed_per, deaths_per, place_id, timestamp,
                    new_confirmed_per, new_deaths_per)

                insert_into_cases(cur, values)

                if date_idx == len(dates) - 1:
                    values = (confirmed, deaths, new_confirmed, new_deaths, confirmed_per, deaths_per, place_id,
                        new_confirmed_per, new_deaths_per)

                    insert_into_place_list(cur, values)

            insert_into_cases(cur, (total_confirmed, total_deaths, new_confirmed_total, new_deaths_total, 100, 100,
                aggregate_place_id, timestamp, 100, 100))

            if date_idx == len(dates) - 1:
                insert_into_place_list(cur, (total_confirmed, total_deaths, new_confirmed_total, new_deaths_total, 100,
                    100, aggregate_place_id, 100, 100))

                for i in [['confirmed_latest', new_confirmed_total], ['deaths_latest', new_deaths_total],
                    ['recovered_latest', 0], ['confirmed_total', total_confirmed], ['deaths_total', total_deaths],
                    ['recovered_total', 0], ['cfr_total', 0], ['last_update', int(time.time())]
                  ]:
                  insert_value(cur, i[0], i[1])

            for place in data.keys():
                data[place]['previous'] = dict(data[place]['current'])
                data[place]['current'] = {'confirmed': 0, 'deaths': 0}

    if place_type == 'countries':
        for v in [['United States', 'US'], ['Taiwan', 'Taiwan*'], ['North Korea', 'Korea, North'],
            ['South Korea', 'Korea, South']]:
            cur.execute('UPDATE places SET place = %s WHERE place = %s AND place_type = "country"', (v[0], v[1]))

    conn.commit()

def get_place_ids(cur, place_type):
    ids = {}
    cur.execute('SELECT id, place FROM places WHERE place_type = %s', (place_type))

    for v in cur.fetchall():
        ids[v[1]] = v[0]

    return ids

def read_json(page):
    return json.loads(urllib.request.urlopen(page).read())

def read_file(file_name):
    return list(map(str.strip, open(file_name, 'r').readlines()))

def insert_populations(conn):
    cur = conn.cursor()

    country_place_ids = get_place_ids(cur, 'country')
    us_place_ids = get_place_ids(cur, 'us')

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

    print('Inserting country populations')

    for country in read_json('https://restcountries.com/v2/all'):
        country_insert = country['name']

        if country_insert not in country_place_ids:
            continue

        if country['name'] in fix:
            country_insert = fix[country_insert]

        q = 'INSERT INTO population (place_id, population) VALUES (%s, %s)'
        cur.execute(q, (country_place_ids[country_insert], country['population']))

    print('Inserting state populations')

    for state in read_file('../res/states'):
        split = state.split(',')

        q = 'INSERT INTO population (place_id, population) VALUES (%s, %s)'
        cur.execute(q, (us_place_ids[split[0]], int(split[1])))

    for v in [['West Bank and Gaza', 2747943], ['Global', 7713468100]]:
        cur.execute('INSERT INTO population (place_id, population) VALUES (%s, %s)', (country_place_ids[v[0]], v[1]))

    conn.commit()

def insert_articles(place_id, last_update, response, cur):
    for article in response['articles']:
        source = article['source']['name']
        url = article['url']
        title = article['title']
        image = article['urlToImage']
        published = arrow.get(article['publishedAt']).int_timestamp

        if published > last_update:
            q = 'INSERT INTO news (place_id, title, source, url, image, published) VALUES (%s, %s, %s, %s, %s, %s)'
            cur.execute(q, (place_id, title, source, url, image, published))

def get_place_articles(place, place_type, last_update, cur, api):
    options = ')+(covid%20OR%20coronavirus)&pageSize=100&language=en&sortBy=publishedAt&apiKey='
    url = (NEWS_API + '/everything?qInTitle=(' + place.replace(' ', '%20') + options + api)

    return read_json(url)

def insert_us_news(cur, last_update, api):
    print('Inserting US news')

    cur.execute('SELECT DISTINCT(place), id FROM places WHERE place_type = "us" AND place != "United States"')

    for state in cur.fetchall():
        response = get_place_articles(state[0], 'us', last_update, cur, api)
        insert_articles(state[1], last_update, response, cur)

def insert_country_news(cur, last_update, api):
    print('Inserting country news')

    cur.execute('SELECT place, places.id FROM cases \
        INNER JOIN places ON cases.place_id = places.id \
        WHERE timestamp = (SELECT timestamp \
        FROM cases ORDER BY timestamp DESC LIMIT 1) \
        AND place_type = "country" AND place != "Global" \
        ORDER BY confirmed DESC LIMIT 20')

    for country in cur.fetchall():
        response = get_place_articles(country[0], 'country', last_update, cur, api)
        insert_articles(country[1], last_update, response, cur)

def insert_global_news(cur, last_update, api):
    options = '/everything?qInTitle=(covid%20OR%20coronavirus)&pageSize=100&language=en&sortBy=publishedAt&apiKey='
    url = (NEWS_API + options + api)

    response = read_json(url)
    cur.execute('SELECT id FROM places WHERE place = "Global"')
    insert_articles(cur.fetchone()[0], last_update, response, cur)

def insert_news_articles(conn):
    cur = conn.cursor()
    api = read_file('../res/news')[0]

    cur.execute('SELECT input_value FROM key_values WHERE input_key = "news_last_update"')

    last_update = cur.fetchone()
    last_update = int(last_update[0]) if last_update != None else 0

    insert_us_news(cur, last_update, api)
    insert_country_news(cur, last_update, api)
    insert_global_news(cur, last_update, api)

    insert_value(cur, 'news_last_update', int(time.time()))
    conn.commit()

def insert_value(cur, key, value):
    cur.execute('SELECT * FROM key_values WHERE input_key = %s', (key,))

    if cur.fetchone() == None:
        cur.execute('INSERT INTO key_values (input_key, input_value) VALUES (%s, %s)', (key, value))
    else:
        cur.execute('UPDATE key_values SET input_value = %s WHERE input_key = %s', (value, key))

def clear_tables(conn):
    cur = conn.cursor()

    cur.execute('DELETE FROM key_values;')
    cur.execute('DELETE FROM news;')
    cur.execute('DELETE FROM population;')
    cur.execute('DELETE FROM place_list;')
    cur.execute('DELETE FROM cases;')
    cur.execute('DELETE FROM places;')

    print('Tables cleared')

    conn.commit()

try:
    arg = sys.argv[1]
except:
    print('No option - init, news, or delete')
    exit()

creds = list(map(str.strip, open('../res/credentials', 'r').readlines()))

try:
    conn = pymysql.connect(db = creds[0], user = creds[1], passwd = creds[2], unix_socket = creds[5])
except:
    conn = pymysql.connect(db = creds[0], user = creds[1], passwd = creds[2])

if arg == 'init':
    insert_place_ids(conn, 'states')
    insert_place_ids(conn, 'countries')
    insert_populations(conn)
    insert_cases(conn, 'states')
    insert_cases(conn, 'countries')
elif arg == 'news':
    insert_news_articles(conn)
elif arg == 'clear':
    clear_tables(conn)
else:
    print('Bad option - init, news, or clear')
