import sys
import json
import urllib
import arrow
import requests
import csv
import datetime
import time
import psycopg2
import pymysql

class PlaceData:
  confirmed = []
  deaths = []
  recovered = []

def get_place_ids(cur, place_type):
  ids = {}
  q = 'SELECT id, place FROM places WHERE place_type = %s'
  cur.execute(q, (place_type))

  print('Getting place IDs for: "' + place_type + '"')

  for v in cur.fetchall():
    ids[v[1]] = v[0]

  return ids

def make_conn(cred_file):
  creds = list(map(str.strip, open(cred_file, 'r').readlines()))

  if creds[0] == 'mysql':
    try:
      return pymysql.connect(db = creds[1],
        user = creds[2],
        passwd = creds[3],
        unix_socket = creds[6])
    except:
      return pymysql.connect(db = creds[1],
        user = creds[2],
        passwd = creds[3])
  else:
    return psycopg2.connect(database = creds[1],
      user = creds[2],
      password = creds[3],
      host = creds[4],
      port = creds[5])

def read_json(page):
  return json.loads(urllib.request.urlopen(page).read())

def read_file(file_name):
  return list(map(str.strip, open(file_name, 'r').readlines()))

def population(conn):
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

  for country in read_json('https://restcountries.com/v2/all'):
    country_ins = country['name']

    print('Inserting population for: ' + country_ins)

    if country['name'] in fix:
      country_ins = fix[country_ins]

    if country_ins not in country_place_ids:
      continue

    cur.execute('INSERT INTO population (place_id, population) \
      VALUES (%s, %s)',
      (country_place_ids[country_ins], country['population'],))

  for state in read_file('../res/states'):
    split = state.split(',')

    print('Inserting population for: ' + split[0])

    cur.execute('INSERT INTO population (place_id, population) \
      VALUES (%s, %s)', (us_place_ids[split[0]], int(split[1]),))

  for v in [['West Bank and Gaza', 2747943], ['Global', 7713468100]]:
    cur.execute('INSERT INTO population (place_id, population) \
      VALUES (%s, %s)', (country_place_ids[v[0]], v[1],))

  conn.commit()

def insert_articles(place_id, last_update, response, cur):
  for article in response['articles']:
    source = article['source']['name']
    url = article['url']
    title = article['title']
    image = article['urlToImage']
    published = arrow.get(article['publishedAt']).int_timestamp

    if published > last_update:
      q = 'INSERT INTO news (place_id, title, source, url, image, \
        published) VALUES (%s, %s, %s, %s, %s, %s)'
      v = (place_id, title, source, url, image, published,)
      cur.execute(q, v)
      print('Insert news article info: ' + str(v))

def get_place_articles(place, place_type, last_update, cur, api):
  url = ('https://newsapi.org/v2/everything?' +
    'qInTitle=(' + place.replace(' ', '%20') +
    ')+(covid%20OR%20coronavirus)&pageSize=100' +
    '&language=en&sortBy=publishedAt&apiKey=' + api)

  return read_json(url)

def insert_us_news(cur, last_update, api):
  cur.execute('SELECT DISTINCT(place), id FROM places WHERE place_type = "us"')

  for state in cur.fetchall():
    response = get_place_articles(state[0], 'us', last_update, cur, api)
    insert_articles(state[1], last_update, response, cur)

def insert_country_news(cur, last_update, api):
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
  url = ('https://newsapi.org/v2/everything?' +
    'qInTitle=(covid%20OR%20coronavirus)&pageSize=100' +
    '&language=en&sortBy=publishedAt&apiKey=' + api)

  response = read_json(url)
  cur.execute('SELECT id FROM places WHERE place = "Global"')
  insert_articles(cur.fetchone()[0], last_update, response, cur)

def news(conn):
  cur = conn.cursor()
  api = read_file('../res/news')[0]

  cur.execute('SELECT input_value FROM key_values WHERE input_key = \
    "news_last_update"')

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
    q = 'INSERT INTO key_values (input_key, input_value) VALUES (%s, %s)'
    cur.execute(q, (key, value,))
  else:
    q = 'UPDATE key_values SET input_value = %s WHERE input_key = %s'
    cur.execute(q, (value, key,))

def perc(a, b):
  try:
    val = a / float(b) * 100
  except:
    val = 0

  return val

def get_dates(stat, place_type, csv_read, dates):
  if stat == 'confirmed':
    k = get_case_pos(stat, place_type)

    while k < len(csv_read[0]):
      d = csv_read[0][k].split('/')
      dt = datetime.datetime(int(d[2]) + 2000, int(d[0]), int(d[1]), 0, 0)
      dates.append(int(time.mktime(dt.timetuple())))
      k += 1

def get_case_pos(stat, place_type):
  if place_type == 'country':
    i = 4
  else:
    if stat == 'confirmed':
      i = 12
    else:
      i = 13

  return i

def get_data(stat, place_type, cases, dates):
  print('Getting data for stat: "' + stat + '" and place type: "' +
    place_type + '"')

  if place_type == 'country':
    open('data', 'wb').write(requests.get((
      'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/' +
      'master/csse_covid_19_data/csse_covid_19_time_series/' +
      'time_series_covid19_') + stat + '_global.csv').content)
  else:
    open('data', 'wb').write(requests.get((
      'https://github.com/CSSEGISandData/COVID-19/' +
      'raw/master/csse_covid_19_data/csse_covid_19_time_series/' +
      'time_series_covid19_') + stat + '_US.csv').content)

  with open('data') as csvfile:
    a = list(csv.reader(csvfile, delimiter = ',', quotechar = '"'))
    i = get_case_pos(stat, place_type)
    d = 1

    get_dates(stat, place_type, a, dates)

    while i < len(a):
      b = list(a[d])
      lb = len(b)
      z = 0

      if place_type == 'country':
        place = a[d][1]
        offset = lb - 4
      else:
        place = a[i][6]
        offset = lb - 12

      j = get_case_pos(stat, place_type)

      if place not in cases:
        cases[place] = PlaceData()
        cases[place].confirmed = [0] * offset
        cases[place].deaths = [0] * offset
        cases[place].recovered = [0] * offset

      while j < lb:
        v = int(b[j])

        if stat == 'confirmed':
          cases[place].confirmed[z] += v
        elif stat == 'deaths':
          cases[place].deaths[z] += v
        else:
          cases[place].recovered[z] += v

        j += 1
        z += 1

      d += 1
      i += 1

def init_cases(conn, place_type, cases, dates, place_ids):
  cur = conn.cursor()

  days = len(cases['China' if place_type == 'country' else 'Maine'].confirmed)

  inc_confirmed = [0] * days
  inc_deaths = [0] * days
  inc_recovered = [0] * days

  tot_confirmed = [0] * days
  tot_deaths = [0] * days
  tot_recovered = [0] * days

  for key, value in cases.items():
    print('Processing total values from ' + key)

    prev_confirmed = prev_deaths =  prev_recovered = 0

    for i in range(len(value.confirmed)):
      confirmed = value.confirmed[i]
      deaths = value.deaths[i]
      recovered = value.recovered[i]

      new_confirmed = confirmed - prev_confirmed
      new_deaths = deaths - prev_deaths
      new_recovered = recovered - prev_recovered

      if new_confirmed < 0: new_confirmed = 0
      if new_deaths < 0: new_deaths = 0
      if new_recovered < 0: new_recovered = 0

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

      if new_confirmed < 0: new_confirmed = 0
      if new_deaths < 0: new_deaths = 0
      if new_recovered < 0: new_recovered = 0

      confirmed_per = perc(confirmed, tot_confirmed[i])
      deaths_per = perc(deaths, tot_deaths[i])
      recovered_per = perc(recovered, tot_recovered[i])

      new_confirmed_per = perc(new_confirmed, inc_confirmed[i])
      new_deaths_per = perc(new_deaths, inc_deaths[i])
      new_recovered_per = perc(new_recovered, inc_recovered[i])

      cfr = perc(deaths, confirmed)

      cur.execute('INSERT INTO cases (timestamp, confirmed, \
        deaths, cfr, new_confirmed, new_deaths, \
        recovered, new_recovered, \
        confirmed_per, deaths_per, recovered_per, \
        new_confirmed_per, new_deaths_per, new_recovered_per, place_id) \
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
        (timestamp, confirmed, deaths, cfr, new_confirmed,
        new_deaths, recovered, new_recovered, confirmed_per,
        deaths_per, recovered_per, new_confirmed_per,
        new_deaths_per, new_recovered_per, place_ids[key],))

      prev_confirmed = confirmed
      prev_deaths = deaths
      prev_recovered = recovered

    print('Inserting place_list value for ' + key)

    cur.execute('INSERT INTO place_list (confirmed, \
      deaths, cfr, new_confirmed, new_deaths, \
      recovered, new_recovered, \
      confirmed_per, deaths_per, recovered_per, \
      new_confirmed_per, new_deaths_per, new_recovered_per, place_id) \
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
      (confirmed, deaths, cfr, new_confirmed,
      new_deaths, recovered, new_recovered, confirmed_per,
      deaths_per, recovered_per, new_confirmed_per,
      new_deaths_per, new_recovered_per, place_ids[key],))

  if place_type == 'country':
    for i in range(days):
      print('Inserting value for Global [' + str(i) + ']')

      cfr_total = tot_deaths[i] / float(tot_confirmed[i]) * 100

      cur.execute('INSERT INTO cases (timestamp, \
        confirmed, deaths, recovered, cfr, \
        new_confirmed, new_deaths, new_recovered, place_id, \
        confirmed_per, deaths_per, recovered_per, new_confirmed_per, \
        new_deaths_per, new_recovered_per) VALUES (%s, %s, \
        %s, %s, %s, %s, %s, %s, %s, 100, 100, 100, 100, 100, 100)',
        (dates[i], tot_confirmed[i], tot_deaths[i], tot_recovered[i],
        cfr_total, inc_confirmed[i], inc_deaths[i], inc_recovered[i],
        place_ids['Global'],))

    print('Inserting place_list value for Global')

    cur.execute('INSERT INTO place_list (confirmed, deaths, recovered, cfr, \
      new_confirmed, new_deaths, new_recovered, place_id, \
      confirmed_per, deaths_per, recovered_per, \
      new_confirmed_per, new_deaths_per, new_recovered_per) \
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 100, 100, 100, 100, 100, 100)',
      (tot_confirmed[i], tot_deaths[i], tot_recovered[i], cfr_total,
      inc_confirmed[i], inc_deaths[i], inc_recovered[i], place_ids['Global'],))

    for i in range(days):
      for j in [['inc_confirmed', inc_confirmed[i]],
          ['inc_deaths', inc_deaths[i]],
          ['inc_recovered', inc_recovered[i]],
          ['tot_confirmed', tot_confirmed[i]],
          ['tot_deaths', tot_deaths[i]],
          ['tot_recovered', tot_recovered[i]]
        ]:
        q = 'INSERT INTO daily(timestamp, type, value) VALUES (%s, %s, %s)'
        v = (dates[i], j[0], j[1],)
        cur.execute(q, v)
        print('Insert daily: ' + str(v))

    for i in [['confirmed_latest', inc_confirmed[i]],
        ['deaths_latest', inc_deaths[i]],
        ['recovered_latest', inc_recovered[i]],
        ['confirmed_total', tot_confirmed[i]],
        ['deaths_total', tot_deaths[i]],
        ['recovered_total', tot_recovered[i]],
        ['cfr_total', cfr_total],
        ['last_update', int(time.time())]
      ]:
      insert_value(cur, i[0], i[1])

  conn.commit()

def get_most_recent_timestamp(cur, place_type):
  if place_type == 'country':
    cur.execute('SELECT id FROM places WHERE place = "China"')
  else:
    cur.execute('SELECT id FROM places WHERE place = "California"')

  place_id = cur.fetchone()[0]

  cur.execute('SELECT timestamp FROM cases WHERE place_id = %s \
    ORDER BY timestamp DESC  LIMIT 1', (place_id,))

  return cur.fetchone()[0]

def check_if_update_available(cur, dates, place_type):
  new_timestamp = dates[-1]
  last_timestamp = get_most_recent_timestamp(cur, place_type)

  if last_timestamp == new_timestamp:
    print('No updates found: ' + str(new_timestamp))
    return (True, last_timestamp, new_timestamp)

  return (False, last_timestamp, new_timestamp)

def update_cases(conn, place_type, cases, dates, place_ids):
  cur = conn.cursor()
  check = check_if_update_available(cur, dates, place_type)

  if check[0] == True:
    return

  last_timestamp = check[1]
  new_timestamp = check[2]

  cur.execute('SELECT SUM(confirmed), SUM(deaths), SUM(recovered) \
    FROM cases INNER JOIN places ON cases.place_id = places.id \
    WHERE timestamp = %s AND places.place_type = %s \
    AND places.place != "Global"',
    (last_timestamp, place_type,))

  db_counts = cur.fetchall()[0]

  db_confirmed_sum = db_counts[0]
  db_deaths_sum = db_counts[1]
  db_recovered_sum = db_counts[2]

  recent_confirmed_sum = 0
  recent_deaths_sum = 0
  recent_recovered_sum = 0

  for key, value in cases.items():
    recent_confirmed_sum += value.confirmed[-1]
    recent_deaths_sum += value.deaths[-1]
    recent_recovered_sum += value.recovered[-1]

  all_confirmed_new = recent_confirmed_sum - db_confirmed_sum
  all_deaths_new = recent_deaths_sum - db_deaths_sum
  all_recovered_new = recent_recovered_sum - db_recovered_sum

  for key, value in cases.items():
    print('Inserting cases update for ' + key)

    place_id = place_ids[key]

    cur.execute('SELECT confirmed, deaths, recovered \
      FROM cases WHERE timestamp = %s AND place_id = %s',
      (last_timestamp, place_id,))

    res = cur.fetchall()[0]

    recent_confirmed = value.confirmed[-1]
    recent_deaths = value.deaths[-1]
    recent_recovered = value.recovered[-1]

    new_confirmed = recent_confirmed - res[0]
    new_deaths = recent_deaths - res[1]
    new_recovered = recent_recovered - res[2]

    if new_confirmed < 0: new_confirmed = 0
    if new_deaths < 0: new_deaths = 0
    if new_recovered < 0: new_recovered = 0

    v = (new_timestamp, recent_confirmed,
      recent_deaths, recent_recovered, new_confirmed, new_deaths, new_recovered,
      perc(recent_confirmed, recent_confirmed_sum),
      perc(recent_deaths, recent_deaths_sum),
      perc(recent_recovered, recent_recovered_sum),
      perc(new_confirmed, all_confirmed_new),
      perc(new_deaths, all_deaths_new),
      perc(new_recovered, all_recovered_new),
      perc(recent_deaths, recent_confirmed),
      place_id)

    cur.execute('INSERT INTO cases (timestamp, confirmed, \
      deaths, recovered, new_confirmed, new_deaths, new_recovered, \
      confirmed_per, deaths_per, recovered_per, new_confirmed_per, \
      new_deaths_per, new_recovered_per, cfr, place_id) VALUES (%s, %s, %s, \
      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', v)

    print('Updating place_list for ' + key)

    cur.execute('UPDATE place_list SET confirmed = %s, \
      deaths = %s, recovered = %s, new_confirmed = %s, new_deaths = %s, \
      new_recovered = %s, confirmed_per = %s, deaths_per = %s, \
      recovered_per = %s, new_confirmed_per = %s, new_deaths_per = %s, \
      new_recovered_per = %s, cfr = %s WHERE place_id = %s', v[1:])

  if place_type == 'country':
    print('Inserting update for Global')

    cfr = perc(recent_deaths_sum, recent_confirmed_sum)

    cur.execute('INSERT INTO cases (timestamp, confirmed, deaths, \
      recovered, cfr, new_confirmed, new_deaths, \
      new_recovered, place_id, \
      confirmed_per, deaths_per, recovered_per, new_confirmed_per, \
      new_deaths_per, new_recovered_per) VALUES (%s, %s, \
      %s, %s, %s, %s, %s, %s, %s, 100, 100, 100, 100, 100, 100)',
      (new_timestamp, recent_confirmed_sum, recent_deaths_sum,
      recent_recovered_sum, cfr, all_confirmed_new, all_deaths_new,
      all_recovered_new, place_ids['Global'],))

    print('Updating place_list value for Global')

    cur.execute('UPDATE place_list SET confirmed = %s, deaths = %s, \
      recovered = %s, cfr = %s, new_confirmed = %s, new_deaths = %s, \
      new_recovered = %s WHERE place_id = %s',
      (recent_confirmed_sum, recent_deaths_sum, recent_recovered_sum, cfr,
        all_confirmed_new, all_deaths_new, all_recovered_new,
      place_ids['Global'],))

    for j in [['inc_confirmed', all_confirmed_new],
        ['inc_deaths', all_deaths_new],
        ['inc_recovered', all_recovered_new],
        ['tot_confirmed', recent_confirmed_sum],
        ['tot_deaths', recent_deaths_sum],
        ['tot_recovered', recent_recovered_sum]
      ]:
      q = 'INSERT INTO daily(timestamp, type, value) VALUES (%s, %s, %s)'
      v = (new_timestamp, j[0], j[1],)
      cur.execute(q, v)
      print('Insert daily: ' + str(v))

    for i in [['confirmed_total', recent_confirmed_sum],
        ['deaths_total', recent_deaths_sum],
        ['recovered_total', recent_recovered_sum],
        ['confirmed_latest', all_confirmed_new],
        ['deaths_latest', all_deaths_new],
        ['recovered_latest', all_recovered_new],
        ['cfr_total', cfr],
        ['last_update', int(time.time())]
      ]:
      insert_value(cur, i[0], i[1])

  conn.commit()

def insert_cases(conn, execute_type):
  def insert_place_index(key, place, cur):
    vals = (key, place,)
    cur.execute('INSERT INTO places (place, place_type) VALUES (%s, %s)', vals)
    print('Inserting place index: ' + str(vals))

  cur = conn.cursor()

  if execute_type == 'init':
    insert_place_index('Global', 'country', cur)

  for place in ['country', 'us']:
    cases = {}
    dates = []

    if place == 'us':
      stats = ['confirmed', 'deaths']
    else:
      stats = ['confirmed', 'deaths', 'recovered']

    for stat in stats:
      get_data(stat, place, cases, dates)

    if place == 'country':
      for v in [['United States', 'US'],
          ['Taiwan', 'Taiwan*'],
          ['North Korea', 'Korea, North'],
          ['South Korea', 'Korea, South']
        ]:
        cases[v[0]] = cases.pop(v[1])

    if execute_type == 'init':
      for key, value in cases.items():
        insert_place_index(key, place, cur)

      place_ids = get_place_ids(cur, place)
      init_cases(conn, place, cases, dates, place_ids)
    else:
      place_ids = get_place_ids(cur, place)
      update_cases(conn, place, cases, dates, place_ids)

  conn.commit()

def delete(conn):
  cur = conn.cursor()
  tables = ['place_list', 'population', 'news', 'daily', 'key_values', 'cases',
    'places']

  for v in tables:
    print('Deleting ' + v)
    cur.execute('DELETE FROM ' + v)

  conn.commit()

def main():
  conn = make_conn('../res/credentials')
  arg = sys.argv[1]

  if arg == 'init':
    insert_cases(conn, 'init')
    population(conn)
  elif arg == 'update':
    insert_cases(conn, 'update')
  elif arg == 'delete':
    delete(conn)
  elif arg == 'news':
    news(conn)
  elif arg == 'population':
    population(conn)
  elif arg == 'fix_final':
    fix_final(conn)

def fix_final(conn):
  previous = {}
  current = {}

  cur = conn.cursor()

  for i in range(8, 10):
    print(i)
    latest = ('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/03-0' +
      str(i) + '-2023.csv')

    open('data', 'wb').write(requests.get(latest).content)

    with open('data') as csvfile:
      for v in list(csv.reader(csvfile, delimiter = ',', quotechar = '"')):
        if v[0] == 'Province_State':
          continue

        place = v[0]
        if i == 8:
          previous[place] = {'confirmed': int(v[5]), 'deaths': int(v[6]), 'recovered': 0}
        else:
          current[place] = {'confirmed': int(v[5]), 'deaths': int(v[6]), 'recovered': 0}

          p_confirmed = previous[place]['confirmed']
          p_deaths = previous[place]['deaths']
          p_recovered = previous[place]['recovered']

          confirmed = current[place]['confirmed']
          deaths = current[place]['deaths']
          recovered = current[place]['recovered']

          cur.execute('SELECT id FROM places WHERE place = %s and place_type = "us"', (place))
          place_id = cur.fetchone()[0]

          cur.execute('UPDATE place_list SET confirmed = %s, deaths = %s, recovered = %s, new_confirmed = %s, new_deaths = %s, new_recovered = %s WHERE place_id = %s', (confirmed, deaths, recovered, confirmed - p_confirmed, deaths - p_deaths, recovered - p_recovered, place_id, ))

          cur.execute('UPDATE cases SET confirmed = %s, deaths = %s, recovered = %s, new_confirmed = %s, new_deaths = %s, new_recovered = %s WHERE place_id = %s AND timestamp = 1678338000', (confirmed, deaths, recovered, confirmed - p_confirmed, deaths - p_deaths, recovered - p_recovered, place_id, ))

  conn.commit()

main()
