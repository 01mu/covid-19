CREATE TABLE cases(country TEXT, timestamp INT,
    confirmed INT, deaths INT, recovered INT,
    new_confirmed INT, new_deaths INT, new_recovered INT,
    confirmed_per FLOAT, deaths_per FLOAT, recovered_per FLOAT,
    new_confirmed_per FLOAT, new_deaths_per FLOAT,
    new_recovered_per FLOAT, cfr FLOAT);
CREATE TABLE cases_us(state TEXT, timestamp INT,
    confirmed INT, deaths INT, recovered INT,
    new_confirmed INT, new_deaths INT, new_recovered INT,
    confirmed_per FLOAT, deaths_per FLOAT, recovered_per FLOAT,
    new_confirmed_per FLOAT, new_deaths_per FLOAT,
    new_recovered_per FLOAT, cfr FLOAT);
CREATE TABLE daily(timestamp INT, type TEXT, value INT);
CREATE TABLE key_values(input_key TEXT, input_value TEXT);
CREATE TABLE population(place TEXT, type INT, population BIGINT)
