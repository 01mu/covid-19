CREATE database covid;

USE covid;

CREATE TABLE places(id BIGINT unsigned AUTO_INCREMENT, place TEXT,
    place_type TEXT,
    PRIMARY KEY(id));

CREATE TABLE cases(place_id BIGINT unsigned, timestamp BIGINT,
    confirmed BIGINT, deaths BIGINT, recovered BIGINT,
    new_confirmed BIGINT, new_deaths BIGINT, new_recovered BIGINT,
    confirmed_per FLOAT, deaths_per FLOAT, recovered_per FLOAT,
    new_confirmed_per FLOAT, new_deaths_per FLOAT,
    new_recovered_per FLOAT, cfr FLOAT,
    FOREIGN KEY(place_id) REFERENCES places (id));

CREATE TABLE place_list(place_id BIGINT unsigned,
    confirmed BIGINT, deaths BIGINT, recovered BIGINT,
    new_confirmed BIGINT, new_deaths BIGINT, new_recovered BIGINT,
    confirmed_per FLOAT, deaths_per FLOAT, recovered_per FLOAT,
    new_confirmed_per FLOAT, new_deaths_per FLOAT,
    new_recovered_per FLOAT, cfr FLOAT,
    FOREIGN KEY(place_id) REFERENCES places (id));

CREATE TABLE population(place_id BIGINT unsigned, population BIGINT,
    FOREIGN KEY(place_id) REFERENCES places (id));

CREATE TABLE news(place_id BIGINT unsigned, title LONGTEXT, source TEXT,
    url TEXT, image TEXT, published INT,
    FOREIGN KEY(place_id) REFERENCES places (id));

CREATE TABLE daily(timestamp INT, type TEXT, value INT);

CREATE TABLE key_values(input_key TEXT, input_value TEXT);

ALTER TABLE news MODIFY COLUMN title LONGTEXT CHARACTER SET utf8 COLLATE
    utf8_general_ci NOT NULL;
