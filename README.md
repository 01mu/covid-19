# crypto
Get [COVID-19 data](https://github.com/CSSEGISandData/COVID-19) and write it to a database.
## Usage
### Create tables
Create tables after creating a `credentials` file based on `credentials_example`.
| Table | Function |
| --- | --- |
| cases | Includes cumulative data and new cases by country for a given day |
| new_daily | Includes daily aggregates for data independent of country
| key_values | Store confirmed, death, and recovery values
```
./covid-19.py create-tables
```
```
CREATE TABLE cases(id SERIAL PRIMARY KEY)
ALTER TABLE cases ADD COLUMN country TEXT
ALTER TABLE cases ADD COLUMN timestamp INT
...
CREATE TABLE key_values(id SERIAL PRIMARY KEY)
ALTER TABLE key_values ADD COLUMN input_key TEXT
ALTER TABLE key_values ADD COLUMN input_value TEXT

```
### Update cases
Write data to tables.
```
./covid-19.py update-cases
```
```
inserting 53 values for Canada
inserting 53 values for Lithuania
inserting 53 values for Cambodia
...
update: (415, 'deaths_latest')
update: (2373, 'recovered_latest')
update: (3.7276908687909187, 'cfr_total')
```
### Clear tables
Delete table entries.
```
./covid-19.py clear-tables
```
```
cases cleared
new_daily cleared
key_values cleared
```
