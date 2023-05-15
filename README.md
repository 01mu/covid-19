# covid.py
Retrive [historical COVID-19 case information](https://github.com/CSSEGISandData/COVID-19) and write it to a MySQL database.
## Usage
### Source database and add your credentials
1. Create the tables listed in `res/db.sql`.
2. Create `res/credentials` and include your database credentials.
3. Create `res/news` and include your [News API](https://newsapi.org/) API key.
### Initialize
Pull data from [the John Hopkins Coronavirus Resource Center dataset](https://github.com/CSSEGISandData/COVID-19) and insert country and US state population figures.
```
cd covid-19/src
python3 covid.py init
```
```
Inserting states place IDs
Inserting states case data for 04-12-2020
Inserting states case data for 04-13-2020
...
Inserting countries place IDs
Inserting countries case data for 01-21-2021
Inserting countries case data for 01-22-2021
...
Inserting country populations
Inserting state populations

```
### Update news articles from [News API](https://newsapi.org/)
```
cd covid-19/src
python3 covid.py news
```
```
Inserting US news
Inserting country news
```
### Clear tables
```
cd covid-19/src
python3 covid.py clear
```
```
Tables cleared
```

