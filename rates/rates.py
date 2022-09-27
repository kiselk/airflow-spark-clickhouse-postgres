import requests
from clickhouse_driver import Client
from datetime import datetime
import pandas as pd
from io import StringIO
import csv


client = Client(host='localhost', settings={'use_numpy': True})


def getExchangeRate(base, symbol, path, params=None):
    url = 'https://api.exchangerate.host/' + path
    if not params:
        params = {
            'base': base,
            'source': 'crypto',
            'symbols': symbol,
            'places': 12
        }
    response = requests.get(url, params=params)
    data = response.json()
    return (data['rates'][symbol], data['date'])


def getExchangeRateCSV(base, symbol, path, params=None, format='csv'):
    url = 'https://api.exchangerate.host/' + path
    if not params:
        params = {
            'base': base,
            'source': 'crypto',
            'symbols': symbol,
            'places': 12,
            'format': 'CSV'
        }

    response = requests.get(url, params=params)
    content = response.content.decode('utf-8')
    if format == 'csv':
        cr = csv.reader(content.splitlines(), delimiter=',')
        next(cr)

        return list(cr)
    elif format == 'df':
        csvStringIO = StringIO(content)
        columns = ["code", "rate", "base", "date"]
        # df = pd.read_csv(csvStringIO, sep=",", header=None,
        #                 names=columns)
        # 'date', 'code', 'rate', 'base', 'start_date', 'end_date
        df = pd.read_csv(csvStringIO, sep=",")
        if 'start_date' in params:
            df.drop(
                ['start_date', 'end_date'], axis=1, inplace=True)
        return df
    return None
    # return (data['rates'][symbol], data['date'])


def getLatest(base, symbol):
    return getExchangeRate(base, symbol, path='latest')


def getLatestCSV(base, symbol, format):
    return getExchangeRateCSV(base, symbol, path='latest', format=format)


def getHistorical(base, symbol, format, start_date, end_date):
    return getExchangeRateCSV(base, symbol, path='timeseries', format=format,
                              params={
                                  'start_date': start_date,
                                  'end_date': end_date,
                                  'base': base,
                                  'source': 'crypto',
                                  'symbols': symbol,
                                  'format': 'CSV',
                                  'places': 12
                              })


def createRawTables():
    client.execute('DROP TABLE IF EXISTS raw_rates')
    client.execute('CREATE TABLE raw_rates (base String, symbol String, rate Float64 CODEC(Gorilla), date String, timestamp DateTime CODEC(DoubleDelta)) ENGINE = Memory')


def createRawCSVTables():
    client.execute('DROP TABLE IF EXISTS raw_CSV_rates')
    client.execute(
        'CREATE TABLE raw_CSV_rates (code String,rate String,base String,date String) ENGINE = Memory')


def createAggTables():
    client.execute('DROP TABLE IF EXISTS agg_rates')
    client.execute('CREATE TABLE agg_rates (base String, symbol String, min_rate Float64 CODEC(Gorilla), avg_rate Float64 CODEC(Gorilla), max_rate Float64 CODEC(Gorilla), date String, timestamp DateTime CODEC(DoubleDelta) ) ENGINE = Memory')


def insertRaw(base, symbol, rate, date):
    client.execute(
        'INSERT INTO raw_rates (base, symbol, rate, date, timestamp) VALUES',
        [[base, symbol, float(rate), date, datetime.now()]]
    )


def getRaw():
    return client.execute('select * from raw_rates')


def getRawCSV():
    return client.execute('select * from raw_CSV_rates')


def cleanLastRow():
    client.execute("""
          alter table agg_rates delete where date in (select max(date) from agg_rates);
          """)


def insertRawCSV(df):
    client.execute(""" INSERT INTO raw_CSV_rates VALUES
  """, df)


def insertRawDF(df: pd.DataFrame):
    client.insert_dataframe(""" INSERT INTO raw_CSV_rates VALUES
  """, df)


def aggregate():
    client.execute(
        """
            insert into agg_rates 
            select base, symbol, avg(rate) as avg_rate, min(rate) as min_rate, max(rate) as max_rate, date, NOW()  from raw_rates
            where date not in (select distinct date from agg_rates)
            group by base, symbol, date
            """
    )


def aggregateFromCSV():
    client.execute(""" 
  
  insert into agg_rates 
select base, symbol, avg(rate) as avg_rate, min(rate) as min_rate, max(rate) as max_rate,date,NOW()  from 

(select base, code as symbol, cast(REPLACE (rate,',','.') as float) as rate, cast(date as Date) as date from raw_CSV_rates
where cast(date as Date) not in (select distinct date from agg_rates) ) as converted
group by base, symbol, date;

  """)


def sample():
    base = 'BTC'
    symbol = 'USD'
    # (rate, date) = getLatest(base, symbol)

    data = getLatestCSV(base, symbol, format='df')
    insertRawDF(data)

    historical = getHistorical(
        base, symbol, format='df', start_date='2021-01-01', end_date=datetime.today().strftime('%Y-%m-%d'))
    insertRawDF(historical)

    # insertRaw(base, symbol, rate, date)
    print(getRawCSV())
    # print(csv)


if __name__ == '__main__':
    createRawTables()
    createAggTables()
    createRawCSVTables()
