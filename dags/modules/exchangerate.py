from tracemalloc import start
import pandas as pd
from io import StringIO
import requests


class clickhousefunctions:

    def getExchangeRate(base, symbol, path, start_date=None, end_date=None):
        url = 'https://api.exchangerate.host/' + path
        params = {
            'base': base,
            'source': 'crypto',
            'symbols': symbol,
            'places': 12,
            'format': 'CSV'
        }
        if start_date is not None:
            params['start_date'] = start_date
        if end_date is not None:
            params['end_date'] = end_date
        response = requests.get(url, params=params)
        content = response.content.decode('utf-8')

        csvStringIO = StringIO(content)
        df = pd.read_csv(csvStringIO, sep=",")
        if 'start_date' in params and start_date is not None:
            df.drop(
                ['start_date', 'end_date'], axis=1, inplace=True)
        return df
