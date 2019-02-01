import requests
import logging
from bs4 import BeautifulSoup

LOG = logging.getLogger()


def get_data(url):
    """Extract data from Soup object and convert into dict"""
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Got unexpected response from %s:\n%s" %
                        (url, response.reason))
    soup = BeautifulSoup(response.content)

    table_of_contents = list(soup.find('tbody').children)
    LOG.debug('Found data in Soup:\n%s' % table_of_contents)
    table_entries = []
    labels = ['Symbol', 'Name', 'Price (Intraday)', 'Change', '% Change', 'Volume', 'Avg Vol (3 month)', 'Market Cap', 'PE Ratio (TTM)']
    for entry in table_of_contents:
        data = {}
        for label in labels:
            data[label] = entry.find('td', attrs={'aria-label': label}).get_text()
        LOG.debug('Collected data for %s:\n%s' % (data['Name'], data))
        table_entries.append(data)
    return table_entries


def get_gainers(url=None):
    url = url or "https://ca.finance.yahoo.com/screener/predefined/day_gainers?guccounter=1"
    return get_data(url)


def get_losers(url=None):
    url = url or "https://ca.finance.yahoo.com/screener/predefined/day_losers"
    return get_data(url)


def database_store(data: dict, table: str):
    cursor = sqlite3.connect('market.db').cursor()
    LOG.debug('Check that table %s exists.' % table)
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s'" % table)
    if not cursor.fetchone():
        cursor.execute('''CREATE TABLE %s
                          (symbol text, name text, price real, change real, %change text, volume text, 3m_average_vol real, cap text, pe_ratio real)''')
        LOG.debug('Created table %s.' % table)
    # Continue with data insert
