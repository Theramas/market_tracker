import requests
import logging
import sqlite3
import csv
from bs4 import BeautifulSoup

LOG = logging.getLogger()

LABEL_ALIASES = {
    'Name': ('name', 'text'),
    'Price (Intraday)': ('price', 'real'),
    'Change': ('change', 'real'),
    '% Change': ('percent_change', 'text'),
    'Volume': ('volume', 'text'),
    'Avg Vol (3 month)': ('three_month_average_vol', 'real'),
    'Market Cap': ('cap', 'text'),
    'PE Ratio (TTM)': ('pe_ratio', 'real')}


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
    for entry in table_of_contents:
        data = {}
        for label in LABEL_ALIASES.keys():
            data[LABEL_ALIASES[label][0]] = entry.find('td', attrs={'aria-label': label}).get_text()
        LOG.debug('Collected data for %s:\n%s' % (data['name'], data))
        table_entries.append(data)
    return table_entries


def get_gainers(url=None):
    url = url or "https://ca.finance.yahoo.com/screener/predefined/day_gainers?guccounter=1"
    data = get_data(url)
    database_store(data=data, table_name='gainers')


def get_losers(url=None):
    url = url or "https://ca.finance.yahoo.com/screener/predefined/day_losers"
    data = get_data(url)
    database_store(data=data, table_name='losers')


def database_store(data: dict, table_name: str):
    connection = sqlite3.connect('market.db')
    cursor = connection.cursor()
    LOG.debug('Check that table %s exists.' % table_name)
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s'" % table_name)
    if not cursor.fetchone():
        table_values = ', '.join([' '.join(v) for v in sorted(LABEL_ALIASES.values())])
        cursor.execute("CREATE TABLE %s (%s)" % (table_name, table_values))
        LOG.debug('Created table %s.' % table_name)
    for entry in data:
        command = "INSERT INTO {table_name} ({keys}) VALUES ({values})".format(
            table_name=table_name,
            keys=', '.join(entry.keys()),
            values=(len(entry)-1) * "?, " + "?"
        )
        cursor.execute(command, list(entry.values()))
    connection.commit()
    connection.close()


def save_to_xml():
    connection = sqlite3.connect('market.db')
    cursor = connection.cursor()
    cursor.execute("SELECT name, price, change from gainers")
    gainers = cursor.fetchall()
    cursor.execute("SELECT name, price, change from losers")
    losers = cursor.fetchall()
    connection.commit()
    connection.close()
    with open('gainers.csv', 'a') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['Company Name', 'Price', 'Change'])
        for name, price, change in gainers:
            writer.writerow([name, price, change])
    with open('losers.csv', 'a') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['Company Name', 'Price', 'Change'])
        for name, price, change in losers:
            writer.writerow([name, price, change])

