import requests
import logging
import sqlite3
import csv
import smtplib
import os

from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

from bs4 import BeautifulSoup

LOG = logging.getLogger('collector')
LOG_HANDLER = logging.FileHandler('%s/logs/collector.log' % os.getcwd())
LOG_FORMATTER = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
LOG_HANDLER.setFormatter(LOG_FORMATTER)
LOG.addHandler(LOG_HANDLER)
LOG.setLevel(logging.DEBUG)

LABEL_ALIASES = {
    'Name': ('name', 'text'),
    'Price (Intraday)': ('price', 'real'),
    'Change': ('change', 'real'),
    '% Change': ('percent_change', 'text'),
    'Volume': ('volume', 'text'),
    'Avg Vol (3 month)': ('three_month_average_vol', 'real'),
    'Market Cap': ('cap', 'text'),
    'PE Ratio (TTM)': ('pe_ratio', 'real')}


def get_data_from_website(url):
    """Extract data from Soup object and convert into dict"""
    response = requests.get(url)
    LOG.debug(f"Got response from {url}:\n{response}")
    if response.status_code != 200:
        raise Exception(f"Got unexpected response from {url}:\n{response.reason}")
    soup = BeautifulSoup(response.content, features="lxml")

    table_of_contents = list(soup.find('tbody').children)
    LOG.debug(f'Found data in Soup:\n{table_of_contents}.')
    table_entries = []
    for entry in table_of_contents:
        data = {}
        for label in LABEL_ALIASES.keys():
            data[LABEL_ALIASES[label][0]] = entry.find('td', attrs={'aria-label': label}).get_text()
        LOG.debug('Collected data for %s:\n%s' % (data['name'], data))
        table_entries.append(data)
    return table_entries


def store_in_database(data: dict, table_name: str):
    """Store collected web data in local database"""
    connection = sqlite3.connect('market.db')
    cursor = connection.cursor()
    LOG.debug(f'Check that table {table_name} exists.')
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    if not cursor.fetchone():
        table_values = ', '.join([' '.join(v) for v in sorted(LABEL_ALIASES.values())])
        cursor.execute(f"CREATE TABLE {table_name} ({table_values})")
        LOG.debug('Created table %s.' % table_name)
    for entry in data:
        command = "INSERT INTO {table_name} ({keys}) VALUES ({values})".format(
            table_name=table_name,
            keys=', '.join(entry.keys()),
            values=(len(entry)-1) * "?, " + "?"
        )
        cursor.execute(command, list(entry.values()))
    LOG.debug(f'Successfully inserted data into {table_name}:\n{data}')
    connection.commit()
    connection.close()


def collect_data(sources: list = None):
    """Get data from specified URls and store it in local database"""
    sources = sources or [
        ("gainers", "https://ca.finance.yahoo.com/screener/predefined/day_gainers?guccounter=1"),
        ("losers", "https://ca.finance.yahoo.com/screener/predefined/day_losers")
    ]
    for source in sources:
        LOG.debug('Collecting data from %s' % source[1])
        data = get_data_from_website(source[1])
        store_in_database(data=data, table_name=source[0])


def get_top_choices_from_database(table: str, metric: str, amount: int):
    """Extract N amount of top choices from local databse based on specified metric"""
    connection = sqlite3.connect('market.db')
    cursor = connection.cursor()
    if table == 'gainers':
        LOG.debug(f"Attempting to get top {amount} gainers based on {metric}.")
        command = "SELECT name, {metric} FROM {table} ORDER BY {metric} DESC LIMIT {amount}".format(
            metric=metric,
            table=table,
            amount=amount)
    elif table == 'losers':
        LOG.debug(f"Attempting to get top {amount} losers based on {metric}.")
        command = "SELECT name, {metric} FROM {table} ORDER BY {metric} LIMIT {amount}".format(
            metric=metric,
            table=table,
            amount=amount)
    cursor.execute(command)
    data = cursor.fetchall()
    connection.commit()
    connection.close()
    return data


def write_to_xml(data: list, file_name: str):
    """Write data to xml (csv) file."""
    with open('%s.csv' % file_name, 'a') as csv_file:
        LOG.debug(f"Writing data to {file_name}.csv.")
        writer = csv.writer(csv_file)
        writer.writerow(['Gainers', ' ', 'Losers', ' '])
        writer.writerow(['Company Name', 'Change',
                         'Company Name', 'Change'])
        for gainer_name, gainer_change, loser_name, loser_change in data:
            writer.writerow([gainer_name, gainer_change,
                             loser_name, loser_change])


def send_mail(send_from: str, send_to: list, subject: str, text: str, files=[], server="127.0.0.1"):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in files:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)

    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()


def make_report(receiver_email, smtp_server="127.0.0.1"):
    """Makes csv reports for 5 top gainers/losers and emails it to specified address"""
    gainers = get_top_choices_from_database(table='gainers', metric='change', amount=5)
    LOG.debug(f"Top 5 gainers:\n{gainers}")
    losers = get_top_choices_from_database(table='losers', metric='change', amount=5)
    LOG.debug(f"Top 5 losers:\n{losers}")
    data = list(map(lambda x, y: (x[0], x[1], y[0], y[1]), gainers, losers))
    write_to_xml(data, 'report')
    mail_text = "Top 5 gainers of the day:\nCompany Change\n{gainers}\n\nTop 5 losers of the day:\nCompany Change\n{losers}".format(
        gainers=[gainer[0] + ' ' + str(gainer[1]) for gainer in gainers],
        losers=[loser[0] + ' ' + str(loser[1]) for loser in losers])
    send_mail(
        "test@gmail.com",
        receiver_email,
        "Best Gainers/Losers report",
        mail_text,
        files=["report.csv"],
        server=smtp_server)
    LOG.debug("Removing local csv report")
    os.remove('report.csv')
