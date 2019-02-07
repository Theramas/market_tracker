import requests
import logging
import sqlite3
import csv
import smtplib

from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

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


def get_data_from_website(url):
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


def collect_data():
    sources = [
        ("gainers", "https://ca.finance.yahoo.com/screener/predefined/day_gainers?guccounter=1")
        ("losers", "https://ca.finance.yahoo.com/screener/predefined/day_losers")
    ]
    for source in sources:
        data = get_data_from_website(source[1])
        database_store(data=data, table_name=source[0])


def extract_from_database(table: str, metric: str, amount: int):
    connection = sqlite3.connect('market.db')
    cursor = connection.cursor()
    if table == 'gainers':
        command = "SELECT name, {metric} FROM {table} ORDER BY {metric} DESC LIMIT {amount}".format(
            metric=metric,
            table=table,
            amount=amount)
    elif table == 'losers':
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
    with open('%s.csv' % file_name, 'a') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['Gainers', ' ', 'Losers', ' '])
        writer.writerow(['Company Name', 'Change',
                         'Company Name', 'Change'])
        for gainer_name, gainer_change, loser_name, loser_change in data:
            writer.writerow([gainer_name, gainer_change,
                             loser_name, loser_change])


def send_mail(send_from: str, send_to: list, subject: str, text: str, files=[],
              server="127.0.0.1"):

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
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)

    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()


def make_report():
    gainers = extract_from_database(table='gainers', metric='change', amount=5)
    losers = extract_from_database(table='losers', metric='change', amount=5)
    data = list(map(lambda x, y: (x[0], x[1], y[0], y[1]), gainers, losers))
    write_to_xml(data, 'report')
    mail_text = "Top 5 gainers of the day:\nCompany Change\n{gainers}\n\nTop 5 losers of the day:\nCompany Change\n{losers}".format(
        gainers=[gainer[0] + ' ' + str(gainer[1]) for gainer in gainers],
        losers=[loser[0] + ' ' + str(loser[1]) for loser in losers])
    send_mail("test@gmail.com", "nkalmykov13@gmail.com", "Best Gainers/Losers report", mail_text, files=["report.csv"])
    os.remove('report.csv')
