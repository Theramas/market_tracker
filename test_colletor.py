import logging
import unittest
import sqlite3
import csv
import os

import collector


class TestCollector(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.connection = sqlite3.connect('market.db')
        cls.cursor = cls.connection.cursor()

    @classmethod
    def tearDownClass(cls):
        try:
            cls.cursor.execute("DROP TABLE test")
        except:
            pass
        cls.connection.close()
        os.remove('test.csv')

    def test_web_scrapping_success(self):
        url = "https://ca.finance.yahoo.com/screener/predefined/day_gainers?guccounter=1"
        data = collector.get_data_from_website(url)
        self.assertTrue(data)

    def test_web_scrapping_failure(self):
        url = "foobar"
        with self.assertRaises(Exception):
            collector.get_data_from_website(url)

    def test_storing_data_in_database(self):
        data = [{a[0]: 'test' for a in collector.LABEL_ALIASES.values()}]
        collector.store_in_database(data, 'test')
        self.cursor.execute("SELECT * from test")
        result = self.cursor.fetchall()
        self.assertEqual(
            result, [('test', 'test', 'test', 'test', 'test', 'test', 'test', 'test')])

    def test_get_top(self):
        data_from_function = collector.get_top_choices_from_database('gainers', 'change', 1)[0]
        self.cursor.execute("SELECT name, MAX(change) from gainers")
        data_from_database = self.cursor.fetchone()
        self.assertEqual(data_from_database, data_from_function)

    def tests_write_to_xml(self):
        collector.write_to_xml([[1, 2, 3, 4]], 'test')
        with open('test.csv', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            third_row_text = ' '.join(list(reader)[2])
        self.assertEqual(third_row_text, '1,2,3,4')


if __name__ == '__main__':
    unittest.main()
