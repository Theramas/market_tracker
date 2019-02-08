import logging
import unittest
import sqlite3
from unittest.mock import MagicMock

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


if __name__ == '__main__':
    unittest.main()
