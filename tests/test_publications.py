
import os
import unittest

from utils import read_file

from publications import WosRecord


class TestPublications(unittest.TestCase):

    def setUp(self):
        raw = read_file('data/test_rec.xml')
        self.rec = WosRecord(raw)

    def test_funding(self):
        grants = self.rec.grants()
        self.assertEqual(len(grants), 4)
        self.assertEqual(grants[0]["agency"], "Strategic Program for Young Researchers")
        self.assertEqual(grants[0]["ids"][0], "55")

if __name__ == '__main__':
    unittest.main()