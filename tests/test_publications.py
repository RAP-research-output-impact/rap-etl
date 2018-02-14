"""
Publications parsing tests
"""

import unittest

from utils import read_file

from publications import WosRecord
import settings


class TestPublications(unittest.TestCase):

    def setUp(self):
        raw = read_file('data/test_rec.xml')
        self.rec = WosRecord(raw)

    def test_funding(self):
        grants = self.rec.grants()
        self.assertEqual(len(grants), 4)
        self.assertEqual(grants[0]["agency"], "Strategic Program for Young Researchers")
        self.assertEqual(grants[0]["ids"][0], "55")

    def test_address(self):
        addrs = self.rec.addresses()
        first_addr = addrs[0]
        self.assertEqual(first_addr['sub_organizations'][0], settings.DEPARTMENT_UNKNOWN_LABEL)

if __name__ == '__main__':
    unittest.main()