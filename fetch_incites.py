"""
Harvest data from InCites API.
"""

from collections import defaultdict
import csv
import json
import logging
import os
import sys

import requests

import settings
from settings import logger
from lib.utils import get_env, get_incites_base_path, get_incites_output_path

base_url = 'https://incites.thomsonreuters.com/incites-app/'


def get_start_stop(release):
    d = settings.DATA_RELEASE[release]
    start = int(d['start'].split('-')[0])
    stop = int(d['end'].split('-')[0])
    return start, stop


class InCites(object):

    def __init__(self):
        self.session = None

    def login(self):
        s = requests.Session()
        payload = {'username': get_env('INCITES_USER'), 'password': get_env('INCITES_PASSWORD'), 'IPStatus': 'IPValid'}
        s.post('https://login.incites.thomsonreuters.com/?DestApp=IC2&locale=en_US&A%autoreload 2lias=IC2', data=payload)
        self.session = s

    def logout(self):
        self.session.get('https://incites.thomsonreuters.com/incites-app/redirect/signout?lc=en')
        return True

    def _data_request(self, url, payload):
        self.session.headers.update({'Content-Type': 'application/json'})
        rsp = self.session.post(url, data=json.dumps(payload))
        if rsp.status_code != 200:
            raise Exception("Status code not 200:\n" + rsp.text)
        return rsp.json()

    def docs_citations_by_year(self, org, start, stop):
        url = base_url + 'explore/0/organization/data/trend/page'
        payload = {
            "take": 500,
            "skip": 0,
            "sortBy": "timesCited",
            "sortOrder": "desc",
            "filters": {
                "orgname": {
                    "is": [org]
                },
                "period": {
                    "is": [start, stop]
                }
            },
            "pinned": [],
            "indicators": [
                "wosDocuments",
                "timesCited",
                "key",
                "seqNumber"
            ]
        }
        rsp = self._data_request(url, payload)
        items = rsp['items']
        cites = []
        docs = []
        for item in items:
            docs.append({"count": item['wosDocuments'], 'year': item['year']})
            cites.append({"count": item['timesCited'], 'year': item['year']})
        return docs, cites

    def categories_by_year(self, org, start, stop):
        out = defaultdict(list)
        url = base_url + "explore/0/subject/data/table/page"
        for year in range(start, stop + 1):
            payload = {
                "take": 500,
                "skip": 0,
                "pinned": [],
                "sortBy": "wosDocuments",
                "indicators": [
                    "key",
                    "sbjName",
                    "wosDocuments"
                ],
                "filters": {
                    "schema": {
                        "is": "Web of Science"
                    },
                    "sbjname": {
                        "not": ["OVERALL"]
                    },
                    "orgname": {
                        "is": [org]
                    },
                    "period": {
                        "is": [year, year]
                    }
                }
            }
            rsp = self._data_request(url, payload)
            items = rsp['items']
            for item in items:
                out[item['sbjName']].append([{"count": item['wosDocuments']['value'], 'year': year}])

        # Reshape format to match previous format with list of categories and counts.
        rout = []
        for k in out.keys():
            years = out[k]
            rout.append({"category": k, "counts": years})

        return rout


def read_orgs(org_file):
    out = []
    with open(org_file) as ogf:
        for n, row in enumerate(csv.DictReader(ogf, delimiter=",")):
            name = row['name']
            oid = row['rap_id']
            out.append((oid, name))
            if n >= 100:
                break
    return out


def get_org(args):
    release, ic, oid, name, start, stop = args
    logger.info("Processing InCites for release v{} and {} {}.".format(release, oid, name))
    cats_out = get_incites_output_path(release, "categories-by-year", oid)
    docs_out = get_incites_output_path(release, 'total', oid)
    cites_out = get_incites_output_path(release, 'cites', oid)

    if (not os.path.exists(cites_out) or (not os.path.exists(docs_out))):
        docs, cites = ic.docs_citations_by_year(name, start, stop)

        with open(docs_out, "wb") as of:
            json.dump(docs, of)

        with open(cites_out, "wb") as of:
            json.dump(cites, of)
    else:
        logger.info("Either cites or total file exists for {}. Skipping".format(name))


    if (not os.path.exists(cats_out)):
        cats_by_year = ic.categories_by_year(name, start, stop)
        with open(cats_out, "wb") as of:
            json.dump(cats_by_year, of)
    else:
        logger.info("Categories by year file exists for {}. Skipping.".format(name))

    return True


def main(input_file):
    import multiprocessing

    release = 2
    start, stop = get_start_stop(release)
    logger.info("Setting up InCites connection.")
    ic = InCites()
    ic.login()
    orgs = [(release, ic, oid, name, start, stop) for oid, name in read_orgs(input_file)]

    p = multiprocessing.Pool(4)
    p.map(get_org, orgs)

    ic.logout()


if __name__ == '__main__':
    #raise Exception("No main function")
    main(sys.argv[1])
