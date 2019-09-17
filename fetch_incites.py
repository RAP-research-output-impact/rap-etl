"""
Harvest data from InCites API.
"""
import argparse
from collections import defaultdict
import csv
import json
import logging
import multiprocessing
import os
import sys

import requests

import settings
from settings import logger
from lib.utils import get_env, get_incites_base_path, get_incites_output_path
from lib import utils

from map_metrics import get_unified_orgs

base_url = 'https://incites.clarivate.com/incites-app/'
"""
base_url = 'https://incites.thomsonreuters.com/incites-app/'
            https://incites.clarivate.com/incites-app/explore/0/organization/data/table/page
            url = base_url + 'explore/0/organization/data/trend/page'
            url = base_url +  explore/0/subject/data/table/page"
"""


def get_start_stop(release):
    start = int(utils.RELEASE_FROM.split('-')[0])
    stop = int(utils.RELEASE_TO.split('-')[0])
    logger.info("get_start_stop (start: {}, stop: {})".format(start, stop))
    return start, stop


class InCites(object):

    def __init__(self):
        self.session = None

    def login(self):
        s = requests.Session()
        payload = {'username': get_env('INCITES_USER'), 'password': get_env('INCITES_PASSWORD'), 'IPStatus': 'IPValid'}
        s.post('https://login.incites.clarivate.com/?DestApp=IC2&locale=en_US&Autoreload&Alias=IC2', data=payload)
#       s.post('https://login.incites.thomsonreuters.com/?DestApp=IC2&locale=en_US&Autoreload&Alias=IC2', data=payload)
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
        logger.info("docs_citations_by_year (start: {}, stop: {}, url: {})".format(start, stop, url))
        payload = {
            "queryDataCollection": "ESCI",
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
                "norm",
                "prcntDocsIn90",
                "prcntDocsIn99",
                "prcntIndCollab",
                "prcntIntCollab",
                "key",
                "seqNumber"
            ]
        }
        rsp = self._data_request(url, payload)
        items = rsp['items']
        cites = []
        docs = []
        impact = []
        top10 = []
        top1 = []
        collind = []
        collint = []
        for item in items:
            docs.append({"count": item['wosDocuments'], 'year': item['year']})
            cites.append({"count": item['timesCited'], 'year': item['year']})
            impact.append({"count": item['norm'], 'year': item['year']})
            top10.append({"count": item['prcntDocsIn90'], 'year': item['year']})
            top1.append({"count": item['prcntDocsIn99'], 'year': item['year']})
            collind.append({"count": item['prcntIndCollab'], 'year': item['year']})
            collint.append({"count": item['prcntIntCollab'], 'year': item['year']})
        return docs, cites, impact, top10, top1, collind, collint

    def categories_by_year(self, org, start, stop):
        logger.info("categories_by_year (start: {}, stop: {})".format(start, stop))
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
                out[item['sbjName']].append({"count": item['wosDocuments']['value'], 'year': year})

        # Reshape format to match previous format with list of categories and counts.
        rout = []
        for k in out.keys():
            years = out[k]
            rout.append({"category": k, "counts": years})

        return rout


def get_org(args):
    release, ic, oid, name, start, stop = args
    logger.info("get_org (start: {}, stop: {})".format(start, stop))
    logger.info("Processing InCites for release {} and {} {}.".format(release, oid, name))
    cats_out = get_incites_output_path(release, "categories-by-year", oid)
    docs_out = get_incites_output_path(release, 'total', oid)
    cites_out = get_incites_output_path(release, 'cites', oid)
    impact_out = get_incites_output_path(release, 'impact', oid)
    top10_out = get_incites_output_path(release, 'top10', oid)
    top1_out = get_incites_output_path(release, 'top1', oid)
    collind_out = get_incites_output_path(release, 'collind', oid)
    collint_out = get_incites_output_path(release, 'collint', oid)

    if (not os.path.exists(cites_out) or (not os.path.exists(docs_out))):
        docs, cites, impact, top10, top1, collind, collint = ic.docs_citations_by_year(name, start, stop)

        with open(docs_out, "wb") as of:
            json.dump(docs, of)

        with open(cites_out, "wb") as of:
            json.dump(cites, of)

        with open(impact_out, "wb") as of:
            json.dump(impact, of)

        with open(top10_out, "wb") as of:
            json.dump(top10, of)

        with open(top1_out, "wb") as of:
            json.dump(top1, of)

        with open(collind_out, "wb") as of:
            json.dump(collind, of)

        with open(collint_out, "wb") as of:
            json.dump(collint, of)
    else:
        logger.info("Either cites or total file exists for {}. Skipping".format(name))


    if (not os.path.exists(cats_out)):
        cats_by_year = ic.categories_by_year(name, start, stop)
        with open(cats_out, "wb") as of:
            json.dump(cats_by_year, of)
    else:
        logger.info("Categories by year file exists for {}. Skipping.".format(name))

    return True


def main(release):
    start, stop = get_start_stop(release)
    logger.info("main (start: {}, stop: {})".format(start, stop))
    logger.info("Setting up InCites connection.")
    ic = InCites()
    ic.login()
    orgs = [(release, ic, oid, name, start, stop) for oid, name in get_unified_orgs(release)]

    p = multiprocessing.Pool(4)
    p.map(get_org, orgs)

    ic.logout()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch Incites Data')
    parser.add_argument("--release", type=int)
    args = parser.parse_args()
    utils.release(args.release)
    if utils.RELEASE == 0:
        raise Exception("fatal: release not found: {}".format(args.release))
    main(utils.RELEASE)
