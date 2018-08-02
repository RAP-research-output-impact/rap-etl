"""
Harvest data from InCites API.
"""

import json
import os
import sys

import requests

from lib.utils import get_env

base_url = 'https://incites.thomsonreuters.com/incites-app/'



class InCites(object):

    def __init__(self):
        self.session = None

    def login(self):
        s = requests.Session()
        payload = {'username': get_env('INCITES_USER'), 'password': get_env('INCITES_PASSWORD'), 'IPStatus': 'IPValid'}
        s.post('https://login.incites.thomsonreuters.com/?DestApp=IC2&locale=en_US&Alias=IC2', data=payload)
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
            "take": 200,
            "skip": 0,
            "sortBy": "timesCited",
            "sortOrder": "desc",
            "filters": {
                "orgname": {
                    "is": [org]
                },
                "personIdTypeGroup": {
                    "is": "name"
                },
                "personIdType": {
                    "is": "fullName"
                },
                "schema": {
                    "is": "Web of Science"
                },
                "period": {
                    "is": [start, stop]
                }
            },
            "pinned": [],
            "indicators": [
                "orgName",
                "rank",
                "wosDocuments",
                "timesCited",
                "percentCited",
                "norm",
                "key",
                "seqNumber",
                "hasProfile"
            ]
        }
        data = self._data_request(url, payload)
        return data

    def categories_by_year(self, org, year):
        url = base_url + "explore/0/subject/data/table/page"
        payload = {
            "take": 25,
            "skip": 0,
            "pinned": [],
            "sortBy": "wosDocuments",
            "sortOrder": "desc",
            "indicators": [
                "key",
                "seqNumber",
                "sbjName",
                "rank",
                "wosDocuments",
                "norm",
                "timesCited",
                "percentCited"
            ],
            "filters": {
                "schema": {
                    "is": "Web of Science"
                },
                "sbjname": {
                    "not": ["OVERALL"]
                },
                "assprsnIdTypeGroup": {
                    "is": "name"
                },
                "assprsnIdType": {
                    "is": "fullName"
                },
                "orgname": {
                    "is": [org]
                },
                "personIdTypeGroup": {
                    "is": "name"
                },
                "personIdType": {
                    "is": "fullName"
                }
            }
        }
        return self._data_request(url, payload)


if __name__ == '__main__':
    raise Exception("No main function")
