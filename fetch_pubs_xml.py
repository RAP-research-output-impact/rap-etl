"""
Fetch documents from WOS webservices expanded and write to XML.

$ python fetch_pubs_xml.py --help
"""


import argparse
import sys
import os
from string import Template
import time

import xml.etree.ElementTree as ET

import json

from namespaces import (
	TMP,
	D,
	WOS,
	BIBO
)

from lib import wose

from log_setup import get_logger

logger = get_logger()


def ln(uri):
	return uri.toPython().split('/')[-1]


def make_out_dir(path):
    rp = os.path.realpath(path)
    if os.path.exists(rp) is not True:
        os.mkdir(rp)
    return rp


QUERY = Template("""
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
   xmlns:woksearch="http://woksearch.v3.wokmws.thomsonreuters.com"
   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
   <soapenv:Header/>
   <soapenv:Body>
      <woksearch:search>
         <queryParameters>
            <databaseId>WOS</databaseId>
            <userQuery>$query</userQuery>
            TSPAN
            <queryLanguage>en</queryLanguage>
         </queryParameters>
         <retrieveParameters>
            <firstRecord>$first</firstRecord>
            <count>$count</count>
         </retrieveParameters>
      </woksearch:search>
   </soapenv:Body>
</soapenv:Envelope>
""")

TSPAN = Template("""
    <timeSpan>
        <begin>$start</begin>
        <end>$end</end>
    </timeSpan>
""")


def prep_qstring(query, first=1, count=25, start=None, end=None):
    q = QUERY.substitute(query=query, first=first, count=count)
    if start is None:
        return q.replace("TSPAN", "")
    else:
        if end is None:
            end = date.today().isoformat()
        tspan = TSPAN.substitute(start=start, end=end)
        return q.replace("TSPAN", tspan)


def get_path(ut, base_path="/tmp/wose2/"):
    ut = ut.lstrip("WOS:")
    num = ut.lstrip("0")[:2]
    path = os.path.join(base_path, num)
    if not os.path.exists(path):
        os.makedirs(path)
    fn = os.path.join(path, ut + ".xml")
    return fn

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch Web of Science Documents')
    parser.add_argument('--session', '-s', default=None, help="WOS session id")
    parser.add_argument('--start', default=None, required=True, help="Date start")
    parser.add_argument('--end', default=None, required=True, help="Date end")
    parser.add_argument('--query', '-q', required=True)
    parser.add_argument('--out', '-o', default="wos")
    args = parser.parse_args(sys.argv[1:])
    start_stop = []
    logger.info("Query: {}".format(args.query))
    #query = "OG=(Technical University of Denmark)"
    q = prep_qstring(args.query, count=100, start=args.start, end=args.end)
    logger.info("Fetching publications from WoS")
    logger.info("WOS query: {}".format(q))
    user = os.environ['WOS_USER']
    password = os.environ['WOS_PASSWORD']
    # Authenticate if no session ID is passed in.
    sid = args.session
    if sid is None:
        wos = wose.Session(user=user, password=password)
        sid = wos.authenticate()
        logger.info("Session ID: {}.".format(sid))
    qid, num, records = wose.raw_query(q, sid, get_all=True)
    logger.info("{} records found.".format(len(records)))
    # Make output dir
    outd = make_out_dir(args.out)
    for rec in records:
        ut = rec.find('./UID').text
        path = get_path(ut, base_path=outd)
        with open(path, 'w') as outfile:
            outfile.write(ET.tostring(rec))


