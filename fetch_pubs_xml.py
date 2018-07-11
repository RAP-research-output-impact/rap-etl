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

from lib import wose

from settings import logger, ORG_NAME, DATA_RELEASE, PUBS_PATH


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


def output_path(base, release):
    return os.path.join(base, "v{}".format(str(release)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch Web of Science Documents')
    parser.add_argument('release', type=int, help="Release number. Needs to be specified in settings.DATA_RELEASE")
    parser.add_argument('--session', '-s', default=None, help="WOS session id")
    parser.add_argument('--out', '-o', default="wos")
    args = parser.parse_args()
    start_stop = []
    query = "OG=({})".format(ORG_NAME)
    logger.info("Query: {}".format(query))
    try:
        rel = DATA_RELEASE[args.release]
    except KeyError:
        raise Exception("Release {} not found. Make sure release is specified in settings.DATA_RELEASE".format(args.release))
    q = prep_qstring(query, count=100, start=rel['start'], end=rel['end'])
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
    op = output_path(PUBS_PATH, args.release)
    outd = make_out_dir(op)
    for rec in records:
        ut = rec.find('./UID').text
        path = get_path(ut, base_path=outd)
        with open(path, 'w') as outfile:
            outfile.write(ET.tostring(rec))


