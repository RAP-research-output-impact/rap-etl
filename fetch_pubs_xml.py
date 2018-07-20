"""
Fetch documents from WOS webservices expanded and write to XML.

$ python fetch_pubs_xml.py --help
"""


import argparse
import os
from string import Template

import xml.etree.ElementTree as ET

from lib import wose, utils

from settings import logger, ORG_NAME, DATA_RELEASE


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


def fetch(query, start, stop, sid):
    q = prep_qstring(query, count=100, start=start, end=stop)
    logger.info("Fetching publications from WoS")
    logger.info("WOS query: {}".format(q))
    user = os.environ['WOS_USER']
    password = os.environ['WOS_PASSWORD']

    # Authenticate if no session ID is passed in.
    if sid is None:
        wos = wose.Session(user=user, password=password)
        sid = wos.authenticate()
        logger.info("Session ID: {}.".format(sid))

    qid, num, records = wose.raw_query(q, sid, get_all=True)
    logger.info("{} records found.".format(len(records)))
    # Make output dir
    op = utils.get_pubs_base_path(args.release)
    for rec in records:
        ut = rec.find('./UID').text
        path = get_path(ut, base_path=op)
        with open(path, 'w') as outfile:
            outfile.write(ET.tostring(rec))
    return True



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch Web of Science Documents')
    parser.add_argument('--release', '-r', type=int, help="Release number. Needs to be specified in settings.DATA_RELEASE")
    parser.add_argument('--session', '-s', default=None, help="WOS session id")
    #parser.add_argument('--out', '-o', default="wos")
    args = parser.parse_args()
    start_stop = []
    query = "OG=({})".format(ORG_NAME)
    logger.info("Query: {}".format(query))
    try:
        rel = DATA_RELEASE[args.release]
    except KeyError:
        raise Exception("Release {} not found. Make sure release is specified in settings.DATA_RELEASE".format(args.release))

    fetch(query, rel['start'], rel['end'], args.session)


