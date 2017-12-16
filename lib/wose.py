"""
Minimal client for query Web of Science Web Services Expanded.
"""
import base64
import math
import os
import re
from string import Template
import xml.etree.ElementTree as ET

import logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("wose-client")

import requests

AUTH_URL = 'http://search.webofknowledge.com/esti/wokmws/ws/WOKMWSAuthenticate?wsdl'
SEARCH_URL = 'http://search.webofknowledge.com/esti/wokmws/ws/WokSearch?wsdl'

NS = {
    'rec': 'http://scientific.thomsonreuters.com/schema/wok5.4/public/FullRecord'
}

# SOAP message for authenticating.
AUTHENTICATE = """
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:auth="http://auth.cxf.wokmws.thomsonreuters.com">
  <soapenv:Header/>
   <soapenv:Body>
    <auth:authenticate/>
 </soapenv:Body>
</soapenv:Envelope>
"""

# SOAP message for closing session.
CLOSE = """
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:auth="http://auth.cxf.wokmws.thomsonreuters.com">
   <soapenv:Header/>
   <soapenv:Body>
      <auth:closeSession/>
   </soapenv:Body>
</soapenv:Envelope>
"""

# Basic query template
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
            <queryLanguage>en</queryLanguage>
         </queryParameters>
         <retrieveParameters>
            <firstRecord>1</firstRecord>
            <count>$count</count>
         </retrieveParameters>
      </woksearch:search>
   </soapenv:Body>
</soapenv:Envelope>
""")


# SOAP message for retrieving prior query
RETRIEVE = Template("""
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
<soap:Body>
  <ns2:retrieve xmlns:ns2="http://woksearch.v3.wokmws.thomsonreuters.com">

    <queryId>$qid</queryId>

    <retrieveParameters>
       <firstRecord>$start</firstRecord>
       <count>50</count>
    </retrieveParameters>

  </ns2:retrieve>
</soap:Body>
</soap:Envelope>
""")


class ExceedsException(Exception):
    def __init__(self,*args,**kwargs):
        Exception.__init__(self,*args,**kwargs)

def get_error_message(raw):
    doc = ET.fromstring(raw)
    msg = doc.find('.//faultstring').text
    if msg.startswith('(IIE0022)'):
        raise ExceedsException(msg)


def get_pages(initial, total, psize=50):
    more_pages = math.ceil(float(total - initial)/ psize)
    for pg in range(int(more_pages)):
        pnum = pg + 1
        if pnum == 1:
            start = initial
        else:
            start = start + psize
        if start > total:
            break
        yield start


class Session(object):

    def __init__(self, user=None, password=None, sid=None):
        self.user = user
        self.password = password
        self.sid = sid

    def wauth_header(self):
        return {"Authorization": "Basic %s" % base64.b64encode("%s:%s" % (self.user, self.password))}

    def set_sid(self, sid):
        self.sid = sid

    def sid_header(self):
        if self.sid is None:
            raise Exception("No session available")
        else:
            return {"Cookie": "SID=\"" + self.sid + "\""}

    def authenticate(self):
        logger.debug("Authenticating with WoS.")
        rsp = requests.post(
            AUTH_URL,
            data=AUTHENTICATE,
            headers=self.wauth_header()
        )
        if rsp.status_code == 500:
            raise Exception("WoS returned 500 error:\n" + rsp.text)
        try:
            self.set_sid(ET.fromstring(rsp.text).find('.//return').text)
        except Exception, e:
            raise Exception("Unable to parse SID:\n" + rsp.text)
        return self.sid

    def close(self):
        rsp = requests.post(AUTH_URL, data=CLOSE, cookies=self.sid_header())
        logger.debug("Closing session. Status code: {}.".format(rsp.status_code))
        if rsp.status_code != 200:
            logger.error(rsp.text)
            raise Exception("Failed to close WoS session")


class Client(object):
    def __init__(self, sid):
        self.sid = sid

    def sid_header(self):
        if self.sid is None:
            raise Exception("No session available")
        else:
            return {"Cookie": "SID=\"" + self.sid + "\""}

    def query(self, query_doc):
        rsp = requests.post(SEARCH_URL, data=query_doc, headers=self.sid_header())
        logger.debug("WOS query:\n {}".format(query_doc))
        logger.debug("Query status code: {}".format(rsp.status_code))
        if rsp.status_code != 200:
            logger.error(rsp.text)
            raise Exception("Query error")
        rsp_doc = ET.fromstring(rsp.text.encode('utf-8', 'ignore'))
        found = rsp_doc.find('.//recordsFound').text
        qid = rsp_doc.find('.//queryId').text
        records = rsp_doc.find('.//records').text
        # strip the namespace to make parsing less verbose
        xml = re.sub(' xmlns="[^"]+"', '', records, count=1)
        return qid, int(found), xml

    def retrieve(self, query_doc):
        rsp = requests.post(SEARCH_URL, data=query_doc, headers=self.sid_header())
        logger.debug("WOS query:\n {}".format(query_doc))
        logger.debug("Query status code: {}".format(rsp.status_code))
        if rsp.status_code == 500:
            try:
                emsg = get_error_message(rsp.text)
            except ExceedsException:
                # No problem here just a deduplication issue.
                return ""
            logger.warning(emsg)
        rsp_doc = ET.fromstring(rsp.text.encode('utf-8', 'ignore'))
        try:
            records = rsp_doc.find('.//records').text
        except AttributeError:
            return ""
        # strip the namespace to make parsing less verbose
        xml = re.sub(' xmlns="[^"]+"', '', records, count=1)
        return xml


def get_recs(raw):
    try:
        return ET.fromstring(raw).findall('.//REC')
    except Exception:
        return []


def query(q, sid, count=100, get_all=False):
    wq = Client(sid)
    fq = QUERY.substitute(query=q, count=count)
    logger.info("QUERY:\n" + fq)
    out_recs = []
    if get_all is False:
        qid, num, records = wq.query(fq)
        out_recs += get_recs(records)
    else:
        qid, num, records = wq.query(fq)
        out_recs += get_recs(records)
        # are there more records to fetch?
        if num > count:
            for start in get_pages(count, num):
                logger.debug("Batch start {}. Batch size {}.".format(start, count))
                # Build retrieve query
                rq = RETRIEVE.substitute(qid=qid, start=start)
                logger.debug(rq)
                recs = wq.retrieve(rq).strip()
                out_recs += get_recs(recs)
    return qid, num, out_recs


def raw_query(q, sid, count=100, get_all=False):
    """
    Use for sending in full SOAP message for a query.
    """
    wq = Client(sid)
    logger.info("QUERY:\n" + q)
    out_recs = []
    if get_all is False:
        qid, num, records = wq.query(q)
        logger.info("Found {} records for search.".format(num))
        return qid, num, get_recs(records)
    else:
        qid, num, records = wq.query(q)
        out_recs += get_recs(records)
        # are there more records to fetch?
        if num > count:
            for start in get_pages(count, num):
                logger.info("Batch start {}. Batch size {}.".format(start, count))
                # Build retrieve query
                rq = RETRIEVE.substitute(qid=qid, start=start)
                logger.debug(rq)
                recs = wq.retrieve(rq).strip()
                tr = get_recs(recs)
                out_recs += tr
    return qid, num, out_recs

def get_dp(rec, path):
    try:
        return rec.find(path).text
    except AttributeError:
        return None


if __name__ == "__main__":
    import sys
    import os

    user = os.environ['WOS_USER']
    password = os.environ['WOS_PASSWORD']

    # Authenticate if no session ID is passed in.
    sid = sys.argv[1]
    if sid.find("auth") > -1:
        wos = Session(user=user, password=password)
        sid = wos.authenticate()
        print sid

    q = sys.argv[2]

    qid, num, records = query(q, sid, count=10)

    for rec in records:
        print rec.find('./UID').text, rec.find('./static_data/summary/titles/title/[@type="item"]').text
