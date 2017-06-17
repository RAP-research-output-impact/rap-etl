from log_setup import get_logger

logger = get_logger()

# WOS namespace
NS = {
    'rec': 'http://scientific.thomsonreuters.com/schema/wok5.4/public/FullRecord'
}

RECORD_PATH = 'data/pubs/*/*.xml'
CACHE_PATH = 'data/rdf/'

PUB_GRAPH = "http://localhost/data/pubs"
CATEGORY_GRAPH = "http://localhost/data/wos-categories"
KEYWORDS_PLUS_GRAPH = "http://localhost/data/wos-keywords-plus"
PEOPLE_GRAPH = "http://localhost/data/people"
AFFILIATION_NG = "http://localhost/data/people-affiliation"


ORCID_FILE = 'data/dais_to_orcid.json'
RID_FILE = 'data/dais_to_rid.json'
AU_ID_FILE = 'data/au_id_to_dais.json'


SEED = 71