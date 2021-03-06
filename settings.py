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
PEOPLE_IDENTIFIERS_GRAPH = "http://localhost/data/people-identifiers"
PEOPLE_EMAIL_GRAPH = "http://localhost/data/people-email"
PEOPLE_DTU_DAIS_GRAPH = "http://localhost/data/people-dtu-dais"
AFFILIATION_NG = "http://localhost/data/people-affiliation"
PEOPLE_AUTHORSHIP = "http://localhost/data/people-authorship"
ADDRESS_GRAPH = "http://localhost/data/address"
SUBORG_GRAPH = "http://localhost/data/suborgs"
CLEAN_SUBORG_GRAPH = "'http://localhost/data/clean-suborgs'"
CATEGORY_NG = "http://localhost/data/wos-venue-categories"
COUNTRY_CODE_NG = "http://localhost/data/organization-extra"

# Incites
INCITES_TOP_CATEGORIES = "http://localhost/data/incites-top-categories"
INCITES_PUB_YEAR_COUNTS = "http://localhost/data/incites-pub-year-counts"
INCITES_TOTAL_CITES_YEAR = "http://localhost/data/incites-total-cites-year-counts"

# Format
#Seq #,Title,20 Char,Publisher,Prods,ISSN,E-ISSN,Country,Language,SCIE,SSCI,AHCI,WoS Category
#D2783J,2D Materials,2D MATER,IOP PUBLISHING LTD,D  JS Q  XC S  CC ES,2053-1583,2053-1583,ENGLAND,English,1,0,0,"Materials Science, Multidisciplinary"
CATEGORY_FILE = "data/source_files/wos-categories.csv"

# Label used when no suborganization found in address
DEPARTMENT_UNKNOWN_LABEL = "Department Unknown"


ORCID_FILE = 'data/dais_to_orcid.json'
RID_FILE = 'data/dais_to_rid.json'
AU_ID_FILE = 'data/au_id_to_dais.json'

COUNTRY_CODE_KEY_FILE = 'data/org_country_code_key.json'


SEED = 71
