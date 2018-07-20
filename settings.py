from lib.log_setup import get_logger

logger = get_logger()

# WOS organization enhanced name for the home organization
ORG_NAME = "Technical University of Denmark"

DATA_RELEASE = {
    3: {
        'start': '2018-01-01',
        'end': '2018-03-30'
    },
    2: {
        'start': '2018-01-14',
        'end': '2018-02-14'
    },
    1: {
        'start': '2018-01-01',
        'end': '2018-01-31'
    }
}


RECORD_PATH = 'data/pubs/*/*.xml'
RDF_PATH = 'data/rdf/'
PUBS_PATH = 'data/pubs/'
STAGING_PATH = 'data/staging'

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

# Label used when no suborganization found in address
DEPARTMENT_UNKNOWN_LABEL = "Department Unknown"


ORCID_FILE = 'data/dais_to_orcid.json'
RID_FILE = 'data/dais_to_rid.json'
AU_ID_FILE = 'data/au_id_to_dais.json'


SEED = 71


# Map WOS country name to VIVO ISO 3 URIs
# Most ISO codes match the name exactly, some need special handling. List those here.
# Missing - Ivory Coast, Reunion, Taiwan
COUNTRY_REPLACE = {
    'Bosnia & Herzegovina': 'Bosnia_and_Herzegovina',
    'Czech Republic': 'Czech_Republic_the',
    'Costa Rica': 'Costa_Rica',
    'Gambia': 'Gambia__the',
    'Iran': 'Iran_Islamic_Rep_of_',
    'Libya': 'Libyan_Arab_Jamahiriya__the',
    'Netherlands': 'Netherlands_the',
    'Peoples R China': 'China',
    'Philippines': 'Philippines__the',
    'Republic of Georgia': 'Georgia',
    'Russia': 'Russian_Federation__the',
    'South Korea': 'Republic_of_Korea__the',
    'Sudan': 'Sudan_the',
    'Tanzania': 'United_Republic_of_Tanzania__the',
    'United Arab Emirates': 'United_Arab_Emirates__the',
    'United Kingdom': 'United_Kingdom_of_Great_Britain_and_Northern_Ireland__the',
    'United States': 'United_States_of_America',
    'Vietnam': 'Viet_Nam'
}


# WOS namespace
NS = {
    'rec': 'http://scientific.thomsonreuters.com/schema/wok5.4/public/FullRecord'
}
