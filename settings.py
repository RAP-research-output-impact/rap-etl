
from namespaces import D
from lib.log_setup import get_logger

logger = get_logger()

# WOS organization enhanced name for the home organization
ORG_NAME = "Technical University of Denmark"

DATA_RELEASE = {
    3: {
        'start': '2007-01-01',
        'end': '2018-09-14'
    },
    2: {
        'start': '2007-01-01',
        'end': '2018-09-14'
    },
    1: {
        'start': '2007-01-01',
        'end': '2018-09-14'
    }
}

RECORD_PATH = 'data/pubs/*/*.xml'
RDF_PATH = 'data/rdf/'
PUBS_PATH = 'data/pubs/'
INCITES_PATH = 'data/incites/'
STAGING_PATH = 'data/staging'

NG_BASE = "http://localhost/data/"

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
CLEAN_SUBORG_GRAPH = "http://localhost/data/clean-suborgs"
CATEGORY_NG = "http://localhost/data/wos-venue-categories"
COUNTRY_CODE_NG = "http://localhost/data/organization-extra"
ADDRESS_COUNTRY_GRAPH = "http://localhost/data/address-country"

# Incites
INCITES_TOP_CATEGORIES = "http://localhost/data/incites-top-categories"
INCITES_PUB_YEAR_COUNTS = "http://localhost/data/incites-pub-year-counts"
INCITES_TOTAL_CITES_YEAR = "http://localhost/data/incites-total-cites-year-counts"

# Label used when no suborganization found in address
DEPARTMENT_UNKNOWN_LABEL = "Department Unknown"


ORCID_FILE = 'dais_to_orcid.json'
RID_FILE = 'dais_to_rid.json'
AU_ID_FILE = 'au_id_to_dais.json'


SEED = 71


# Map WOS country name to VIVO ISO 3 URIs
# Most ISO codes match the name exactly, some need special handling. List those here.
# Missing - Ivory Coast, Reunion, Taiwan
COUNTRY_REPLACE = {
    'belarus':               'Belarus',
    'bermuda':               'Bermuda',
    'bolivia':               'Bolivia',
    'bosnia_herceg':         'Bosnia_and_Herzegovina',
    'bosnia_herzegovina':    'Bosnia_and_Herzegovina',
    'brunei':                'Brunei_Darussalam',
    'byelarus':              'Belarus',
    'czech_republic':        'Czech_Republic_the',
    'costa_rica':            'Costa_Rica',
    'england':               'United_Kingdom_of_Great_Britain_and_Northern_Ireland__the',
    'fr_polynesia':          'French_Polynesia',
    'gambia':                'Gambia__the',
    'iran':                  'Iran_Islamic_Rep_of_',
    'ivory_coast':           'Cote_d_Ivoire',
    'libya':                 'Libyan_Arab_Jamahiriya__the',
    'moldova':               'Republic_of_Moldova',
    'mongol_peo_rep':        'Mongolia',
    'netherlands':           'Netherlands_the',
    'niger':                 'Niger_the',
    'north_ireland':         'United_Kingdom_of_Great_Britain_and_Northern_Ireland__the',
    'papua_n_guinea':        'Papua_New_Guinea',
    'peoples_r_china':       'China',
    'philippines':           'Philippines__the',
    'rep_of_georgia':        'Georgia',
    'republic_of_georgia':   'Georgia',
    'reunion':               'Reunion',
    'russia':                'Russian_Federation__the',
    'scotland':              'United_Kingdom_of_Great_Britain_and_Northern_Ireland__the',
    'south_korea':           'Republic_of_Korea__the',
    'sudan':                 'Sudan_the',
    'tanzania':              'United_Republic_of_Tanzania__the',
    'u_arab_emirates':       'United_Arab_Emirates__the',
    'united_arab_emirates':  'United_Arab_Emirates__the',
    'united_kingdom':        'United_Kingdom_of_Great_Britain_and_Northern_Ireland__the',
    'united_states':         'United_States_of_America',
    'usa':                   'United_States_of_America',
    'vietnam':               'Viet_Nam',
    'wales':                 'United_Kingdom_of_Great_Britain_and_Northern_Ireland__the'
}

# Countries not in VIVO by default but added here.
ADDED_COUNTRIES = {
    'greenland':             D['country-greenland'],
    'macedonia':             D['country-macedonia'],
    'taiwan':                D['country-taiwan']
}
COUNTRY_OVERRIDE = {
    'org-euratom':                                                                   'Belgium',
    'org-general-electric':                                                          'United_States_of_America',
    'org-siemens-ag':                                                                'Germany',
    'org-european-space-agency':                                                     'France',
    'org-european-commission-joint-research-centre':                                 'Belgium',
    'org-nokia-corporation':                                                         'Finland',
    'org-ericsson':                                                                  'Sweden',
    'org-technical-university-of-denmark':                                           'Denmark',
    'org-alcatel-lucent':                                                            'France',
    'org-food-agriculture-organization-of-the-united-nations-fao':                   'Italy',
    'org-world-health-organization':                                                 'Switzerland',
    'org-nxp-semiconductors':                                                        'Netherlands_the',
    'org-bayer-ag':                                                                  'Germany',
    'org-novozymes':                                                                 'Denmark',
    'org-international-business-machines-ibm':                                       'United_States_of_America',
    'org-astrazeneca':                                                               'United_Kingdom_of_Great_Britain_and_Northern_Ireland__the',
    'org-microsoft':                                                                 'United_States_of_America',
    'org-novo-nordisk':                                                              'Denmark',
    'org-agilent-technologies':                                                      'United_States_of_America',
    'org-thermo-fisher-scientific':                                                  'United_States_of_America',
    'org-le-reseau-international-des-instituts-pasteur-riip':                        'France',
    'org-veolia':                                                                    'United_States_of_America',
    'org-huawei-technologies':                                                       'China',
    'org-basf':                                                                      'Germany',
    'org-dsm-nv':                                                                    'Netherlands_the',
    'org-furukawa-electric':                                                         'Japan',
    'org-massachusetts-institute-of-technology-mit':                                 'United_States_of_America',
    'org-roche-holding':                                                             'Switzerland',
    'org-european-molecular-biology-laboratory-embl':                                'Germany',
    'org-pfizer':                                                                    'United_States_of_America',
    'org-pan-american-health-organization':                                          'United_States_of_America',
    'org-boehringer-ingelheim':                                                      'Germany',
    'org-vestas-wind-systems':                                                       'Denmark',
    'org-sanofi-aventis':                                                            'France',
    'org-danone-nutricia':                                                           'France',
    'org-fujitsu-ltd':                                                               'Japan',
    'org-saint-gobain-sa':                                                           'France',
    'org-glaxosmithkline':                                                           'United_Kingdom_of_Great_Britain_and_Northern_Ireland__the',
    'org-intel-corporation':                                                         'United_States_of_America',
    'org-fujitsu-laboratories-ltd':                                                  'Japan',
    'org-unilever':                                                                  'Netherlands_the',
    'org-sigma-aldrich':                                                             'United_States_of_America',
    'org-european-southern-observatory':                                             'Chile',
    'org-novartis':                                                                  'Switzerland',
    'org-syngenta':                                                                  'Switzerland',
    'org-imec':                                                                      'Belgium',
    'org-institut-de-recherche-pour-le-developpement-ird':                           'France',
    'org-national-astronomical-observatory-of-japan':                                'Japan',
    'org-sintef':                                                                    'Norway',
    'org-university-of-queensland':                                                  'Australia',
    'org-bayer-cropscience':                                                         'Germany',
    'org-l-oreal-group':                                                             'France',
    'org-university-of-california-system':                                           'United_States_of_America',
    'org-monash-university':                                                         'Australia',
    'org-kovalevsky-institute-of-marine-biological-research':                        'Russian_Federation__the',
    'org-philips-research':                                                          'Netherlands_the',
    'org-vtt-technical-research-center-finland':                                     'Finland',
    'org-general-motors':                                                            'United_States_of_America',
    'org-procter-gamble':                                                            'United_States_of_America',
    'org-university-of-california-berkeley':                                         'United_States_of_America',
    'org-johnson-johnson':                                                           'United_States_of_America',
    'org-aix-marseille-universite':                                                  'France',
    'org-polish-academy-of-sciences':                                                'Poland',
    'org-denso':                                                                     'Japan',
    'org-toyota-motor-corporation':                                                  'Japan',
    'org-european-academy-of-bozen-bolzano':                                         'Italy',
    'org-thales-group':                                                              'France',
    'org-national-institutes-of-natural-sciences-nins-japan':                        'Japan',
    'org-hanoi-university-of-science-technology':                                    'Viet_Nam',
    'org-korea-institute-of-industrial-technology-kitech':                           'Republic_of_Korea__the',
    'org-akzonobel':                                                                 'Netherlands_the',
    'org-cochlear':                                                                  'Australia',
    'org-exxon-mobil-corporation':                                                   'United_States_of_America',
    'org-national-optical-astronomy-observatory':                                    'United_States_of_America',
    'org-korea-institute-of-science-technology':                                     'Republic_of_Korea__the',
    'org-hasselt-university':                                                        'Belgium',
    'org-greenland-institute-of-natural-resources':                                  'greenland',
    'org-university-of-munich':                                                      'Germany',
    'org-alstom':                                                                    'France',
    'org-international-crops-research-institute-for-the-semi-arid-tropics-icrisat':  'India',
    'org-ec-jrc-institute-for-energy-transport-iet':                                 'Netherlands_the',
    'org-firmenich':                                                                 'Switzerland',
    'org-konkuk-university':                                                         'Republic_of_Korea__the',
    'org-smithsonian-institution':                                                   'United_States_of_America',
    'org-nec-corporation':                                                           'Japan',
    'org-nokia-siemens-networks':                                                    'Finland',
    'org-abb':                                                                       'Switzerland',
    'org-philips':                                                                   'Netherlands_the',
    'org-weill-cornell-medical-college-qatar':                                       'United_States_of_America',
    'org-illumina':                                                                  'United_States_of_America',
    'org-university-of-oxford':                                                      'United_Kingdom_of_Great_Britain_and_Northern_Ireland__the',
    'org-airbus-group':                                                              'Netherlands_the',
    'org-eads-astrium':                                                              'France',
    'org-janssen-pharmaceuticals':                                                   'Belgium',
    'org-carnegie-mellon-university':                                                'United_States_of_America',
    'org-merck-company':                                                             'United_States_of_America',
    'org-medimmune':                                                                 'United_States_of_America',
    'org-eli-lilly':                                                                 'United_States_of_America',
    'org-rand-corporation':                                                          'United_States_of_America',
    'org-university-of-yaounde-i':                                                   'Cameroon',
    'org-university-cheikh-anta-diop-dakar':                                         'Senegal',
    'org-bayer-healthcare-pharmaceuticals':                                          'Germany',
    'org-swedish-university-of-agricultural-sciences':                               'Sweden'
}

# WOS namespace
NS = {
    'rec': 'http://scientific.thomsonreuters.com/schema/wok5.4/public/FullRecord'
}
