from log_setup import get_logger

logger = get_logger()

# WOS namespace
NS = {
    'rec': 'http://scientific.thomsonreuters.com/schema/wok5.4/public/FullRecord'
}

RECORD_PATH = 'data/pubs/*.xml'
CACHE_PATH = 'data/rdf/'

PUB_GRAPH = "http://localhost/data/pubs"
CATEGORY_GRAPH = "http://localhost/data/wos-categories"
KEYWORDS_PLUS_GRAPH = "http://localhost/data/wos-keywords-plus"

# For sampling pubs during development
SEED = 71