## rap-etl

Extracting RAP information from the Web of Science and mapping it to VIVO RDF.

Requirements:

* Python 2.7
* VIVO SPARQL Update API credentials
* Web of Science Expanded API credentials

To install:
* `$ pip install -r requirements.txt`

Set environment variables:
See example in .env-sample.
* `$ cp .env-sample .env`

Adjust values for VIVO and WOS API and:
* `$ source .env`

### Loading

#### Step 1 - getting Web of Science publications

*`fetch_pubs.xml`

#### Step 2 - add ontologies to VIVO

* see `ontology` directory. Use the VIVO interface to load these ontology extensions.

#### Step 3 - process full WOS XML

Load 100 sample publications into your VIVO instance. This will start a multi-step process that maps the full XML documents obtained in Step 1 to the VIVO ontology and WOS extensions.

* `$ ./load.sh 100`
