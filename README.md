## rap-etl

Extracting RAP information from the Web of Science and mapping it to VIVO RDF.

_This is a work in-progress. More functionality and documentation will be coming._

More details about the project are available [here](https://widgets.figshare.com/articles/5266435/embed?show_title=1).

### Requirements

* Python 2.7
* VIVO SPARQL Update API credentials
* Web of Science Expanded API credentials
* InCites access

To install:
* `$ pip install -r requirements.txt`

Set environment variables:
See example in .env-sample.
* `$ cp .env-sample .env`

Adjust values for VIVO and WOS API and:
* `$ source .env`


### Data sources

* Web of Science Web Services Expanded
* InCites API
* Web of Science categories
* Web of Science organization enhanced names


### Tests
Initial unit tests are in`tests`. Run as:
```
python -m unittest discover tests
```