## rap-etl

Extracting RAP information from the Web of Science and mapping it to VIVO RDF.

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

### Tests
Initial unit tests are in`tests`. Run as:
```
python -m unittest discover tests
```


### Running a harvest

##### Create a "data release" in `settings.py`
Update the start and end date for the time span you are interested in and increment the version number. It's important to keep these sequential. For demonstration purposes we will use '5' as the release version, with '4' being the previous release.

##### Fetch new WOS publication XML for the data release

`$ python fetch_pubs_xml.py --release 5`

##### Map publication XML to RAP RDF

`$ python tasks.py --release 5`

This script can be run in parallel with [luigi](https://github.com/spotify/luigi). To do so, first start the luigi scheduler server and then run `tasks.py`. The `convert.sh` script handles this for you. Run as:

`$ ./convert 5`

##### Find additions and removals.

We will compare this release RDF against the last data release so that we only post relevant changes to VIVO. You are expected to retain the RDF for at least one prior release on the local filesystem.

`$ python diff_releases.py --release 5`

This script will save RDF files of additions and removals to the `staging` directory specified in `settings.py`, which is set to `data/staging` by default.

##### Post the additions to VIVO

`$ python post_rdf.py --path data/staging/5/add/*.nt`

##### Post the removals/deletes to VIVO

`$ python post_rdf.py --delete --path data/staging/5/delete/*.nt`

The RAP should now be updated with the latest release.

### Updating InCites data

#### Harvest InCites data

Run the InCites harvest using the organizations found above.

`python fetch_incites.py --release 5`

#### Map InCites data to RDF

Map the InCites data to RDF.

`python map_metrices.py --release 5`

##### Post the additions to VIVO

`$ python post_rdf.py --path data/incites/*.nt`

### Building person profiles
Person profiles can be generated from the WOS XML. These profiles use a combination of ResearcherID, ORICD, and DAIS identifiers.

`$ python build_profiles.py --release 5`
