#!/bin/bash

set -e

export PYTHONWARNINGS="ignore"

#
# Script to run luigi with central scheduler and map retrieved
# XML to RAP RDF
#

pid=data/tmp/luigi.pid
luigid --background --pidfile ${pid} --logdir data/tmp/luigi.log --state-path data/tmp/state.ld
python tasks.py --scheduler --release $1
kill `head -n 1 ${pid}`


