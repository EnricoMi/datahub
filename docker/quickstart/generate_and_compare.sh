#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

set -euxo pipefail

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
python generate_docker_quickstart.py ../docker-compose.yml ../docker-compose.override.yml temp.quickstart.yml
python generate_docker_quickstart.py ../docker-compose-with-neo4j.yml ../docker-compose-with-neo4j.override.yml temp-with-neo4j.quickstart.yml
python generate_docker_quickstart.py ../docker-compose-with-dgraph.yml ../docker-compose-with-dgraph.override.yml temp-with-dgraph.quickstart.yml

python generate_docker_quickstart.py ../monitoring/docker-compose.monitoring.yml temp.quickstart.monitoring.yml

for flavour in "" "-with-neo4j" "-with-dgraph"
do
  if cmp docker-compose$flavour.quickstart.yml temp$flavour.quickstart.yml; then
    printf "docker-compose$flavour.quickstart.yml is up to date."
  else
    printf "docker-compose$flavour.quickstart.yml is out of date."
    exit 1
  fi
done

if cmp docker-compose.quickstart.monitoring.yml temp.quickstart.monitoring.yml; then
  printf "docker-compose.quickstart.monitoring.yml is up to date."
else
  printf "docker-compose.quickstart.monitoring.yml is out of date."
  exit 1
fi

exit 0
