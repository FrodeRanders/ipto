#!/bin/sh
set -eu
repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec mvn -B -ntp -f "$repo_dir/implementations/java/pom.xml" \
  -pl quarkus-app -am -Pweb clean package "$@"
