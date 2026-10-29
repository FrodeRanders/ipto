#!/bin/sh
set -eu
repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
app_dir="$repo_dir/implementations/java/quarkus-app/target/quarkus-app"
if [ ! -d "$app_dir/lib/main" ]; then
  echo "Build the application first with ./build.sh -DskipTests" >&2
  exit 1
fi
exec java -cp "$app_dir/app/*:$app_dir/lib/main/*" org.gautelis.ipto.security.PasswordHash
