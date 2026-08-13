# Runs a mongosh script inside the compose `mongodb` container.
#   ./run_mongo_queries.sh                 # runs mongo_queries.js
#   ./run_mongo_queries.sh other_file.js   # runs something else

set -euo pipefail

cd "$(dirname "$0")"
script_file="${1:-mongo_queries.js}"

if [[ ! -f "$script_file" ]]; then
  echo "No such script: $script_file" >&2
  exit 1
fi

exec docker compose exec -T mongodb \
  mongosh -u mongo -p mongo --authenticationDatabase admin --quiet \
  --eval "$(cat "$script_file")"
