#!/usr/bin/env bash
# Downloads the Neo4j Connector for Apache Kafka JAR into docker/connect/plugins/
# so the Connect image can be built without relying on Confluent Hub (which fails
# behind proxies and is rate limited).
#
# Usage:
#   bash scripts/fetch_neo4j_connector.sh              # latest release
#   bash scripts/fetch_neo4j_connector.sh 5.2.0        # a specific version
set -euo pipefail

REPO="neo4j/neo4j-kafka-connector"
DEST="docker/connect/plugins"
VERSION="${1:-}"
mkdir -p "$DEST"

echo "==> Resolving release from github.com/$REPO"

if [ -z "$VERSION" ]; then
  API="https://api.github.com/repos/$REPO/releases/latest"
else
  API="https://api.github.com/repos/$REPO/releases/tags/$VERSION"
fi

JSON=$(curl -fsSL "$API" 2>/dev/null || echo "")

if [ -z "$JSON" ] || echo "$JSON" | grep -q '"message".*rate limit'; then
  cat <<'EOF'

  Could not query the GitHub API (rate limit or no network).

  Download the JAR manually instead:

    1. Open  https://github.com/neo4j/neo4j-kafka-connector/releases
    2. From the latest release, download the asset for
       "Neo4j Connector for Apache Kafka" - the self-contained .jar
       (NOT the Confluent .zip package)
    3. Save it into  docker/connect/plugins/
    4. Re-run:  docker compose build connect

EOF
  exit 1
fi

# Pick the plain Apache-Kafka self-contained jar, not the Confluent zip.
URL=$(echo "$JSON" | python3 -c "
import json,sys
d=json.load(sys.stdin)
assets=d.get('assets',[])
jars=[a for a in assets if a['name'].endswith('.jar')]
# prefer names without 'confluent'
pref=[a for a in jars if 'confluent' not in a['name'].lower()] or jars
if not pref:
    sys.exit(1)
a=pref[0]
print(a['browser_download_url'])
print(a['name'], file=sys.stderr)
" 2>/tmp/_asset_name) || {
  echo "  No .jar asset found in that release. Download manually (see above)."
  exit 1
}

NAME=$(cat /tmp/_asset_name)
TAG=$(echo "$JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['tag_name'])")

echo "==> Release : $TAG"
echo "==> Asset   : $NAME"
echo "==> Saving to $DEST/"

rm -f "$DEST"/*.jar
curl -fL --progress-bar -o "$DEST/$NAME" "$URL"

echo
echo "Downloaded:"
ls -lh "$DEST"/*.jar
echo
echo "Next:  docker compose build connect && docker compose up -d"
