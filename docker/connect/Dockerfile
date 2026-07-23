# Kafka Connect with the Neo4j connector baked in.
#
# Building the plugin into the image (instead of running `confluent-hub install`
# at container start) removes the single most common failure in this lab: the
# hub install failing silently behind a proxy, leaving Connect running with no
# Neo4j plugin and connectors that refuse to register.
#
# Before building, populate the plugins directory:
#     bash scripts/fetch_neo4j_connector.sh
#
FROM confluentinc/cp-kafka-connect:7.6.1

# Self-contained connector JAR(s), fetched by scripts/fetch_neo4j_connector.sh
COPY plugins/ /usr/share/confluent-hub-components/

USER root
RUN chown -R appuser:appuser /usr/share/confluent-hub-components/ || true
USER appuser

# Fail fast at build time if the plugin directory is empty, rather than at
# connector-registration time with a confusing ClassNotFound error.
RUN ls /usr/share/confluent-hub-components/*.jar > /dev/null 2>&1 \
    || (echo "ERROR: no connector jar found. Run scripts/fetch_neo4j_connector.sh first." && exit 1)
