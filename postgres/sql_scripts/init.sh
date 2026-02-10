#!/bin/bash
set -e

# Variables
DB_HOST=postgres-sport-poc
DB_USER="${USER}"

export PGPASSWORD="${PW}"

# Attente que la DB RH soit accessible
until psql -h $DB_HOST -U $DB_USER -d rh -c '\q' 2>/dev/null; do
  echo "Attente de la base RH sur $DB_HOST..."
  sleep 2
done

# Exécuter les scripts SQL sur sport_poc
echo "Initalisation base sport_poc ..."
for f in /scripts/sport_poc/*.sql; do
    psql -h $DB_HOST -U $DB_USER -d sport_poc -f "$f"
done
echo "Initalisation sport_poc OK !"

# Exécuter les scripts SQL sur RH
echo "Initalisation base rh ..."
for f in /scripts/rh/*.sql; do
    psql -h $DB_HOST -U $DB_USER -d rh -f "$f"
done
echo "Initalisation rh OK !"

# Créer les connecteurs debezium
echo "Initalisation des connecteurs debezium ..."
DEBEZIUM=http://debezium:8083

# Fonction pour attendre qu'un connecteur soit RUNNING
wait_for_connector() {
  NAME=$1
  echo "Attente du connecteur $NAME..."
  until curl -s $DEBEZIUM/connectors/$NAME/status | grep -q '"state":"RUNNING"'; do
    sleep 2
  done
  echo "$NAME est RUNNING ✅"
}

# Fonction pour créer un connecteur si il n'existe pas
create_connector() {
  NAME=$1
  JSON_PAYLOAD=$2

  if curl -s $DEBEZIUM/connectors/$NAME | grep -q '"name"'; then
    echo "Connecteur $NAME déjà existant, skip"
  else
    echo "Création du connecteur $NAME..."
    curl -s -X POST $DEBEZIUM/connectors \
      -H "Content-Type: application/json" \
      -d "$JSON_PAYLOAD"
    wait_for_connector $NAME
  fi
}

# Liste de connecteurs à créer
create_connector "postgres-sport-connector" "$(cat <<EOF
{
  "name": "postgres-sport-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "${DB_USER}",
    "database.password": "${PGPASSWORD}",
    "database.dbname": "sport_poc",
    "topic.prefix": "pg_sport",
    "table.include.list": "public.evenements_sport",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_sport"
  }
}
EOF
)"

create_connector "postgres-rh-connector" "$(cat <<EOF
{
  "name": "postgres-rh-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "${DB_USER}",
    "database.password": "${PGPASSWORD}",
    "database.dbname": "rh",
    "topic.prefix": "pg_rh",
    "table.include.list": "public.salaries",
    "plugin.name": "pgoutput",
	"snapshot.mode": "initial",
    "slot.name": "debezium_rh"
  }
}
EOF
)"

echo "Initalisation des connecteurs debezium OK !"

echo "Initialisation terminée !"




