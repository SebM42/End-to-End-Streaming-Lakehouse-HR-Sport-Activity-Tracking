#!/bin/bash
set -e

# Créer la DB si elle n'existe pas
psql -v ON_ERROR_STOP=1 --username ${POSTGRES_USER:-postgres} --dbname ${POSTGRES_DB} <<-EOSQL
  CREATE DATABASE rh;
EOSQL