# -*- coding: utf-8 -*-
"""
Created on Sat Jan 10 11:40:18 2026

"""

import great_expectations as ge
from prometheus_client import start_http_server, Gauge
import json
import time
import random
import requests
import traceback
import os
import pandas as pd
from datetime import datetime

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from kafka import KafkaConsumer
from deltalake import write_deltalake, DeltaTable
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import int32, timestamp

import osmnx as ox
import networkx as nx
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from geopy.distance import geodesic

# =========================================================
# 0. Templates message slack
# =========================================================
INTERJECTIONS = [
    "Bravo ",
    "Super",
    "Bien joué",
    "Magnifique",
    "Quel effort",
    "Belle perf",
    "Impressionnant",
    "Ça rigole pas",
    "Chapeau"
]

TEMPLATES_BY_SPORT = {
    "Tennis": {
        "constats": [
            "tu viens de jouer au tennis pendant {duree_formate} ",
            "match de tennis terminé en {duree_formate} ",
            "session de tennis bouclée en {duree_formate} "
        ],
        "encouragements": [
            "Quelle énergie",
            "Belle intensité",
            "Continue comme ça",
            "Ça envoie du lourd"
        ],
        "emojis": ["🎾", "🔥", "💪"]
    },

    "Badminton": {
        "constats": [
            "tu viens de taper le volant pendant {duree_formate} ",
            "session de badminton terminée en {duree_formate} ",
            "match de badminton bouclé en {duree_formate} "
        ],
        "encouragements": [
            "Quel rythme",
            "Super vivacité",
            "Beau jeu",
            "Ça fuse"
        ],
        "emojis": ["🏸", "⚡", "🔥"]
    },

    "Escalade": {
        "constats": [
            "session d’escalade terminée en {duree_formate} ",
            "tu viens de grimper pendant {duree_formate} ",
            "enchaînement de voies en {duree_formate} "
        ],
        "encouragements": [
            "Belle maîtrise",
            "Quel mental",
            "Force et technique",
            "Bien accroché"
        ],
        "emojis": ["🧗‍♂️", "💪", "🔥"]
    },

    "Randonnée": {
        "constats": [
            "randonnée de {distance_km} km terminée en {duree_formate} ",
            "tu viens de parcourir {distance_km} km en randonnée ",
            "{distance_km} km de marche bouclés en {duree_formate} "
        ],
        "encouragements": [
            "Quelle aventure",
            "De beaux paysages à la clé",
            "Encore une belle exploration",
            "La nature te réussit"
        ],
        "emojis": ["🌄", "🥾", "🏕️"]
    },

    "Triathlon": {
        "constats": [
            "séance de triathlon complétée en {duree_formate} ",
            "enchaînement natation, vélo et course terminé ",
            "gros effort de triathlon sur {duree_formate} "
        ],
        "encouragements": [
            "Quel défi",
            "Performance impressionnante",
            "Endurance au top",
            "Respect"
        ],
        "emojis": ["🏊‍♂️", "🚴‍♂️", "🏃‍♂️"]
    },

    "Runing": {
        "constats": [
            "tu viens de courir {distance_km} km en {duree_formate} ",
            "{distance_km} km avalés en {duree_formate} ",
            "sortie running de {distance_km} km terminée "
        ],
        "encouragements": [
            "Quelle foulée",
            "Tu es en feu",
            "Ça déroule",
            "Belle perf"
        ],
        "emojis": ["🏃‍♂️", "🔥", "🏅"]
    },

    "Équitation": {
        "constats": [
            "séance d’équitation de {duree_formate} terminée ",
            "moment à cheval pendant {duree_formate} ",
            "travail équestre complété en {duree_formate} "
        ],
        "encouragements": [
            "Belle complicité",
            "Quel équilibre",
            "Harmonie parfaite",
            "Super connexion"
        ],
        "emojis": ["🐎", "✨", "💫"]
    },

    "Voile": {
        "constats": [
            "sortie voile de {duree_formate} terminée ",
            "navigation complétée en {duree_formate} ",
            "session en mer de {duree_formate} "
        ],
        "encouragements": [
            "Bon vent",
            "Belle maîtrise",
            "Cap parfaitement tenu",
            "Ça glisse"
        ],
        "emojis": ["⛵", "🌊", "💨"]
    },

    "Tennis de table": {
        "constats": [
            "match de ping terminé en {duree_formate} ",
            "session de tennis de table bouclée ",
            "échanges rapides pendant {duree_formate} "
        ],
        "encouragements": [
            "Quel réflexe",
            "Ça va très vite",
            "Beau toucher",
            "Top spin assuré"
        ],
        "emojis": ["🏓", "⚡", "🔥"]
    },

    "Football": {
        "constats": [
            "match de foot joué pendant {duree_formate} ",
            "session football terminée ",
            "tu viens de fouler le terrain pendant {duree_formate} "
        ],
        "encouragements": [
            "Quel collectif",
            "Belle intensité",
            "Ça joue bien",
            "Match solide"
        ],
        "emojis": ["⚽", "🔥", "💪"]
    },

    "Natation": {
        "constats": [
            "séance de natation de {duree_formate} terminée",
            "longueurs enchaînées pendant {duree_formate} ",
            "session piscine complétée"
        ],
        "encouragements": [
            "Belle glisse",
            "Respiration maîtrisée",
            "Ça coule tout seul",
            "Super endurance"
        ],
        "emojis": ["🏊‍♂️", "💦", "🔥"]
    },

    "Judo": {
        "constats": [
            "entraînement de judo de {duree_formate} terminé",
            "séance sur le tatami complétée ",
            "combat et technique pendant {duree_formate} "
        ],
        "encouragements": [
            "Quel mental",
            "Force et respect",
            "Belle maîtrise",
            "Esprit du judo"
        ],
        "emojis": ["🥋", "💪", "🔥"]
    },

    "Basketball": {
        "constats": [
            "match de basket joué pendant {duree_formate} ",
            "session basketball terminée",
            "enchaînement de paniers pendant {duree_formate} "
        ],
        "encouragements": [
            "Belle adresse",
            "Ça score",
            "Quel rythme",
            "Jeu fluide"
        ],
        "emojis": ["🏀", "🔥", "💪"]
    },

    "Rugby": {
        "constats": [
            "match de rugby disputé pendant {duree_formate} ",
            "session rugby complétée",
            "gros combat sur le terrain pendant {duree_formate} "
        ],
        "encouragements": [
            "Quel engagement",
            "Solide et collectif",
            "Respect",
            "Ça plaque fort"
        ],
        "emojis": ["🏉", "💪", "🔥"]
    },

    "Boxe": {
        "constats": [
            "séance de boxe de {duree_formate} terminée",
            "enchaînements de coups pendant {duree_formate} ",
            "entraînement boxing complété"
        ],
        "encouragements": [
            "Quel cardio",
            "Belle explosivité",
            "Ça cogne fort",
            "Mental d’acier"
        ],
        "emojis": ["🥊", "🔥", "💪"]
    }
}

# =========================================================
# 1. Configuration
# =========================================================

with open("/config.json") as f:
    config = json.load(f)

entreprises = {e["id"]: e["adresse"] for e in config["entreprises"]}
rules = config["parametres_regles"]

BRONZE_PATH = "/datalake/bronze/rh_salaries"
ELIG_ANNUELLE_PATH = "/datalake/silver/eligibilite_annuelle"
INCOH_PATH = "/datalake/silver/incoherences_distances"
ELIG_FIN_ANNEE_PATH = "/datalake/gold/eligibilite_fin_annee_projetee_prime_brut"

SLACK_ERROR_PATH = "/datalake/silver/slack_errors"
GOLD_PIPE_ERROR_PATH  = "/datalake/silver/gold_errors"
ELIG_JOURNEE_BIEN_ETRE_PATH = "/datalake/gold/eligibilite_journee_bien_etre_par_an"

ERRORS_GE_PATH = "/datalake/ge/errors"

ox.settings.use_cache = True
ox.settings.cache_folder = "/tmp/osmnx_cache"
ox.settings.log_console = True

# POSTGRES
conn_params = {
    "host": "postgres-sport-poc",
    "port": 5432,
    "user": "admin",
    "password": "admin_sport"
}

# SLACK
SLACK_BOT_TOKEN = "xoxb-10223653826839-10267398545776-hQNGAPwibN0B3b13xb2nndgH"
CHANNEL_ID = "C0A7VC47BL0"

client = WebClient(token=SLACK_BOT_TOKEN)

# GREAT EXPECTATIONS
ge_results_sport = []
ge_results_rh = []

# EXPOSITION POWER BI
parquet_path = "/datalake/parquet"
os.makedirs(parquet_path, exist_ok=True)

# METRICS
PIPELINE_ALIVE = Gauge(
    "consumer_alive",
    "Consumer heartbeat (1 = alive)"
)

# =========================================================
# 2. Kafka Consumer
# =========================================================
BATCH_SIZE = 200  # nombre max de messages par batch

consumer_rh = KafkaConsumer(
    "pg_rh.public.salaries",
    bootstrap_servers="redpanda:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="silver_pipeline_rh_group",
    max_poll_records=BATCH_SIZE,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v is not None else None
)

consumer_rh.subscribe(["pg_rh.public.salaries"])

consumer_sport = KafkaConsumer(
    "pg_sport.public.evenements_sport",
    bootstrap_servers="redpanda:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="silver_pipeline_sport_group",
    max_poll_records=BATCH_SIZE,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v is not None else None
)

# =========================================================
# 3. Géolocalisation & graph OSMNX (chargés UNE fois)
# =========================================================

def geocode_ban(adresse):
    """Retourne (lat, lon) pour une adresse française via l'API BAN"""
    url = "https://api-adresse.data.gouv.fr/search/"
    params = {"q": adresse, "limit": 1}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features")
        if features:
            lon, lat = features[0]["geometry"]["coordinates"]
            return lat, lon, None
        return None, None, 'Erreur : features empty'
    except Exception as e:
        return None, None, f"Erreur géocodage BAN: {e}"

print("Geoloc and graphs OSMNX initialization...")

#geolocator = Nominatim(user_agent="rh_silver_worker", timeout=10)

office_address = entreprises.get(0)

lat_office, lon_office, error_office = geocode_ban(office_address)

if error_office is not None:
    print("CRITICAL ERROR : adresse de l'entreprise non reconnue ; raison : {error_office}")

center_point = (lat_office, lon_office)

graph_drive_file = "/tmp/G_drive.graphml"
graph_walk_file = "/tmp/G_walk.graphml"

if os.path.exists(graph_drive_file):
    G_drive = ox.load_graphml(graph_drive_file)
else:
    G_drive = ox.graph_from_point(center_point,
                                  dist=rules["velo_trottinette_distance_max_m"],
                                  network_type="drive")
    ox.save_graphml(G_drive, graph_drive_file)

if os.path.exists(graph_walk_file):
    G_walk = ox.load_graphml(graph_walk_file)
else:
    G_walk = ox.graph_from_point(center_point,
                                 dist=rules["marche_running_distance_max_m"],
                                 network_type="walk")
    ox.save_graphml(G_walk, graph_walk_file)

print("Graphs OSMNX ready")


# =========================================================
# 4. Fonctions
# =========================================================

def send_slack_message(message: str):
    try:
        response = client.chat_postMessage(
            channel=CHANNEL_ID,
            text=message
        )
        return response
    except SlackApiError as e:
        print(f"Erreur Slack: {e.response['error']}")
        return e.response
    

def distance_lineaire(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).meters


def distance_trajet(lat_s, lon_s, mode):
    if mode == "walk":
        max_dist = rules["marche_running_distance_max_m"]
        G = G_walk
    else:
        max_dist = rules["velo_trottinette_distance_max_m"]
        G = G_drive

    if distance_lineaire(lat_s, lon_s, lat_office, lon_office) > max_dist:
        return None, f"linear distance > {max_dist / 1000} kms"

    try:
        orig = ox.distance.nearest_nodes(G, lon_s, lat_s)
        dest = ox.distance.nearest_nodes(G, lon_office, lat_office)
        return nx.shortest_path_length(G, orig, dest, weight="length"), None
    except Exception as e:
        return None, f"Erreur trajet : {e}"

def compute_distance(adresse, moyen):
    if adresse is None:
        return None, "adresse manquante"
    if moyen is None:
        return None, "moyen_deplacement manquant"

    m = moyen.lower()

    if "marche" in m or "running" in m:
        mode = "walk"
        max_dist = rules["marche_running_distance_max_m"]
    elif "vélo" in m or "trottinette" in m:
        mode = "drive"
        max_dist = rules["velo_trottinette_distance_max_m"]
    else:
        return None, None  # non pertinent

    loc_latitude, loc_longitude, error_geocode = geocode_ban(adresse)

    if error_geocode is not None:
        return None, f"Erreur geocode : {error_geocode}"

    if loc_latitude is None or loc_longitude is None:
        return None, "Erreur geocode inconnue"
    
    d, e = distance_trajet(loc_latitude, loc_longitude, mode)

    if e is not None :
        return None, e
    elif d > max_dist:
        return d, f"distance hors règle : {round(d/1000,2)} kms"
    else:
        return d, None


def est_eligible_prime(moyen, distance):
    if moyen is None or distance is None:
        return False

    m = moyen.lower()

    if "marche" in m or "running" in m:
        return distance <= rules["marche_running_distance_max_m"]
    if "vélo" in m or "trottinette" in m:
        return distance <= rules["velo_trottinette_distance_max_m"]

    return False

def has_relevant_change(before, after):
    if not before or not after:
        return False

    return (
        before.get("adresse_domicile") != after.get("adresse_domicile")
        or before.get("moyen_deplacement") != after.get("moyen_deplacement")
    )

def close_active_silver_row(id_salarie, now):
    dt = DeltaTable(ELIG_ANNUELLE_PATH)
    table = dt.to_pyarrow_table()
    df = table.to_pandas()

    mask = (df["id_salarie"] == id_salarie) & (df["date_fin"].isna())

    if not mask.any():
        return

    df.loc[mask, "date_fin"] = now
    df.loc[mask, "duree"] = (
        (now - df.loc[mask, "date_debut"]).dt.days
    )

    write_deltalake(
        ELIG_ANNUELLE_PATH,
        pa.Table.from_pandas(df),
        mode="overwrite"
    )

def append_silver_and_incoh(
    *,
    id_salarie,
    eligibilite,
    incoh_reason,
    now,
    dt,
    silver_rows,
    incoh_rows
):
    """
    Ajoute :
    - une ligne Silver SCD2 (ouverte)
    - une incohérence si nécessaire
    """

    if incoh_reason:
        incoh_rows.append({
            "id_salarie": id_salarie,
            "raison_incoherence": incoh_reason,
            "timestamp": now
        })

    silver_rows.append({
        "id_salarie": id_salarie,
        "est_eligible_prime_annuel": eligibilite,
        "date_debut": dt,
        "date_fin": pa.scalar(None, type=timestamp('ns')),
        "duree": pa.scalar(None, type=int32())
    })
    
def write_postgres_salaries(rows, action='insert'):
    """
    rows : liste de dicts avec au moins la clé "id_salarie"
    """
    if not rows:
        return

    try:
        with psycopg2.connect(**{**conn_params, "dbname": "sport_poc"}) as conn:
            with conn.cursor() as cur:
                if action == 'insert':
                    # Prépare les valeurs à insérer
                    values = [(r["id_salarie"],) for r in rows]
                    execute_values(
                        cur,
                        "INSERT INTO salaries (id_salarie) VALUES %s",
                        values
                    )
                elif action =='delete':
                    values = [(now, r["id_salarie"]) for r in rows]
                    execute_values(
                        cur,
                        """
                        UPDATE salaries AS s
                        SET deleted_at = v.deleted_at
                        FROM (VALUES %s) AS v(deleted_at, id_salarie)
                        WHERE s.id_salarie = v.id_salarie
                        """,
                        values
                    )
                else:
                    print(f'Action {action} non autorisée. Transaction Postgres interrompue.')
    finally:
        conn.close()

def query_rh_table_for_values(id_salarie: int) -> dict:
    """
    Récupère des informations dynamiques d'un salarié depuis la table RH PostgreSQL
    pour alimenter les messages Slack ou autres traitements.

    Args:
        id_salarie (int): ID du salarié à récupérer.

    Returns:
        dict: dictionnaire des valeurs dynamiques (clé=nom_colonne, valeur)
    """
    # --- Connexion PostgreSQL ---
    

    query = """
        SELECT 
            nom,
            prenom
        FROM salaries
        WHERE id_salarie = %s
    """

    result = {}
    try:
        with psycopg2.connect(**{**conn_params, "dbname": "rh"}) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (id_salarie,))
                row = cur.fetchone()
                if row:
                    result = dict(row)
    except Exception as e:
        print(f"Erreur query_rh_table_for_values id={id_salarie}: {e}")
    
    return result

def query_sport_events_from_salaries_and_year(seuil:int, gain:int) -> dict:
    """
    
    """
    # --- Connexion PostgreSQL ---
    

    query = """
        SELECT
            salarie_id,
            EXTRACT(YEAR FROM date_fin_activite)::INT AS annee,
            COUNT(id_evenement_sport) AS nb_activites,
        	CASE 
                WHEN COUNT(id_evenement_sport) > %s THEN TRUE
                ELSE FALSE
            END AS est_eligible,
            CASE
                WHEN COUNT(id_evenement_sport) > %s THEN %s
                ELSE 0
            END AS nb_jour_bien_etre
        FROM evenements_sport
        GROUP BY salarie_id, annee
    """

    result = []
    with psycopg2.connect(**{**conn_params, "dbname": "sport_poc"}) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query,(seuil,seuil,gain))
            rows = cur.fetchall()
            for row in rows:
                result.append(dict(row))
    return result

def query_rh_table_for_incomes() -> dict:
    """
    
    """
    # --- Connexion PostgreSQL ---
    

    query = """
        SELECT 
            id_salarie,
            salaire_brut as sb
        FROM salaries
    """

    result = {}
    with psycopg2.connect(**{**conn_params, "dbname": "rh"}) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            result = {row['id_salarie']: row['sb'] for row in cur.fetchall()}
    
    return result

def build_slack_message(
        sport: str,
        prenom: str,
        nom: str,
        duree_formate: int,
        distance_km: float | None = None,
        commentaire: str | None = None
    ) -> str:
    
    sport_tpl = TEMPLATES_BY_SPORT[sport]

    interjection = random.choice(INTERJECTIONS)
    constat = random.choice(sport_tpl["constats"])
    encouragement = random.choice(sport_tpl["encouragements"])
    emojis = " ".join(random.sample(
        sport_tpl["emojis"],
        k=random.randint(1, len(sport_tpl["emojis"]) - 1)
    ))

    commentaire_msg = f' ("{commentaire}")' if commentaire else ""

    message = (
        f"{interjection} {prenom} {nom} ! "
        f"{constat.format(distance_km=distance_km, duree_formate=duree_formate)} ! "
        f"{encouragement} ! {emojis} "
        f"{commentaire_msg}. "
    )

    return message

def format_duree(seconds: int) -> str:
    """
    Convertit un temps en secondes en format 'XhYmin'.
    - Si heures = 0, on ne les affiche pas.
    - Les minutes sont arrondies à l'entier le plus proche.
    - Les secondes sont ignorées.
    """
    if seconds is None or seconds <= 0:
        return "0min"
    
    # Conversion
    total_minutes = round(seconds / 60)
    hours = total_minutes // 60
    minutes = total_minutes % 60

    if hours > 0:
        return f"{hours}h{minutes}min" if minutes > 0 else f"{hours}h"
    else:
        return f"{minutes}min"

def generate_batch_id(polled) -> str:
    """
    Génère un batch_id à partir d'un poll Kafka, traçable et rejouable.
    
    Args:
        polled (Dict[TopicPartition, List[Message]]): résultat de consumer.poll()
    
    Returns:
        str: batch_id unique, contenant timestamp + offsets par partition
             ex: "1768571234567_p0:1200-1245_p1:800-832"
    """
    batch_ts = int(time.time() * 1000)  # timestamp millisecondes
    partition_ranges = []

    for tp, msgs in polled.items():
        if not msgs:
            continue
        start_offset = msgs[0].offset
        end_offset = msgs[-1].offset
        partition_ranges.append(f"p{tp.partition}:{start_offset}-{end_offset}")

    batch_id = f"{batch_ts}_" + "_".join(partition_ranges)
    return batch_id

def days_in_year(year):
    return 366 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 365
        
def calcul_prime(row):
    income = incomes.get(row['id_salarie'], None)
    if income is None:
        return None
    if row['ratio_jours'] > rules['%_jours_eligibles_par_an']:
        return round(rules['prime_annuelle_%_brut'] * income / 100,2)
    return 0

# =========================================================
# 5. Boucle principale Kafka - Consommation des topics et pipeline de transformation
# =========================================================
print("Prometheus service starting ...")
start_http_server(8000)
print("Prometheus service up and running.")

print("Worker started, waiting for Kafka messages...",flush=True)

while True:
    PIPELINE_ALIVE.set(1)
    # -------------------------
    # Polls initial
    # -------------------------
    polled_rh = consumer_rh.poll(timeout_ms=200)

    polled_sport = consumer_sport.poll(timeout_ms=200)

    now = datetime.utcnow()
    
    sport_batch_id = generate_batch_id(polled_sport)
    rh_batch_id = generate_batch_id(polled_rh)
    
    # 1. Transformation batch RH
    
    bronze_rows_rh = []
    silver_rows_rh = []
    incoh_rows_rh = []
    postgres_rows_to_create_rh = []
    postgres_rows_to_delete_rh = []
    
    if polled_rh:
        print('Processing poll RH ...')
    
        for msgs in polled_rh.values():
            for msg in msgs:
                if msg.value is None:
                    continue  # on skip les messages sans payload
                try:
                    payload = msg.value  # value désérialisé via value_deserializer
                    payload = payload.get("payload", {})
                except Exception as e:
                    print("Erreur parsing message:", e)
                    traceback.print_exc()
                    continue  # on skip ce message
        
                op = payload.get("op")
                after = payload.get("after")
                before = payload.get("before")
                
                #print(f'Op : {op}, Before : {before}, After : {after}')
                # --- Bronze
                bronze_rows_rh.append({
                    "op": op,
                    "before": json.dumps(before),
                    "after": json.dumps(after),
                    "ingestion_ts": now
                })
        
                # --- Silver
                if op in ("r", "c") and after:
                    postgres_rows_to_create_rh.append({
                        "id_salarie": after.get("id_salarie"),
                    })
                    
                    distance_m, incoh_reason = compute_distance(
                        after.get("adresse_domicile"),
                        after.get("moyen_deplacement")
                    )
        
                    elig = est_eligible_prime(
                        after.get("moyen_deplacement"),
                        distance_m
                    )
                    
                    append_silver_and_incoh(
                        id_salarie=after.get("id_salarie"),
                        eligibilite=elig,
                        incoh_reason=incoh_reason,
                        now=now,
                        dt=datetime(2026, 1, 1, 0, 0, 0),
                        silver_rows=silver_rows_rh,
                        incoh_rows=incoh_rows_rh
                    )
                    
                elif op == "u" and before and after:
                    #print('\nUpdate detected')
                    # ===== UPDATE =====
                    if not has_relevant_change(before, after):
                        #print("Update : no relevant changes detected")
                        # update non pertinent → on ignore
                        pass
                    else:
                        # calcul distance basé sur BEFORE
                        distance_m_before, incoh_reason_b = compute_distance(
                            before.get("adresse_domicile"),
                            before.get("moyen_deplacement")
                        )
                        
                        # calcul distance basé sur AFTER
                        distance_m_after, incoh_reason = compute_distance(
                            after.get("adresse_domicile"),
                            after.get("moyen_deplacement")
                        )
                        #print(f'Update : after adresse = {after.get("adresse_domicile")} ; after moyen = {after.get("moyen_deplacement")} ; after distance = {distance_m_after}m')
                
                        elig_before = est_eligible_prime(
                            before.get("moyen_deplacement"),
                            distance_m_before
                        )
                        elig_after = est_eligible_prime(
                            after.get("moyen_deplacement"),
                            distance_m_after
                        )
                        #print(f'Update : eligibilite before = {elig_before} ; eligibilite after = {elig_after}')
                        if elig_before != elig_after:
                            print('Update : Changement éligibilité')
                            id_salarie = after.get("id_salarie")
                
                            # Clôture de la ligne active
                            close_active_silver_row(
                                id_salarie=id_salarie,
                                now=now
                            )
                            
                            append_silver_and_incoh(
                                id_salarie=after.get("id_salarie"),
                                eligibilite=elig_after,
                                incoh_reason=incoh_reason,
                                now=now,
                                dt=now,
                                silver_rows=silver_rows_rh,
                                incoh_rows=incoh_rows_rh
                        )
                
                elif op == "d" and before:
                    postgres_rows_to_delete_rh.append({
                        "id_salarie": before.get("id_salarie"),
                    })
                    
                    id_salarie = before.get("id_salarie")
                    
                    # Clôturer la ligne active dans Delta Silver
                    close_active_silver_row(
                        id_salarie=id_salarie,
                        now=now
                    )
        print("Poll rh processing succesfully terminated.")

    # 2. Transformation batch Sport
    slack_error_rows = []
    gold_pipe_error_rows = []
    salaries_to_update = []
    annees_to_update = []
    
    if polled_sport:
        print('Processing poll sport ...')
    
        for msgs in polled_sport.values():
            for msg in msgs:
                if msg.value is None:
                    print(f'msg.value est None : {msg}', flush=True)
                    continue
                try:
                    payload = msg.value
                    payload = payload.get("payload", {})
                except Exception as e:
                    print("Erreur parsing message:", e, flush=True)
                    traceback.print_exc()
                    continue
            
            
                op = payload.get("op")
                after = payload.get("after")
                
                #print(f'Op : {op}, After : {after}', flush=True)
                
                if op != "c" and after:
                    continue
                
                salarie = after.get("salarie_id")
                duree_s = int((after.get("date_fin_activite") - after.get("date_debut_activite")) / 1000000)
                distance_km = round(after.get("distance_m") / 1000, 1) if after.get("distance_m") not in (0, None) else None
                annee_activite = datetime.fromtimestamp(after.get("date_fin_activite") / 1_000_000).year if after.get("date_fin_activite") is not None else None
                
                salaries_to_update.append(salarie)
                annees_to_update.append(annee_activite) is annee_activite is not None
                
                # --- Slack ---
                try:
                    dynamic_values_from_rh = query_rh_table_for_values(salarie)
                    
                    slack_message = build_slack_message(
                        after.get("sport_type"),
                        dynamic_values_from_rh['prenom'],
                        dynamic_values_from_rh['nom'],
                        format_duree(duree_s),
                        distance_km,
                        after.get("commentaire")
                    )
                    
                    send_slack_message(slack_message)
                except Exception as e:
                    print(f'Error : {e}', flush=True)
                    traceback.print_exc()
                    slack_error_rows.append({
                        "salarie": salarie,
                        "error": str(e),
                        "timestamp": now
                    })
        print("Poll sport processing succesfully terminated.")
    
    # 3. Écriture batchs
    if bronze_rows_rh:
        write_deltalake(BRONZE_PATH, pa.Table.from_pylist(bronze_rows_rh), mode="append")
        print(f"Writing successful in Bronze RH : {len(bronze_rows_rh)} rows", flush=True)

    if silver_rows_rh:
        # TEST COHERENCE GE : Vérifier que les colonnes id_salarie et date_debut ne contiennent pas de NULL
        silver_rows_rh_ge = ge.from_pandas(silver_rows_rh)
        silver_rows_rh_ge.expect_column_values_to_not_be_null('id_salarie')
        silver_rows_rh_ge.expect_column_values_to_not_be_null('date_debut')
        
        # TEST COHERENCE GE : Vérifier que date_debut est un datetime
        silver_rows_rh_ge.expect_column_values_to_be_of_type('date_debut', 'datetime64[ns]')
        
        # TEST COHERENCE GE : Vérifier que duree est soit null soit > 0
        silver_rows_rh_ge.expect_column_values_to_be_of_type('date_debut', 'datetime64[ns]')
        
        # TEST COHERENCE GE : Vérifier que est_eligible_prime_annuel est un boolean
        silver_rows_rh_ge.expect_column_values_to_be_of_type('est_eligible_prime_annuel', 'bool')
        
        ge_results_rh.append({"batch_id":rh_batch_id,"dataframe_name":"silver_rows_rh_ge","dataframe":silver_rows_rh_ge,"results":silver_rows_rh_ge.validate()})
        
        write_deltalake(ELIG_ANNUELLE_PATH, pa.Table.from_pylist(silver_rows_rh), mode="append")
        print(f"Writing successful in Silver RH : {len(silver_rows_rh)} rows", flush=True)
        
    if postgres_rows_to_create_rh:
        # TEST COHERENCE GE : Vérifier que les colonnes id_salarie ne contiennent pas de NULL
        postgres_rows_to_create_rh_ge = ge.from_pandas(postgres_rows_to_create_rh)
        postgres_rows_to_create_rh_ge.expect_column_values_to_not_be_null('id_salarie')
        
        ge_results_rh.append({"batch_id":rh_batch_id,"dataframe_name":"postgres_rows_to_create_rh","dataframe":postgres_rows_to_create_rh,"results":postgres_rows_to_create_rh_ge.validate()})
        
        write_postgres_salaries(postgres_rows_to_create_rh,action='insert')
        print(f"Writing successful in Postgres sport_poc:salaries (insert) : {len(silver_rows_rh)} rows", flush=True)
    
    if postgres_rows_to_delete_rh:
        # TEST COHERENCE GE : Vérifier que les colonnes id_salarie ne contiennent pas de NULL
        postgres_rows_to_delete_rh_ge = ge.from_pandas(postgres_rows_to_delete_rh)
        postgres_rows_to_delete_rh_ge.expect_column_values_to_not_be_null('id_salarie')
        
        ge_results_rh.append({"batch_id":rh_batch_id,"dataframe_name":"postgres_rows_to_delete_rh_ge","dataframe":postgres_rows_to_delete_rh_ge,"results":postgres_rows_to_delete_rh_ge.validate()})
        
        write_postgres_salaries(postgres_rows_to_delete_rh,action='delete')
        print(f"Writing successful in Postgres sport_poc:salaries (delete) : {len(postgres_rows_to_delete_rh)} rows", flush=True)

    if incoh_rows_rh:
        # TEST COHERENCE GE : Vérifier que les colonnes id_salarie et raison_incoherence ne sont pas NULL
        incoh_rows_rh_ge = ge.from_pandas(incoh_rows_rh)
        incoh_rows_rh_ge.expect_column_values_to_not_be_null('id_salarie')
        incoh_rows_rh_ge.expect_column_values_to_not_be_null('raison_incoherence')
        
        # TEST COHERENCE GE : Vérifier que timestamp n est pas NULL et est un timestamp
        incoh_rows_rh_ge.expect_column_values_to_not_be_null('timestamp')
        incoh_rows_rh_ge.expect_column_values_to_be_of_type("timestamp", "datetime64[ns]")
        
        ge_results_rh.append({"batch_id":rh_batch_id,"dataframe_name":"incoh_rows_rh_ge","dataframe":incoh_rows_rh_ge,"results":incoh_rows_rh_ge.validate()})
    
        write_deltalake(INCOH_PATH, pa.Table.from_pylist(incoh_rows_rh), mode="append")
        print(f"Writing successful in incoherences RH : {len(incoh_rows_rh)} rows", flush=True)

    if slack_error_rows:
        # TEST COHERENCE GE : Vérifier que les colonnes id_salarie et raison_incoherence ne sont pas NULL
        slack_error_rows_ge = ge.from_pandas(incoh_rows_rh)
        slack_error_rows_ge.expect_column_values_to_not_be_null('id_salarie')
        slack_error_rows_ge.expect_column_values_to_not_be_null('error')
        
        # TEST COHERENCE GE : Vérifier que timestamp n est pas NULL et est un timestamp
        slack_error_rows_ge.expect_column_values_to_not_be_null('timestamp')
        slack_error_rows_ge.expect_column_values_to_be_of_type("timestamp", "datetime64[ns]")
        
        ge_results_sport.append({"batch_id":sport_batch_id,"dataframe_name":"slack_error_rows","dataframe":slack_error_rows,"results":slack_error_rows_ge.validate()})
        
        write_deltalake(SLACK_ERROR_PATH,pa.Table.from_pylist(slack_error_rows),mode="append")
        print(f"Writing successful in slack errors : {len(slack_error_rows)} lignes", flush=True)
        
    
    # --- Gold BI from Sport ---
    if polled_sport:
        try:            
            print("Updating Gold dataset 'Eligibilité journée bien être' ...", flush=True)
            data_evenements = query_sport_events_from_salaries_and_year(rules['nb_activite_physique_min'], rules['nb_journee_bien_etre_gagnees'])
            #print(data_evenements, flush=True)
            
            write_deltalake(ELIG_JOURNEE_BIEN_ETRE_PATH, pa.Table.from_pylist(data_evenements), mode="overwrite")
            print("Gold dataset 'Eligibilité journée bien être' successfully updated.", flush=True)
            # temporaire POC : exposition rapide à Power BI
            pq.write_table(pa.Table.from_pylist(data_evenements), f"{parquet_path}/eligibilite_journee_bien_etre_par_an.parquet")
            
                        
        except Exception as e:
            try:
                print(f'Error : {e}', flush=True)
                traceback.print_exc()
                write_deltalake(GOLD_PIPE_ERROR_PATH,pa.Table.from_pylist([{"batch_id": sport_batch_id,"error": str(e)}]),mode="append")
            except Exception as e2:
                traceback.print_exc()
                print(f'Error logging error {e} : {e2} in batch {sport_batch_id}', flush=True)
            
    # --- Gold BI from rh ---
    try:
        silver_not_empty = (DeltaTable(ELIG_ANNUELLE_PATH).to_pyarrow_table().num_rows != 0)
    except:
        silver_not_empty = True
    if (polled_rh or polled_sport) and silver_not_empty:
        try:
            print("Updating Gold dataset 'Eligibilité prime annuelle' ...", flush=True)
            incomes = query_rh_table_for_incomes()
            #print(incomes, flush=True)
            dt_silver = DeltaTable(ELIG_ANNUELLE_PATH)
            df_silver = dt_silver.to_pandas()
            
            # TEST COHERENCE GE : Vérifier que les colonnes de dates ne contiennent pas de NULL
            df_ge = ge.from_pandas(df_silver)
            df_ge.expect_column_values_to_not_be_null('date_debut')
            df_ge.expect_column_values_to_not_be_null('date_fin')
            
            # TEST COHERENCE GE : Vérifier que est_eligible_prime_annuel est booléen
            df_ge.expect_column_values_to_be_in_set('est_eligible_prime_annuel', [True, False])
            
            current_year = datetime.now().year
            end_of_year = pd.Timestamp(f"{current_year}-12-31 23:59:59")
            df_silver['date_fin'] = df_silver['date_fin'].fillna(end_of_year)
            df_silver['durée'] = round((df_silver['date_fin'] - df_silver['date_debut']).dt.total_seconds() / (24 * 3600),0)
            
            # TEST COHERENCE GE : Vérifier que durée (après calcul) est positive
            df_ge = ge.from_pandas(df_silver)
            df_ge.expect_column_values_to_be_between('durée', min_value=0)

            ge_results_sport.append({"batch_id":rh_batch_id,"dataframe_name":"df_silver","dataframe":df_silver,"results":df_ge.validate()})
            
            # Ajouter une colonne 'année' pour l'agrégation
            df_silver['année'] = df_silver['date_fin'].dt.year
            # Somme par année et par salarié
            df_gold = (
                df_silver[df_silver['est_eligible_prime_annuel']]
                .groupby(['id_salarie', 'année'], as_index=False)
                .agg({'durée': 'sum'})
            )
            df_gold['ratio_jours'] = df_gold.apply(lambda row: round(100 * row['durée'] / days_in_year(row['année']),2), axis=1)
            df_gold['montant_prime'] = df_gold.apply(calcul_prime, axis=1)
            
            # TEST COHERENCE GE : Vérifier que les colonnes id_salarie et année ne contiennent pas de NULL
            df_gold_ge = ge.from_pandas(df_gold)
            df_gold_ge.expect_column_values_to_not_be_null('id_salarie')
            df_gold_ge.expect_column_values_to_not_be_null('année')
            
            # TEST COHERENCE GE : Vérifier que ratio_jours est toujours entre 0 et 100
            df_gold_ge.expect_column_values_to_be_between('ratio_jours', min_value=0, max_value=100)
            
            # TEST COHERENCE GE : Vérifier que ratio_jours est toujours supérieur ou égal à 0
            df_gold_ge.expect_column_values_to_be_between('montant_prime', min_value=0)
            
            # TEST COHERENCE GE : Vérifier l'unicité id_salarie + année
            df_gold_ge.expect_compound_columns_to_be_unique(column_list=["id_salarie", "année"])
            
            ge_results_sport.append({"batch_id":rh_batch_id,"dataframe_name":"df_gold","dataframe":df_gold,"results":df_gold_ge.validate()})
            
            write_deltalake(ELIG_FIN_ANNEE_PATH, pa.Table.from_pandas(df_gold), mode='overwrite')
            print("Gold dataset 'Eligibilité prime annuelle' successfully updated.", flush=True)
            # temporaire POC : exposition rapide à Power BI
            pq.write_table(pa.Table.from_pandas(df_gold), f"{parquet_path}/eligibilite_fin_annee.parquet")
            
        except Exception as e:
            try:
                print(f'Error : {e}', flush=True)
                traceback.print_exc()
                write_deltalake(GOLD_PIPE_ERROR_PATH,pa.Table.from_pylist([{"batch_id": rh_batch_id, "error": str(e)}]),mode="append")
            except Exception as e2:
                traceback.print_exc()
                print(f'Error logging error {e} : {e2} in batch {rh_batch_id}', flush=True)
                
        # ECRITURE RESULTATS GE
        try:
            print('Processing Great Expectations checks ...', flush=True)
            ge_errors = []
            for l in [ge_results_sport, ge_results_rh]:
                for item in l:
                    results = item["results"]
                
                    # On ne garde que les validations en échec
                    if results.get("success") is False:
                        df = item["dataframe"]
                
                        failed_rows = []
                
                        for r in results.get("results", []):
                            if not r.get("success"):
                                idxs = r.get("result", {}).get("unexpected_index_list", [])
                
                                for i in idxs:
                                    row = df.iloc[i].to_dict()
                                    row["expectation"] = r["expectation_config"]["expectation_type"]
                                    failed_rows.append(row)
                
                        # On reconstruit l'objet propre
                        new_item = {
                            "batch_id": item["batch_id"],
                            "dataframe_name": item["dataframe_name"],
                            "results": failed_rows
                        }
                
                        ge_errors.append(new_item)
            if ge_errors: 
                write_deltalake(ERRORS_GE_PATH,pa.Table.from_pylist(ge_errors),mode="append")
                print('Great Expectations errors saved successful', flush=True)
            print('Processing Great Expectations successfully terminated.', flush=True)
            print('Waiting for next batch...', flush=True)
        except Exception as e:
            print(f'Error : {e}', flush=True)
            traceback.print_exc()
        
    # 4. Commits Kafka après traitement du batch
    consumer_rh.commit()
    consumer_sport.commit()

    
 
        
 
        
 