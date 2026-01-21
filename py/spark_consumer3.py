# -*- coding: utf-8 -*-
"""
Created on Sat Jan 10 11:40:18 2026

"""

import json
import osmnx as ox
import networkx as nx
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    when,
    lower,
    lit,
    udf
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType
)
from delta.tables import DeltaTable


# =========================================================
# 1. Lecture de la configuration
# =========================================================

with open("/config.json") as f:
    config = json.load(f)

entreprises = {e["id"]: e["adresse"] for e in config["entreprises"]}
ville_entreprises = {e["id"]: e["ville"] for e in config["entreprises"]}


# =========================================================
# 2. Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("sport-poc-consumer-bronze-silver")
    .getOrCreate()
)


# =========================================================
# 3. Schéma Debezium
# =========================================================

schema_debezium = StructType([
    StructField("payload", StructType([
        StructField("op", StringType()),  # r, c, u, d
        StructField("after", StructType([
            StructField("id_salarie", IntegerType()),
            StructField("adresse_domicile", StringType()),
            StructField("moyen_deplacement", StringType()),
        ])),
        StructField("before", StructType([
            StructField("id_salarie", IntegerType()),
            StructField("adresse_domicile", StringType()),
            StructField("moyen_deplacement", StringType()),
        ]))
    ]))
])


# =========================================================
# 4. Chemins Delta
# =========================================================

# Bronze
bronze_path = "/datalake/bronze/rh_salaries"
bronze_checkpoint = "/datalake/checkpoints/bronze_rh_salaries"

# Silver
silver_path = "/datalake/silver/eligibilite_annuelle"
silver_checkpoint = "/datalake/checkpoints/silver_eligibilite"


# =========================================================
# 5. Initialisation table Silver
# =========================================================

schema_silver = StructType([
    StructField("id_salarie", IntegerType(), True),
    StructField("est_eligible_prime_annuel", BooleanType(), True),
    StructField("date_debut", TimestampType(), True),
    StructField("date_fin", TimestampType(), True),
    StructField("duree", IntegerType(), True)
])

if not DeltaTable.isDeltaTable(spark, silver_path):
    (
        spark.createDataFrame([], schema_silver)
        .write
        .format("delta")
        .mode("overwrite")
        .save(silver_path)
    )



# =========================================================
# 6. PIPELINE BRONZE : Kafka ➜ Delta (CDC brut)
# =========================================================

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "pg_rh.public.salaries")
    .option("startingOffsets", "earliest")
    .load()
)

bronze_df = (
    kafka_stream
    .selectExpr("CAST(value AS STRING) as json_string")
    .select(from_json(col("json_string"), schema_debezium).alias("data"))
    .select(
        col("data.payload.op").alias("op"),
        col("data.payload.before").alias("before"),
        col("data.payload.after").alias("after"),
        current_timestamp().alias("ingestion_ts")
    )
)

bronze_query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronze_checkpoint)
    .start(bronze_path)
)

print('\nPIPELINE INIT\n')
# =========================================================
# 7. PIPELINE SILVER : Bronze ➜ Éligibilité métier
# =========================================================
def distance_lineaire(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).meters

rules = config['parametres_regles']

geolocator = Nominatim(user_agent="silver_pipeline")
office_address = entreprises.get(0)
office_loc = geolocator.geocode(office_address)
lat_office, lon_office = office_loc.latitude, office_loc.longitude

center_point = (lat_office, lon_office)
G_drive = ox.graph_from_point(center_point, dist=rules["velo_trotinnette_distance_max_m"], network_type="drive")
G_walk = ox.graph_from_point(center_point, dist=rules["marche_running_distance_max_m"], network_type="walk")

def silver_pipeline(bronze_batch_df, batch_id):
    """
    Application des règles métier à partir de la couche Bronze
    """
    
    def distance_trajet(lat_s, lon_s, mode="walk"):
        """
        Calcule la distance réaliste (trajet) entre l'employé et le bureau.
        - Pré-filtrage avec distance linéaire selon le mode de transport.
        - Calcul du trajet réel avec OSMNX seulement si l'employé est dans la zone.
        
        Paramètres :
        - lat_s, lon_s : coordonnées de l'employé
        - mode : "walk" (marche/running) ou "drive" (vélo/trottinette)
        
        Retour :
        - distance en mètres si calculable
        - None si trop loin ou erreur
        """
        
        if None in [lat_s, lon_s]:
            return None
    
        # Définir la distance linéaire maximale selon le mode
        if mode == "walk":
            max_distance_lineaire = rules["marche_running_distance_max_m"]
            G = G_walk
        else:  # mode drive
            max_distance_lineaire = rules["velo_trotinnette_distance_max_m"]
            G = G_drive
    
        # Pré-filtrage : distance linéaire à vol d'oiseau
        dist_lineaire = distance_lineaire(lat_s, lon_s, lat_office, lon_office)
        if dist_lineaire > max_distance_lineaire:
            return None  # trop loin, on ne calcule pas le trajet
    
        # Calcul du trajet réel sur le graphe OSMNX
        try:
            orig_node = ox.distance.nearest_nodes(G, lon_s, lat_s)
            dest_node = ox.distance.nearest_nodes(G, lon_office, lat_office)
            distance_m = nx.shortest_path_length(G, orig_node, dest_node, weight='length')
            return distance_m
        except Exception as e:
            print("Erreur calcul trajet :", e)
            return None
    
    def udf_distance(adresse, moyen):
        """
        Calcule la distance réaliste uniquement pour les modes pertinents.
        Retourne un tuple (distance_m, raison_incoherence)
        """
    
        if adresse is None:
            return (None, "adresse manquante")
        if moyen is None:
            return (None, "moyen_deplacement manquant")
    
        moyen_lower = moyen.lower()
        # Calculer uniquement si le mode est pertinent
        if "marche" in moyen_lower or "running" in moyen_lower:
            mode = "walk"
            max_distance = rules["marche_running_distance_max_m"]
        elif "vélo" in moyen_lower or "trottinette" in moyen_lower:
            mode = "drive"
            max_distance = rules["velo_trotinnette_distance_max_m"]
        else:
            # Mode non pertinent → aucun calcul, aucune incohérence
            return (None, None)
    
        # Géocodage
        try:
            loc = geolocator.geocode(adresse)
            if loc is None:
                return (None, "adresse non reconnue")
            lat_s, lon_s = loc.latitude, loc.longitude
        except Exception as e:
            return (None, f"erreur géocodage: {e}")
    
        # Calcul distance trajet
        distance_m = distance_trajet(lat_s, lon_s, mode)
    
        # Vérifier incohérence : distance hors cadre
        if distance_m is None or distance_m > max_distance:
            return (distance_m, "distance hors règle ou erreur graphe")
    
        # Tout ok
        return (distance_m, None)
    
    distance_udf = udf(udf_distance, IntegerType())
    
    def udf_eligibilite_prime(moyen, distance_m):
        """
        Détermine l'éligibilité à la prime annuelle
        selon le mode de transport et la distance.
        Retourne True/False.
        """
        if moyen is None or distance_m is None:
            return False
    
        moyen_lower = moyen.lower()
        if "marche" in moyen_lower or "running" in moyen_lower:
            return distance_m <= rules["marche_running_distance_max_m"]
        elif "vélo" in moyen_lower or "trottinette" in moyen_lower:
            return distance_m <= rules["velo_trotinnette_distance_max_m"]
        else:
            # mode non pertinent
            return False
        
    eligibilite_prime_udf = udf(udf_eligibilite_prime, BooleanType())
    
    # --- r & c : snapshot + insert
    df_rc = (
        bronze_batch_df
        .filter(col("op").isin("r", "c"))
        .select("op", "after.*")
    )

    if df_rc.isEmpty():
        print('\nEMPTY !!!!!!!!!!!!!!')
        return
    
    print('\nBRONZE :')
    df_rc.show(5)
    
    
    silver_df = df_rc.withColumn("distance_info", distance_udf(col("adresse_domicile"), col("moyen_deplacement")))
    print('\nSILVER 1st :')
    silver_df.show(5)
    silver_df = silver_df.withColumn("distance_m", col("distance_info.distance_m")) \
                     .withColumn("raison_incoherence", col("distance_info.raison_incoherence")) \
                     .drop("distance_info")
    print('\nSILVER 2nd :')
    silver_df.show(5)
                     
    incoherences_distance_mode_df = silver_df.filter(col("raison_incoherence").isNotNull())
    print('\nINCOHERENCES :')
    incoherences_distance_mode_df.show(5)
    
    incoherences_distance_mode_df.write.format("delta") \
        .mode("append") \
        .save("/datalake/silver/incoherences_distances")

    silver_df = (
        silver_df
        .withColumn(
            "date_debut",
            when(
                col("op") == "r",
                lit("2025-01-01 00:00:00").cast("timestamp")
            ).otherwise(current_timestamp())
        )
        .withColumn(
            "est_eligible_prime_annuel",
            eligibilite_prime_udf(
                col("moyen_deplacement"),
                col("distance_m")
            )
        )
        .select(
            col("id_salarie"),
            col("est_eligible_prime_annuel"),
            col("date_debut"),
            lit(None).cast("timestamp").alias("date_fin"),
            lit(None).cast("int").alias("duree")
        )
    )
    print('\nSILVER AVANT ECRITURE DELTA ET POSTGRE :')
    silver_df.show(5)

    # --- Écriture Delta Silver
    (
        silver_df.write
        .format("delta")
        .mode("append")
        .save(silver_path)
    )

    # --- Écriture PostgreSQL (optionnelle)
    (
        silver_df.select("id_salarie")
        .write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres-sport-poc:5432/sport_poc")
        .option("dbtable", "salaries")
        .option("user", "admin")
        .option("password", "admin_sport")
        .mode("append")
        .save()
    )

if not DeltaTable.isDeltaTable(spark, bronze_path):
    spark.createDataFrame([], bronze_df.schema)\
        .write.format("delta").mode("overwrite").save(bronze_path)

silver_query = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
    .writeStream
    .foreachBatch(silver_pipeline)
    .option("checkpointLocation", silver_checkpoint)
    .start()
)


# =========================================================
# 8. Await termination
# =========================================================

spark.streams.awaitAnyTermination()