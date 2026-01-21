# -*- coding: utf-8 -*-
"""
Created on Fri Jan  9 09:04:58 2026

"""

import json
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, when, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import DoubleType
from delta.tables import DeltaTable

# --- Lire config.json ---
with open("/config.json") as f:
    config = json.load(f)

entreprises = {e["id"]: e["adresse"] for e in config["entreprises"]}

# --- Fonction pour appeler Google Maps API ---
def calculate_distance(adresse_salarie, adresse_entreprise, moyen_deplacement):
    mode = "walking" if ("marche") in moyen_deplacement.lower() else "bicycling"
    api_key = "AIzaSyCKBxnlUTGSHDvxXlcz4Wqsa9R8GRGbph8"
    url = f"https://maps.googleapis.com/maps/api/distancematrix/json?origins={adresse_salarie}&destinations={adresse_entreprise}&mode={mode}&key={api_key}"
    response = requests.get(url)
    data = response.json()
    try:
        distance_m = data["rows"][0]["elements"][0]["distance"]["value"]
        return distance_m
    except Exception as e:
        print("Erreur API:", e)
        return None

# --- Fonction pour déterminer l'éligibilité à la prime annuelle
#def is_eligible(mode_transport:str):
    #if mode_transport is None:
        #return False
    #keywords = ['marche', 'running', 'vélo', 'trottinette']
    
    #return True if any(word in mode_transport.lower() for word in keywords) else False


# --- UDF Spark ---
def udf_distance(after):
    if after is None:
        return None
    adresse = after["adresse_domicile"]
    moyen = after["moyen_deplacement"]
    adresse_ent = entreprises.get(0)
    if adresse_ent is None or adresse is None:
        return None
    return calculate_distance(adresse, adresse_ent, moyen)

#def udf_eligibilite(before_or_after):
    #f before_or_after is None:
        #return False
    #oyen = before_or_after["moyen_deplacement"]
    #return is_eligible(moyen)

# --- Ecriture vers PostgreSQL ---
def write_to_postgres(df):
    df.write \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://postgres-sport-poc:5432/sport_poc") \
      .option("dbtable", "salaries") \
      .option("user", "admin") \
      .option("password", "admin_sport") \
      .mode("append") \
      .save()

# --- Spark session ---
spark = SparkSession.builder \
    .appName("sport-poc-consumer") \
    .getOrCreate() 

# --- UDF Déclarations ---
distance_udf = udf(udf_distance, DoubleType())


# --- Schéma des messages Debezium ---
schema_debezium = StructType([
    StructField("payload", StructType([
        StructField("op", StringType()),  # c=insert, u=update, d=delete
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

# --- Schéma de la table de l'éligibilité aux primes annuelles
schema_eligibilite_annuelle = StructType([
    StructField("id_salarie", IntegerType(), True),
    StructField("est_eligible_prime_annuel", BooleanType(), True),
    StructField("date_debut", TimestampType(), True),
    StructField("date_fin", TimestampType(), True),
    StructField("duree", IntegerType(), True)  # durée en jours
])

delta_path = "/datalake/eligibilite"
checkpoint_path = "/datalake/checkpoints/pg_rh_salaries"
# --- Création de la table si elle n existe pas
if not DeltaTable.isDeltaTable(spark, delta_path):
    spark.createDataFrame([], schema_eligibilite_annuelle).write.format("delta").mode("overwrite").save(delta_path)
delta_eligibilite = DeltaTable.forPath(spark, delta_path)

# --- Init du stream pour le topic PG_RH ---
kafak_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "pg_rh.public.salaries") \
    .option("startingOffsets", "earliest") \
    .load()


def rh_change_pipeline(df, batch_id):
    # --- Extraire le JSON ---   
    df_parsed = df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), schema_debezium).alias("data")) \
        .select("data.payload.*")
        
    
    # --- Séparer le dataset par type d'opération ---
    df_parsed_r = df_parsed.filter(col("op").isin("r"))
    df_parsed_c = df_parsed.filter(col("op").isin("c"))
    df_parsed_u = df_parsed.filter(col("op").isin("u"))
    df_parsed_d = df_parsed.filter(col("op").isin("d"))
    
    # --- INIT (op='r')
    df_parsed_r_after = df_parsed_r.select(col("after.*"))
    
    new_eligibilite = df_parsed_r_after.withColumn(
        "est_eligible_prime_annuel",
        when(
            lower(col("moyen_deplacement")).rlike("marche|running|vélo|trottinette"),
            True
            ).otherwise(False)
        ).select(
            col("id_salarie"),
            col("est_eligible_prime_annuel"),
            lit("2025-01-01 00:00:00").cast(TimestampType()).alias("date_debut"),
            lit(None).cast("timestamp").alias("date_fin"),
            lit(None).cast("int").alias("duree")
        )
    
    new_eligibilite.write.format("delta").mode("append").save(delta_path)
    
    df_to_insert = df_parsed_r_after \
                        .select(
                            col("id_salarie").alias("id_salarie"),
                        )
    
    write_to_postgres(df_to_insert)



query = kafak_stream.writeStream \
    .foreachBatch(rh_change_pipeline) \
    .option("checkpointLocation", checkpoint_path) \
    .start()
query.awaitTermination()