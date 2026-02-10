# -*- coding: utf-8 -*-
"""
Created on Wed Jan  7 10:41:25 2026

"""

import pandas as pd
import random
import argparse
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
env_path = os.path.join(BASE_DIR.parent, ".env")
load_dotenv(env_path)
POSTGRES_ADMIN_USERNAME = os.getenv("POSTGRES_ADMIN_USERNAME")
POSTGRES_ADMIN_PW = os.getenv("POSTGRES_ADMIN_PW")

# charge les données sportives
sport_data = pd.read_excel(BASE_DIR.parent / 'data_metier' / 'Donnees_sportive.xlsx').dropna()

# paramètres aléatoires des sports
SPORTS_PARAMS = {
    "Tennis": {"duree_sec": (3600, 10800)},
    "Badminton": {"duree_sec": (3600, 10800)},
    "Escalade": {"duree_sec": (1800, 7200)},
    "Randonnée": {"duree_sec": (3600, 7200), "vitesse_m_s": (0.8, 1.4)},
    "Triathlon": {"duree_sec": (5400, 43200), "vitesse_m_s": (3.6, 5.5)},
    "Runing": {"duree_sec": (1800, 43200), "vitesse_m_s": (2, 5)},
    "Équitation": {"duree_sec": (1800, 10800), "vitesse_m_s": (1.7, 7.5)},
    "Voile": {"duree_sec": (1800, 43200), "vitesse_m_s": (2.6, 4.3)},
    "Tennis de table": {"duree_sec": (3600, 10800)},
    "Football": {"duree_sec": (3600, 10800)},
    "Natation": {"duree_sec": (3600, 10800), "vitesse_m_s": (0.5, 1.1)},
    "Judo": {"duree_sec": (3600, 7200)},
    "Basketball": {"duree_sec": (3600, 10800)},
    "Rugby": {"duree_sec": (3600, 10800)},
    "Boxe": {"duree_sec": (1800, 7200)},
}

# templates de commentaires
COMMENT_TEMPLATES = {

    # ---------- SPORTS À INTENSITÉ ----------
    "Tennis": {
        "faible": {
            "neutre": ["Séance axée sur le relâchement et la régularité."],
            "positif": ["Bonne séance fluide avec des échanges maîtrisés."],
            "exigeant": ["Travail technique volontairement mesuré."]
        },
        "modérée": {
            "neutre": ["Séance équilibrée avec un bon rythme de jeu."],
            "positif": ["Bon engagement et échanges dynamiques."],
            "exigeant": ["Jeu constant demandant de la concentration."]
        },
        "élevée": {
            "neutre": ["Séance intense avec des échanges soutenus."],
            "positif": ["Très bon niveau d’intensité et d’engagement."],
            "exigeant": ["Forte sollicitation physique et mentale."]
        },
    },

    "Badminton": {
        "faible": {
            "neutre": ["Séance légère axée sur la mobilité."],
            "positif": ["Bonne fluidité dans les déplacements."],
            "exigeant": ["Travail contrôlé sur les appuis."]
        },
        "modérée": {
            "neutre": ["Séance dynamique avec un rythme constant."],
            "positif": ["Bon engagement sur l’ensemble des échanges."],
            "exigeant": ["Déplacements rapides demandant de la précision."]
        },
        "élevée": {
            "neutre": ["Séance très rythmée et exigeante."],
            "positif": ["Excellente intensité et réactivité."],
            "exigeant": ["Forte sollicitation cardio et musculaire."]
        },
    },

    "Escalade": {
        "faible": {
            "neutre": ["Séance technique axée sur les placements."],
            "positif": ["Bonne gestion des mouvements."],
            "exigeant": ["Travail précis sur les appuis."]
        },
        "modérée": {
            "neutre": ["Séance équilibrée entre technique et effort."],
            "positif": ["Bonne fluidité dans les enchaînements."],
            "exigeant": ["Effort soutenu demandant de la concentration."]
        },
        "élevée": {
            "neutre": ["Séance exigeante sur le plan physique."],
            "positif": ["Très bon engagement sur les voies."],
            "exigeant": ["Forte sollicitation musculaire."]
        },
    },

    "Tennis de table": {
        "faible": {
            "neutre": ["Séance technique orientée contrôle."],
            "positif": ["Bon travail de précision."],
            "exigeant": ["Répétitions maîtrisées."]
        },
        "modérée": {
            "neutre": ["Séance rythmée avec des échanges variés."],
            "positif": ["Bonne réactivité et coordination."],
            "exigeant": ["Jeu demandant une attention constante."]
        },
        "élevée": {
            "neutre": ["Séance rapide et intense."],
            "positif": ["Très bonne explosivité dans le jeu."],
            "exigeant": ["Sollicitation importante des réflexes."]
        },
    },

    "Natation": {
        "faible": {
            "neutre": ["Séance axée sur la technique et la glisse."],
            "positif": ["Bonne aisance dans l’eau."],
            "exigeant": ["Travail précis des mouvements."]
        },
        "modérée": {
            "neutre": ["Séance équilibrée et fluide."],
            "positif": ["Bon rythme et coordination."],
            "exigeant": ["Effort régulier bien maîtrisé."]
        },
        "élevée": {
            "neutre": ["Séance intense et soutenue."],
            "positif": ["Très bon engagement physique."],
            "exigeant": ["Sollicitation musculaire importante."]
        },
    },

    "Judo": {
        "faible": {
            "neutre": ["Séance axée sur la technique et les placements."],
            "positif": ["Bonne maîtrise des mouvements."],
            "exigeant": ["Travail contrôlé des gestes."]
        },
        "modérée": {
            "neutre": ["Séance dynamique avec des enchaînements variés."],
            "positif": ["Bon engagement dans les phases de combat."],
            "exigeant": ["Effort constant et précis."]
        },
        "élevée": {
            "neutre": ["Séance très engagée physiquement."],
            "positif": ["Excellente intensité dans les combats."],
            "exigeant": ["Forte sollicitation physique."]
        },
    },

    "Rugby": {
        "faible": {
            "neutre": ["Séance orientée coordination et placements."],
            "positif": ["Bonne cohésion de groupe."],
            "exigeant": ["Travail structuré et contrôlé."]
        },
        "modérée": {
            "neutre": ["Séance collective avec un bon engagement."],
            "positif": ["Bonne intensité dans le jeu."],
            "exigeant": ["Sollicitation physique constante."]
        },
        "élevée": {
            "neutre": ["Séance très physique et engagée."],
            "positif": ["Excellent engagement collectif."],
            "exigeant": ["Forte intensité dans les impacts."]
        },
    },

    "Boxe": {
        "faible": {
            "neutre": ["Séance technique orientée précision."],
            "positif": ["Bonne maîtrise des enchaînements."],
            "exigeant": ["Travail contrôlé des gestes."]
        },
        "modérée": {
            "neutre": ["Séance dynamique et bien rythmée."],
            "positif": ["Bon engagement et coordination."],
            "exigeant": ["Sollicitation cardio régulière."]
        },
        "élevée": {
            "neutre": ["Séance très intense."],
            "positif": ["Excellent niveau d’engagement."],
            "exigeant": ["Effort physique très soutenu."]
        },
    },

    # ---------- SPORTS ORIENTÉS LIEU ----------
    "Randonnée": {
        "nature": ["Sortie agréable en pleine nature.", "Parcours varié dans un cadre naturel."],
        "montagne": ["Sortie en terrain vallonné.", "Environnement naturel exigeant."],
        "urbain": ["Parcours urbain fluide et accessible."]
    },

    "Runing": {
        "nature": ["Sortie en environnement naturel.", "Parcours agréable hors des zones urbaines."],
        "urbain": ["Parcours urbain rythmé.", "Sortie fluide en milieu urbain."],
        "mixte": ["Parcours varié entre ville et espaces ouverts."]
    },

    "Triathlon": {
        "outdoor": ["Environnement varié et stimulant.", "Conditions extérieures bien exploitées."],
        "mixte": ["Parcours diversifié et exigeant."],
    },

    "Equitation": {
        "nature": ["Balade agréable en extérieur.", "Sortie en pleine nature avec le cheval."],
        "carriere": ["Travail en carrière structuré."],
    },

    "Voile": {
        "mer": ["Navigation en conditions maritimes.", "Sortie en mer bien maîtrisée."],
        "plan_eau": ["Navigation fluide sur plan d’eau calme."]
    },

    # ---------- SPORTS ORIENTÉS TYPE ----------
    "Football": {
        "competition": ["Match engagé avec une bonne intensité collective."],
        "entrainement": ["Séance collective structurée."],
        "chilling": ["Match convivial et détendu."]
    },

    "Basketball": {
        "competition": ["Match dynamique et engagé."],
        "entrainement": ["Séance axée sur le jeu collectif."],
        "chilling": ["Match amical dans une bonne ambiance."]
    },
    
    # ---------- DEFAULT ------------
    "DEFAULT": {
        "neutre": [
            "Séance sportive réalisée avec régularité.",
            "Bonne implication pendant l’activité.",
            "Séance effectuée dans de bonnes conditions.",
            "Activité menée avec sérieux et engagement.",
            "Participation active à la séance.",
            "Séance agréable et bien structurée.",
            "Bonne maîtrise de l’activité pendant la séance.",
            "Effort constant et régulier tout au long de la séance.",
            "Séance productive et satisfaisante.",
            "Engagement apprécié lors de l’activité."
        ]
    },
}

# génération d'un stock de commentaire par sport
AVAILABLE_TEMPLATES_BY_SPORT = {}

for sport, axes in COMMENT_TEMPLATES.items():
    templates_set = set()
    for axis_value in axes.values():
        if isinstance(axis_value, dict):
            for tone_block in axis_value.values():
                templates_set.update(tone_block)
        else:
            templates_set.update(axis_value)
    AVAILABLE_TEMPLATES_BY_SPORT[sport] = templates_set

# probabilité qu un commentaire soit généré
PROBABILITY = 1 / 50

def maybe_generate_comment_for_sport(sport):
    # fallback sur DEFAULT si sport inconnu
    sport_key = sport if sport in AVAILABLE_TEMPLATES_BY_SPORT else "DEFAULT"
    available = AVAILABLE_TEMPLATES_BY_SPORT[sport_key]

    if not available:
        # plus aucun template dispo → probabilité = 0
        return None

    # test probabiliste
    if random.random() > PROBABILITY:
        return None

    # tirage d’un template unique
    template = random.choice(list(available))
    available.remove(template)  # on marque le template comme utilisé
    return template


def generate_random_date_within_last_year():
    today = datetime.now()
    days_back = random.randint(0, 365)
    random_time = timedelta(
        days=days_back,
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )
    return today - random_time

def generate_event(row):
    salarie = row["ID salarié"]
    sport = row["Pratique d'un sport"]
    
    if sport in SPORTS_PARAMS:
        params = SPORTS_PARAMS[sport]
        duree_min, duree_max = params["duree_sec"]
        date_debut = generate_random_date_within_last_year()
        duree_sec = random.randint(duree_min, duree_max)
        date_fin = date_debut + timedelta(seconds=duree_sec)
        if "vitesse_m_s" in params:
            vitesse = random.uniform(params["vitesse_m_s"][0], params["vitesse_m_s"][1])
            distance_m = round(duree_sec * vitesse, 2)
        else:
            distance_m = 0.0
    else:
        # Fallback pour sport inconnu
        date_debut = generate_random_date_within_last_year()
        duree_sec = random.randint(3600, 7200)
        date_fin = date_debut + timedelta(seconds=duree_sec)
        distance_m = 0.0
    
    return pd.Series({
        "salarie_id": salarie,
        "date_debut_activite": date_debut,
        "date_fin_activite": date_fin,
        "distance_m": distance_m,
        "sport_type": sport,
        "commentaire": maybe_generate_comment_for_sport(sport)
    })

def main(nb_events):
    global df_events
    # Génération des événements
    events = [
        generate_event(sport_data.sample(1).iloc[0])
        for _ in range(nb_events)
    ]

    df_events = pd.DataFrame(events)

    # -----------------------
    # Connexion PostgreSQL
    # -----------------------
    
    engine = create_engine(
        f"postgresql+psycopg2://{POSTGRES_ADMIN_USERNAME}:{POSTGRES_ADMIN_PW}@localhost:5433/sport_poc"
    )

    # Insertion en base
    try:
        df_events.to_sql(
            name="evenements_sport",
            con=engine,
            if_exists="append",
            index=False,
            )
        print(f"{len(df_events)} événements insérés avec succès.")
    except SQLAlchemyError as e:
        print("Erreur SQLAlchemy :")
        print(e)
    

    

# -----------------------
# CLI
# -----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Génération d'événements sportifs")
    parser.add_argument(
        "-e",
        "--nb-events",
        type=int,
        required=False,
        default=1000,
        help="Nombre d'événements à générer",
    )

    args = parser.parse_args()
    main(args.nb_events)