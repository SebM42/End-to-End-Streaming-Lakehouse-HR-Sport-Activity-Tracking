\connect sport_poc

CREATE TABLE salaries (
    id_salarie SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	deleted_at TIMESTAMP
);

CREATE TABLE evenements_sport (
    id_evenement_sport SERIAL PRIMARY KEY,
    salarie_id INT NOT NULL REFERENCES salaries(id_salarie),
    date_debut_activite TIMESTAMP NOT NULL,
	date_fin_activite TIMESTAMP NOT NULL CHECK (date_fin_activite > date_debut_activite),
	sport_type VARCHAR(50) NOT NULL,
    distance_m INT CHECK (distance_m >= 0),
    commentaire TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_evenements_sport_salarie ON evenements_sport(salarie_id);

ALTER TABLE evenements_sport REPLICA IDENTITY FULL;




