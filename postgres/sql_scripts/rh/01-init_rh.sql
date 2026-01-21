\connect rh


CREATE TABLE salaries (
    id_salarie INT PRIMARY KEY,
    nom VARCHAR(100) NOT NULL,
    prenom VARCHAR(100) NOT NULL,
    ddn DATE NOT NULL DEFAULT '2000-01-01',
    bu VARCHAR(30) NOT NULL,
	date_embauche DATE NOT NULL DEFAULT '2000-01-01',
	salaire_brut INT NOT NULL,
	type_contrat VARCHAR(20) NOT NULL,
	nb_jours_CP INT NOT NULL,
	adresse_domicile TEXT NOT NULL,
	moyen_deplacement TEXT NOT NULL,
	ddn_as_text TEXT NOT NULL,
	date_embauche_as_text TEXT NOT NULL
);

\copy salaries(id_salarie, nom, prenom, ddn_as_text, bu, date_embauche_as_text, salaire_brut, type_contrat, nb_jours_CP, adresse_domicile, moyen_deplacement) FROM 'scripts/rh/Donnees_RH.csv' CSV HEADER;

UPDATE salaries
SET 
	date_embauche = TO_DATE(date_embauche_as_text, 'DD/MM/YYYY'),
	ddn = TO_DATE(ddn_as_text, 'DD/MM/YYYY');

ALTER TABLE salaries 
DROP COLUMN date_embauche_as_text,
DROP COLUMN ddn_as_text,
REPLICA IDENTITY FULL;