BEGIN;

-- Table externe des observations
DROP FOREIGN TABLE IF EXISTS programme_externe.obsocc_pnf ;
CREATE FOREIGN TABLE programme_externe.obsocc_pnf (
        id_obs integer,
        date_obs date,
        date_debut_obs date,
        date_fin_obs date,
        date_textuelle text,
        regne text,
        nom_vern text,
        nom_complet text,
        cd_nom text,
        effectif_textuel text,
        effectif_min integer,
        effectif_max integer,
        type_effectif text,
        phenologie text,
        id_waypoint text,
        longitude decimal,
        latitude decimal,
        localisation text,
        observateur text,
        numerisateur integer,
        validateur integer,
        structure text,
        remarque_obs text,
        code_insee text,
        id_lieu_dit text,
        diffusable boolean,
        precision text,
        statut_validation text,
        id_etude integer,
        id_protocole integer,
        effectif integer,
        url_photo text,
        commentaire_photo text,
        decision_validation text,
        heure_obs time,
        determination text,
        elevation integer,
        geometrie geometry
)
SERVER pnf_svr
OPTIONS (table_name 'saisie_observation', schema_name 'saisie');
;


-- Tables temporaire locales pour copier les tables distantes
DROP TABLE IF EXISTS programme_externe.obsocc_pnf_dur;
CREATE TABLE programme_externe.obsocc_pnf_dur AS
SELECT * FROM programme_externe.obsocc_pnf;

CREATE INDEX ON programme_externe.obsocc_pnf_dur (id_obs);
CREATE INDEX ON programme_externe.obsocc_pnf_dur USING GIST (geometrie);
CREATE INDEX ON programme_externe.obsocc_pnf_dur (ST_GeometryType(geometrie));

CREATE INDEX ON programme_externe.obsocc_pnf_dur (CAST(cd_nom AS bigint));
CREATE INDEX ON programme_externe.obsocc_pnf_dur (id_obs);

-- Table des personnes
CREATE FOREIGN TABLE programme_externe.personne (
    id_personne int4,
    remarque text ,
    fax text ,
    portable text ,
    tel_pro text ,
    tel_perso text ,
    pays text ,
    ville text ,
    code_postal text ,
    adresse_1 text ,
    prenom text ,
    nom text ,
    email text ,
    role text ,
    specialite text ,
    mot_de_passe text ,
    createur int4 ,
    titre text ,
    date_maj date,
    id_structure integer
)
SERVER pnf_svr
OPTIONS (table_name 'personne', schema_name 'md');
;

CREATE FOREIGN TABLE programme_externe.structure_pnf
(
  id_structure integer,
  nom_structure text,
  detail_nom_structure text,
  statut text,
  adresse_1 text,
  code_postal text,
  ville text,
  pays text,
  tel text,
  fax text,
  courriel_1 text,
  courriel_2 text,
  site_web text,
  remarque text,
  createur integer,
  diffusable boolean,
  date_maj date
 )
 SERVER pnf_svr
OPTIONS (table_name 'structure', schema_name 'md');
;

DROP TABLE IF EXISTS programme_externe.structure;
CREATE TABLE programme_externe.structure AS
SELECT * FROM programme_externe.structure_pnf;

-- table des personnes
ALTER TABLE occtax.personne ADD COLUMN IF NOT EXISTS identifiant_origine INTEGER;

INSERT INTO occtax.personne
(identite, mail, organisme, prenom, nom, anonymiser, identifiant_origine)
SELECT DISTINCT
    TRIM(
    CONCAT(
        UPPER(TRIM(p.nom)),
        ' ',
        TRIM(p.prenom)
    )) AS identite,
    p.email,
    CASE
        WHEN nom_structure IN ('PERSO') THEN 'Indépendant'
        ELSE nom_structure
    END AS organisme,
    TRIM(p.prenom),
    UPPER(TRIM(p.nom)),
    False AS anonymiser,
    p.id_personne
FROM programme_externe.personne AS p
INNER JOIN programme_externe.structure AS s ON s.id_structure = p.id_structure
LEFT JOIN occtax.personne op
ON UPPER(TRIM(p.nom)) = op.nom AND TRIM(p.prenom) = op.prenom
WHERE op.id_personne IS NULL
ON CONFLICT DO NOTHING
;


-- table qui stocke les id personnes d'origine et conserve le lien avec le id_personne de la table occtax.personne
DROP TABLE IF EXISTS programme_externe.png_lien_personne_occtax;
CREATE TABLE IF NOT EXISTS programme_externe.png_lien_personne_occtax (
        id_personne_pnf integer,
        id_personne_occtax integer
);
TRUNCATE TABLE programme_externe.png_lien_personne_occtax;

INSERT INTO programme_externe.png_lien_personne_occtax
(id_personne_pnf, id_personne_occtax)
SELECT
pp.id_personne,
op.id_personne
FROM occtax.personne AS op
INNER JOIN programme_externe.personne pp
ON UPPER(TRIM(pp.nom)) = op.nom AND TRIM(pp.prenom) = op.prenom
;

-- table des relations entre les observations et les personnes
-- Le PNF a 2 tables : saisie_observation pour les obs, personne pour les personnes
-- Dans le champ observateur de saisie_observation, il y a la concatÃ©natino des id_personne, par exemple : 23&45
DROP TABLE IF EXISTS programme_externe.pnf_lien_observation_personne;
CREATE TABLE IF NOT EXISTS programme_externe.pnf_lien_observation_personne (
        id_obs integer,
        id_personne integer
);
TRUNCATE TABLE programme_externe.pnf_lien_observation_personne;

INSERT INTO programme_externe.pnf_lien_observation_personne (id_obs, id_personne)
SELECT id_obs, unnest( string_to_array( observateur, '&' ) )::integer AS id_personne
FROM programme_externe.obsocc_pnf_dur AS s;
-- pour la finalisation, voir import: fichier 3 car cela concerne les observations


-- Tables des lieux-dit
DROP TABLE IF EXISTS programme_externe.lieu_dit;
CREATE FOREIGN TABLE IF NOT EXISTS programme_externe.lieu_dit (
        id text,
        nom text,
        geometrie geometry
)
SERVER pnf_svr
OPTIONS (table_name 'lieu_dit', schema_name 'ign_bd_topo');

DROP TABLE IF EXISTS programme_externe.lieu_dit_dur;
CREATE TABLE programme_externe.lieu_dit_dur AS
SELECT * FROM programme_externe.lieu_dit;

CREATE INDEX ON programme_externe.lieu_dit_dur (id);


COMMIT;
