BEGIN;

SET search_path TO taxon,occtax,gestion,sig,public;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1/ Lancer 1_obsocc_import_source.sql

-- 2/ Lancer 2_obsocc_import_taxon.sql --> PAS BESOIN ICI

-- 3/ Import des données dans occtax:


-- Suppression des données avant réimport
SELECT count(*) FROM observation WHERE jdd_id = 'PNG-obsocc';
DELETE FROM occtax.observation WHERE jdd_id = 'PNG-obsocc';


INSERT INTO occtax.jdd
    (jdd_id, jdd_code, jdd_description, jdd_metadonnee_dee_id, jdd_cadre)
SELECT
    'PNG-obsocc',
    'Observations occasionnelles',
    'Jeu de données "Observations occasionnelles" du Parc National de Guadeloupe',
    CAST(uuid_generate_v4() AS text),
    'obsocc'
WHERE NOT EXISTS (SELECT jdd_id FROM occtax.jdd WHERE jdd_id = 'PNG-obsocc');

SELECT count(*) FROM observation WHERE jdd_id = 'PNG-obsocc';
DELETE FROM occtax.observation WHERE jdd_id = 'PNG-obsocc';
SELECT Setval('occtax.observation_cle_obs_seq', (SELECT max(cle_obs) FROM occtax.observation ) );

INSERT INTO occtax.observation
(
    cle_obs, identifiant_permanent, statut_observation,
    cd_nom, cd_ref, version_taxref, nom_cite,

    denombrement_min, denombrement_max, objet_denombrement, type_denombrement,

    commentaire,
    date_debut, date_fin, heure_debut, heure_fin, date_determination,
    altitude_min, altitude_moy, altitude_max, profondeur_min, profondeur_moy, profondeur_max,
    code_idcnp_dispositif,
    dee_date_derniere_modification, dee_date_transformation, dee_floutage,

    diffusion_niveau_precision, ds_publique, identifiant_origine,
    jdd_code, jdd_id, jdd_metadonnee_dee_id, jdd_source_id,organisme_gestionnaire_donnees, org_transformation,
    statut_source, reference_biblio,

    sensible, sensi_date_attribution, sensi_niveau, sensi_referentiel, sensi_version_referentiel,

    validite_niveau, validite_date_validation,

    descriptif_sujet,
    donnee_complementaire,

    precision_geometrie, nature_objet_geo, geom

)


SELECT 
    -- identifiants
    nextval('occtax.observation_cle_obs_seq'::regclass) AS cle_obs,

    CASE
        WHEN loip.identifiant_permanent IS NOT NULL THEN loip.identifiant_permanent
        ELSE CAST(uuid_generate_v4() AS text)
    END AS identifiant_permanent,

    CASE
        WHEN effectif IS NOT NULL AND effectif > 0 THEN 'Pr'
        ELSE 'No'
    END AS statut_observation,

    -- taxons
    -- cd_nom et cd_ref obtenus par jointure avec TAXREF sauf si non prÃ©sents dans TAXREF et nÃ©gatifs ( ie taxons locaux)
    CASE
        WHEN s.cd_nom::bigint < 0
        THEN s.cd_nom::bigint
        ELSE t.cd_ref
    END AS cd_nom, -- on conserve les cd_nom nÃ©gatifs, mais on remplace les autres par le correspndant valide taxref
    CASE
        WHEN s.cd_nom::bigint < 0
        THEN s.cd_nom::bigint
        ELSE t.cd_ref
    END AS cd_ref,
    '10.0' AS version_taxref,
    CASE
        WHEN s.nom_complet IS NULL THEN s.nom_vern
        ELSE s.nom_complet
    END AS nom_cite,

    -- denombrement
    CASE WHEN effectif_min > 0 THEN effectif_min ELSE Coalesce(effectif, effectif_max) END AS denombrement_min,
    CASE WHEN effectif_max > 0 THEN effectif_max ELSE Coalesce(effectif, effectif_min) END AS denombrement_max,
    CASE
            WHEN effectif IS NULL AND Coalesce(effectif_min, 0) = 0 AND Coalesce(effectif_max, 0) = 0 THEN NULL
            ELSE 'IND'::text
    END AS objet_denombrement,
    'NSP' AS type_denombrement,

    -- commentaires
    remarque_obs AS commentaire,

    -- dates
    CASE
        WHEN date_obs IS NULL THEN date_debut_obs::date
        ELSE date_obs::date
    END AS date_debut,
    CASE
        WHEN date_obs IS NULL THEN date_fin_obs::date
        ELSE date_obs::date
    END AS date_fin,
    heure_obs::time with time zone AS heure_debut,
    heure_obs::time with time zone AS heure_fin,

    -- pas de déterminateur, donc date NULL
    NULL::date as date_determination,

    -- localisation

    -- altitudes
    CASE
        WHEN elevation IS NOT NULL THEN elevation::numeric(6,2)
        ELSE NULL
    END as altitude_min,
    NULL::numeric AS altitude_moy,
    CASE
        WHEN elevation IS NOT NULL THEN elevation::numeric(6,2)
        ELSE NULL
    END as altitude_max,
    NULL::numeric AS profondeur_min,
    NULL::numeric AS profondeur_moy,
    NULL::numeric AS profondeur_max,

    -- source
    'obsocc' AS code_idcnp_dispositif,

    -- si le producteur nous indique des id des observations modifiées entre 2 imports, alors on met now() pour ces observations, sinon on reprend la date stockée dans le lien
    CASE
        WHEN loip.identifiant_permanent IS NOT NULL THEN loip.dee_date_derniere_modification
        ELSE now()
    END AS dee_date_derniere_modification,
    CASE
        WHEN loip.dee_date_transformation IS NOT NULL THEN loip.dee_date_transformation
        ELSE now()
    END AS dee_date_transformation,
    'NON' AS dee_floutage, -- nb : pas de données floutées en entrée dans le cadre du SINP

    '0' AS diffusion_niveau_precision,
    'Re' AS ds_publique,
    s.id_obs::text AS identifiant_origine,

    -- jdd
    j.jdd_code,
    j.jdd_id,
    j.jdd_metadonnee_dee_id,
    NULL AS jdd_source_id,


    'PNG' AS organisme_gestionnaire_donnees,
    'PNG' AS org_transformation,

    'Te' AS statut_source,
    NULL AS reference_biblio,

    -- sensibilite
    -- remplissage provisoire à ce stade car une fonction spécifique la calcule une fois l'import réalisé (cf. plus bas)
    CASE
        WHEN diffusable IS TRUE THEN 'OUI'
        ELSE 'NON'
    END AS sensible,

    CASE
        WHEN diffusable IS TRUE THEN NULL::date
        ELSE now()
    END AS sensi_date_attribution,
        
    CASE
        WHEN diffusable IS TRUE THEN '0'
        ELSE '4'
    END AS sensi_niveau,

    CASE
        WHEN diffusable IS TRUE THEN NULL
        ELSE 'PNG'
    END AS sensi_referentiel,

    CASE
        WHEN diffusable IS TRUE THEN NULL
        ELSE 'PNG V1.0'
    END AS sensi_version_referentiel,


    --validation
    CASE
        WHEN statut_validation = 'à valider' THEN '3'
        WHEN statut_validation = 'non valide' THEN '4'
        WHEN statut_validation IS NULL THEN '6'
        ELSE '1'
    END AS validite_niveau,

    -- ex:  'Décision GVL du 16/03/2016 à 14:43'

	CASE
		WHEN decision_validation NOT LIKE '%/%' THEN NULL 
		ELSE to_date( regexp_replace(decision_validation, E'.+ (\\d{2})/(\\d{2})/(\\d{4})?(.+)?', E'\\1/\\2/\\3'), 'DD/MM/YYYY') 
	END as validite_date_validation,
    
    
    -- descriptif sujet
    json_build_object(
        'obs_methode',
        CASE

            WHEN "determination" = 'Vu' THEN '0'
            WHEN "determination" = 'Entendu' THEN '1'
            WHEN "determination" = 'Indéterminé' THEN '21'
            WHEN "determination" = 'Indice de présence' THEN '20' --Autres
            WHEN "determination" = 'Cadavre' THEN '20'
            WHEN "determination" = 'Capture' THEN '20'
            ELSE '21' -- inconnu


        END,

        -- ETAT BIOLOGIQUE
        'occ_etat_biologique',
        CASE
            --WHEN s."HAS_DEATH_INFO" = 'Oui' THEN '3'
            --ELSE '2'
            WHEN "determination" = 'Vu' THEN '2' --observé vivant
            WHEN "determination" = 'Entendu' THEN '2' --observé vivant
            WHEN "determination" = 'Indéterminé' THEN '0' -- NSP
            WHEN "determination" = 'Indice de présence' THEN '0' --NSP
            WHEN "determination" = 'Cadavre' THEN '3'
            WHEN "determination" = 'Capture' THEN '2'
            ELSE '1' -- non renseigné

        END,

        'occ_naturalite', '0', --inconnu

        -- SEXE
        'occ_sexe',
        CASE
            WHEN lower(s."phenologie") = 'Femelle' THEN 2
            WHEN lower(s."phenologie") = 'Mâle' THEN 3
            WHEN lower(s."phenologie") = 'Indeterminé' THEN 1
            ELSE 0 -- Inconnu
        END,

        -- STADE DE VIE
        'occ_stade_de_vie',
        CASE
            WHEN s."type_effectif" = 'Adulte' THEN 2
            WHEN s."type_effectif" = 'Indeterminé' THEN 0
            WHEN s."type_effectif" = 'Juvénile' THEN 3
            WHEN s."type_effectif" = 'Oeuf/ponte' THEN 9
            ELSE 0 -- inconnu
        END,

        -- STATUT BIOGEOGRAPHIQUE
        'occ_statut_biogeographique', '1', -- non renseigné

        'occ_statut_biologique', '1',  -- non renseigné

        'preuve_existante', '0', -- nsp

        'preuve_numerique', NULL,

        'preuve_non_numerique', NULL,

        'obs_contexte', 'localisation',

        'obs_description', 'remarques_obs',

        'occ_methode_determination', NULL

    )::jsonb AS descriptif_sujet,

    json_build_object(
        'lieu-dit', ld.nom
    )::jsonb AS donnee_complementaire,


    -- geometrie
    CASE
        WHEN "precision" = '10 à 100m' THEN 100
        WHEN "precision" = '0 à 10m' THEN 10
        WHEN "precision" = '100 à 500m' THEN 500
        ELSE NULL
    END AS precision_geometrie,
    'St' AS nature_objet_geo,

    CASE
        WHEN s.geometrie IS NOT NULL THEN s.geometrie
        ELSE NULL
    END AS geom


FROM
-- jeu de donnees
(SELECT * FROM occtax.jdd WHERE jdd_id = 'PNG-obsocc') AS j,

-- table source
programme_externe.obsocc_pnf_dur AS s
LEFT JOIN programme_externe.lieu_dit_dur AS ld ON ld.id = s.id_lieu_dit
LEFT JOIN occtax.lien_observation_identifiant_permanent loip ON loip.jdd_id = 'PNG-obsocc' AND loip.identifiant_origine = CAST(s.id_obs AS text)
LEFT JOIN taxon.taxref t ON t.cd_nom = CAST( NULLIF( s.cd_nom, '') AS bigint )
WHERE True

;
SELECT count(*) FROM observation;

-- Vidage puis remplissage de lien_observation_identifiant_permanent pour garder en mÃ©moire les identifiants permanents en cas d'un rÃ©import futur
DELETE FROM occtax.lien_observation_identifiant_permanent WHERE jdd_id = 'PNG-obsocc';
INSERT INTO occtax.lien_observation_identifiant_permanent
(jdd_id, identifiant_origine, identifiant_permanent, dee_date_derniere_modification, dee_date_transformation)
SELECT o.jdd_id, o.identifiant_origine, o.identifiant_permanent, o.dee_date_derniere_modification, o.dee_date_transformation
FROM occtax.observation o
WHERE o.jdd_id = 'PNG-obsocc'
ORDER BY o.cle_obs
;


-- Personnes
-- table personne
-- déjà remplie dans la partie 1 sur l'import des données

-- Table observation_personne
-- SELECT count(*) FROM occtax.personne;
-- SELECT count(*) FROM occtax.observation_personne;
INSERT INTO occtax.observation_personne
(cle_obs, id_personne, role_personne)
SELECT
o.cle_obs,
pp.id_personne_occtax,
'Obs' AS role_personne
FROM occtax.observation AS o
INNER JOIN programme_externe.pnf_lien_observation_personne AS op ON op.id_obs::text = o.identifiant_origine
INNER JOIN programme_externe.png_lien_personne_occtax AS pp ON pp.id_personne_pnf = op.id_personne
;

-- -- attribut additionnel
-- TRUNCATE TABLE occtax.attribut_additionnel RESTART IDENTITY;
-- Laissé ici pour mémoire, mais commenté car mis dans occtax.descriptif_sujet
-- INSERT INTO occtax.attribut_additionnel
-- SELECT DISTINCT
--         o.cle_obs,
--         'détermination' AS parametre,
--         s.determination AS valeur
-- FROM programme_externe.obsocc_pnf_dur s
-- INNER JOIN occtax.observation o ON o.identifiant_origine = s.id_obs::text AND o.jdd_id = 'PNG-obsocc'
-- WHERE TRUE
-- AND determination IS NOT NULL;



-- Mise à jour de relations spatiales
SELECT occtax.occtax_update_spatial_relationships(
    ARRAY['PNG-obsocc']
);

-- Mise à jour des critères de sensibilité et de diffusion
--SELECT occtax.occtax_update_sensibilite_observations(
--    'Ref_provisoire_971',
--    '1.0',
--    'PNG-obsocc',
--    Array['CR', 'EN', 'RE', 'EW', 'NE', 'DD'],
--    ARRAY['EPN', 'EPC', 'EPI'],
    --NULL
--);
-- SELECT count(cle_obs) nb, sensi_niveau FROM observation GROUP BY sensi_niveau;
-- SELECT DISTINCT nom_cite FROM observation WHERE sensi_niveau = '4';

-- Mise à jour des critères de diffusion
REFRESH MATERIALIZED view occtax.observation_diffusion;

COMMIT;
