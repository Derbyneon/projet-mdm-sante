-- ============================================================
-- CRÉATION DE LA BASE DE DONNÉES MDM CLINIQUE (Version 2.1 - Simplifiée)
-- ============================================================
-- Connexion: psql -U mdm_user -d mdm_clinique

-- Création du schéma MDM
CREATE SCHEMA IF NOT EXISTS mdm;

-- ============================================================
-- TABLE 1: MDM_PATIENT
-- ============================================================
-- Référentiel maître des patients

DROP TABLE IF EXISTS mdm.mdm_patient CASCADE;

CREATE TABLE mdm.mdm_patient (
    -- Clé primaire MDM
    patient_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Identifiants sources (Traçabilité)
    patient_id_rdv VARCHAR(50),  -- ID du système Rendez-vous
    patient_id_erp VARCHAR(50),  -- ID du système ERP/Labo
    patient_id_fact VARCHAR(50), -- ID du système Facturation
    
    -- Données démographiques (Golden Record)
    nom VARCHAR(100) NOT NULL,
    prenom VARCHAR(100) NOT NULL,
    date_naissance DATE NOT NULL,
    sexe VARCHAR(10) CHECK (sexe IN ('Masculin', 'Féminin')),
    
    -- Données contact
    adresse TEXT,
    telephone VARCHAR(20),
    email VARCHAR(255),
    
    -- Données médicales simplifiées
    num_dossier VARCHAR(50) UNIQUE,
    historique_medical TEXT, -- Antécédents, allergies
    
    -- Métadonnées MDM
    date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_derniere_maj TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    score_qualite DECIMAL(5,2) DEFAULT 0, -- Score de 0-100
    statut VARCHAR(20) DEFAULT 'Actif' CHECK (statut IN ('Actif', 'Inactif', 'Décédé'))
);

-- Index
CREATE INDEX idx_patient_nom_prenom ON mdm.mdm_patient(nom, prenom);
CREATE INDEX idx_patient_date_naissance ON mdm.mdm_patient(date_naissance);
CREATE INDEX idx_patient_num_dossier ON mdm.mdm_patient(num_dossier);

-- Commentaires
COMMENT ON TABLE mdm.mdm_patient IS 'Référentiel Maître des Patients - Vue unique consolidée';
COMMENT ON COLUMN mdm.mdm_patient.patient_id IS 'Identifiant unique MDM (Golden Record)';
COMMENT ON COLUMN mdm.mdm_patient.patient_id_rdv IS 'ID du patient dans le système de RDV';
COMMENT ON COLUMN mdm.mdm_patient.score_qualite IS 'Score de complétude des données (0-100)';
COMMENT ON COLUMN mdm.mdm_patient.historique_medical IS 'Antécédents médicaux, allergies - DONNÉES SENSIBLES';


-- ============================================================
-- TABLE 2: MDM_MEDECIN
-- ============================================================
-- Référentiel maître des médecins

DROP TABLE IF EXISTS mdm.mdm_medecin CASCADE;

CREATE TABLE mdm.mdm_medecin (
    -- Clé primaire MDM
    medecin_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Identifiants sources
    medecin_id_rdv VARCHAR(50),
    medecin_id_erp VARCHAR(50),
    
    -- Identité
    nom VARCHAR(100) NOT NULL,
    prenom VARCHAR(100) NOT NULL,
    
    -- Informations professionnelles
    specialite VARCHAR(100) NOT NULL,
    num_licence VARCHAR(50) UNIQUE, -- Numéro de licence (ex: RPPS)
    
    -- Contact
    email VARCHAR(255),
    telephone VARCHAR(20),
    
    -- Relation (sera liée à mdm_service)
    service_id UUID, 
    
    -- Disponibilité (simplifié)
    disponibilite VARCHAR(255), -- Ex: "Lundi, Mardi (8h-17h)"
    
    -- Métadonnées MDM
    date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_derniere_maj TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    statut VARCHAR(20) DEFAULT 'Actif' CHECK (statut IN ('Actif', 'Inactif', 'Congé'))
);

-- Index
CREATE INDEX idx_medecin_nom_prenom ON mdm.mdm_medecin(nom, prenom);
CREATE INDEX idx_medecin_specialite ON mdm.mdm_medecin(specialite);
CREATE INDEX idx_medecin_num_licence ON mdm.mdm_medecin(num_licence);

-- Commentaires
COMMENT ON TABLE mdm.mdm_medecin IS 'Référentiel Maître des Médecins et Praticiens';
COMMENT ON COLUMN mdm.mdm_medecin.num_licence IS 'Numéro de licence professionnelle (ex: RPPS)';
COMMENT ON COLUMN mdm.mdm_medecin.disponibilite IS 'Horaires de disponibilité (format texte)';


-- ============================================================
-- TABLE 3: MDM_SERVICE
-- ============================================================
-- Référentiel maître des services

DROP TABLE IF EXISTS mdm.mdm_service CASCADE;

CREATE TABLE mdm.mdm_service (
    -- Clé primaire MDM
    service_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Identifiant source
    service_id_erp VARCHAR(50),
    
    -- Identification
    nom_service VARCHAR(100) UNIQUE NOT NULL,
    description TEXT,
    
    -- Responsabilité (sera liée à mdm_medecin)
    responsable_id UUID,
    
    -- Localisation
    localisation VARCHAR(255), -- Ex: "Bâtiment A, 3ème étage"
    
    -- Horaires (simplifié)
    horaires VARCHAR(255), -- Ex: "Lundi-Vendredi (8h-18h)"
    
    -- Métadonnées MDM
    date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_derniere_maj TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    actif BOOLEAN DEFAULT TRUE
);

-- Index
CREATE INDEX idx_service_nom ON mdm.mdm_service(nom_service);
CREATE INDEX idx_service_responsable ON mdm.mdm_service(responsable_id);

-- Commentaires
COMMENT ON TABLE mdm.mdm_service IS 'Référentiel Maître des Services et Départements';
COMMENT ON COLUMN mdm.mdm_service.responsable_id IS 'Médecin chef de service (FK vers mdm_medecin)';


-- ============================================================
-- AJOUT DES FOREIGN KEYS (Relations MDM)
-- ============================================================

-- Relation Médecin → Service
ALTER TABLE mdm.mdm_medecin
ADD CONSTRAINT fk_medecin_service 
FOREIGN KEY (service_id) 
REFERENCES mdm.mdm_service(service_id)
ON DELETE SET NULL; -- Si le service est supprimé, le médecin reste mais sans service

-- Relation Service → Médecin (responsable)
ALTER TABLE mdm.mdm_service
ADD CONSTRAINT fk_service_responsable
FOREIGN KEY (responsable_id)
REFERENCES mdm.mdm_medecin(medecin_id)
ON DELETE SET NULL; -- Si le médecin est supprimé, le service reste mais sans responsable


-- ============================================================
-- VUE: Vue 360° Patient
-- ============================================================

CREATE OR REPLACE VIEW mdm.v_patient_360 AS
SELECT 
    p.patient_id,
    p.nom,
    p.prenom,
    p.date_naissance,
    -- Calcul de l'âge
    EXTRACT(YEAR FROM AGE(p.date_naissance)) as age, 
    p.sexe,
    p.telephone,
    p.email,
    p.adresse,
    p.num_dossier,
    p.historique_medical,
    p.score_qualite,
    p.statut,
    p.date_derniere_maj
FROM mdm.mdm_patient p
WHERE p.statut = 'Actif';

COMMENT ON VIEW mdm.v_patient_360 IS 'Vue enrichie du patient actif avec calcul âge';


-- ============================================================
-- VUE: Vue Médecins par Service
-- ============================================================
-- Similaire à votre exemple, très utile

CREATE OR REPLACE VIEW mdm.v_medecins_par_service AS
SELECT 
    s.service_id,
    s.nom_service,
    s.localisation,
    s.horaires,
    -- Compte le nombre de médecins
    COUNT(m.medecin_id) AS nombre_medecins_actifs,
    -- Agrège la liste des médecins
    STRING_AGG(m.nom || ' ' || m.prenom, ', ' ORDER BY m.nom) AS liste_medecins,
    -- Ajoute le nom du responsable
    resp.nom || ' ' || resp.prenom AS nom_responsable
FROM mdm.mdm_service s
-- Jointure pour les médecins du service
LEFT JOIN mdm.mdm_medecin m ON s.service_id = m.service_id AND m.statut = 'Actif'
-- Jointure pour le nom du responsable
LEFT JOIN mdm.mdm_medecin resp ON s.responsable_id = resp.medecin_id
WHERE s.actif = TRUE
GROUP BY s.service_id, resp.nom, resp.prenom;

COMMENT ON VIEW mdm.v_medecins_par_service IS 'Agrégation des médecins actifs par service';


-- ============================================================
-- FONCTION: Calcul du score de qualité (Simplifié)
-- ============================================================
-- Calcule un score de complétude simple

CREATE OR REPLACE FUNCTION mdm.calculer_score_qualite_patient(p_patient_id UUID)
RETURNS DECIMAL(5,2) AS $$
DECLARE
    v_score DECIMAL(5,2) := 0;
    v_nb_champs_total INT := 10; -- Nombre total de champs à vérifier
    v_nb_champs_remplis INT := 0;
BEGIN
    SELECT 
        (CASE WHEN nom IS NOT NULL AND nom <> '' THEN 1 ELSE 0 END +
         CASE WHEN prenom IS NOT NULL AND prenom <> '' THEN 1 ELSE 0 END +
         CASE WHEN date_naissance IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN sexe IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN adresse IS NOT NULL AND adresse <> '' THEN 1 ELSE 0 END +
         CASE WHEN telephone IS NOT NULL AND telephone <> '' THEN 1 ELSE 0 END +
         CASE WHEN email IS NOT NULL AND email <> '' THEN 1 ELSE 0 END +
         CASE WHEN num_dossier IS NOT NULL AND num_dossier <> '' THEN 1 ELSE 0 END +
         CASE WHEN historique_medical IS NOT NULL AND historique_medical <> '' THEN 1 ELSE 0 END +
         CASE WHEN statut IS NOT NULL THEN 1 ELSE 0 END)
    INTO v_nb_champs_remplis
    FROM mdm.mdm_patient
    WHERE patient_id = p_patient_id;
    
    -- Calcul simple de complétude
    IF v_nb_champs_total > 0 THEN
        v_score := (v_nb_champs_remplis::DECIMAL / v_nb_champs_total) * 100;
    END IF;
    
    RETURN ROUND(v_score, 2);
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION mdm.calculer_score_qualite_patient IS 'Calcule un score de complétude (0-100) pour un patient';


-- ============================================================
-- TRIGGERS (Déclencheurs)
-- ============================================================

-- Trigger 1: Mettre à jour 'date_derniere_maj'
CREATE OR REPLACE FUNCTION mdm.trigger_update_date_maj()
RETURNS TRIGGER AS $$
BEGIN
    NEW.date_derniere_maj := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Appliquer le trigger aux 3 tables
CREATE TRIGGER trg_patient_update_date
    BEFORE UPDATE ON mdm.mdm_patient
    FOR EACH ROW
    EXECUTE FUNCTION mdm.trigger_update_date_maj();

CREATE TRIGGER trg_medecin_update_date
    BEFORE UPDATE ON mdm.mdm_medecin
    FOR EACH ROW
    EXECUTE FUNCTION mdm.trigger_update_date_maj();

CREATE TRIGGER trg_service_update_date
    BEFORE UPDATE ON mdm.mdm_service
    FOR EACH ROW
    EXECUTE FUNCTION mdm.trigger_update_date_maj();

-- Trigger 2: Calculer le score de qualité
CREATE OR REPLACE FUNCTION mdm.trigger_calc_score_qualite()
RETURNS TRIGGER AS $$
BEGIN
    -- Appelle la fonction de calcul simplifiée
    NEW.score_qualite := mdm.calculer_score_qualite_patient(NEW.patient_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_patient_calc_score
    BEFORE INSERT OR UPDATE ON mdm.mdm_patient
    FOR EACH ROW
    EXECUTE FUNCTION mdm.trigger_calc_score_qualite();


-- ============================================================
-- SÉCURITÉ ET PERMISSIONS (Simplifié)
-- ============================================================

-- Rôle: Consommateurs MDM (lecture seule)
CREATE ROLE mdm_consumer;
GRANT USAGE ON SCHEMA mdm TO mdm_consumer;
GRANT SELECT ON ALL TABLES IN SCHEMA mdm TO mdm_consumer;

-- Rôle: ETL (lecture + écriture)
CREATE ROLE mdm_etl;
GRANT USAGE ON SCHEMA mdm TO mdm_etl;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA mdm TO mdm_etl;


-- ============================================================
-- VÉRIFICATION FINALE
-- ============================================================

SELECT 'Script MDM (v2.1) exécuté avec succès!' AS status,
       (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'mdm' AND table_type = 'BASE TABLE') AS nb_tables,
       (SELECT COUNT(*) FROM information_schema.views WHERE table_schema = 'mdm') AS nb_vues;