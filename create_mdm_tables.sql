-- ============================================================
-- CRÉATION DE LA BASE DE DONNÉES MDM CLINIQUE (VERSION MISE À JOUR)
-- ============================================================
-- Connexion: psql -U mdm_user -d mdm_clinique

-- Création du schéma MDM
CREATE SCHEMA IF NOT EXISTS mdm;

-- ============================================================
-- TABLE 1: MDM_PATIENT (Selon Data Catalogue)
-- ============================================================
DROP TABLE IF EXISTS mdm.mdm_patient CASCADE;

CREATE TABLE mdm.mdm_patient (
    -- Clé primaire MDM
    patient_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Données démographiques
    nom VARCHAR(100) NOT NULL,
    prenom VARCHAR(100) NOT NULL,
    date_naissance DATE NOT NULL,
    sexe VARCHAR(10) CHECK (sexe IN ('M', 'F', 'Masculin', 'Féminin')),
    
    -- Adresse résidentielle
    adresse TEXT,
    
    -- Contact
    telephone VARCHAR(20),
    email VARCHAR(255),
    
    -- Dossier patient
    num_dossier VARCHAR(50) UNIQUE,
    
    -- Historique médical (antécédents, allergies)
    historique_medical TEXT,
    
    -- Métadonnées MDM
    date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour améliorer les performances
CREATE INDEX idx_patient_nom ON mdm.mdm_patient(nom);
CREATE INDEX idx_patient_prenom ON mdm.mdm_patient(prenom);
CREATE INDEX idx_patient_date_naissance ON mdm.mdm_patient(date_naissance);

COMMENT ON TABLE mdm.mdm_patient IS 'Référentiel Maître des Patients - Vue unique et dédupliquée';
COMMENT ON COLUMN mdm.mdm_patient.patient_id IS 'Identifiant unique du patient partagé, tableau de bord BI';
COMMENT ON COLUMN mdm.mdm_patient.nom IS 'Nom de famille du patient';
COMMENT ON COLUMN mdm.mdm_patient.prenom IS 'Prénom du patient';
COMMENT ON COLUMN mdm.mdm_patient.date_naissance IS 'Date de naissance';
COMMENT ON COLUMN mdm.mdm_patient.sexe IS 'Sexe biologique du patient (M/F)';
COMMENT ON COLUMN mdm.mdm_patient.historique_medical IS 'Historique médical du patient';
COMMENT ON COLUMN mdm.mdm_patient.date_creation IS 'Date de création de l''enregistrement patient';


-- ============================================================
-- TABLE 2: MDM_MEDECIN (Selon Data Catalogue)
-- ============================================================
DROP TABLE IF EXISTS mdm.mdm_medecin CASCADE;

CREATE TABLE mdm.mdm_medecin (
    -- Clé primaire MDM
    medecin_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Identité
    nom VARCHAR(100) NOT NULL,
    prenom VARCHAR(100) NOT NULL,
    
    -- Spécialité médicale
    specialite VARCHAR(100) NOT NULL,
    
    -- Relation avec service
    service_id UUID,
    
    -- Informations professionnelles
    num_licence VARCHAR(50) UNIQUE,
    
    -- Disponibilité
    disponibilite VARCHAR(50),
    
    -- Contact
    email VARCHAR(255),
    telephone VARCHAR(20)
);

CREATE INDEX idx_medecin_nom ON mdm.mdm_medecin(nom);
CREATE INDEX idx_medecin_prenom ON mdm.mdm_medecin(prenom);
CREATE INDEX idx_medecin_specialite ON mdm.mdm_medecin(specialite);
CREATE INDEX idx_medecin_service ON mdm.mdm_medecin(service_id);
CREATE INDEX idx_medecin_num_licence ON mdm.mdm_medecin(num_licence);

COMMENT ON TABLE mdm.mdm_medecin IS 'Référentiel Maître des Médecins et Praticiens';
COMMENT ON COLUMN mdm.mdm_medecin.medecin_id IS 'Identifiant unique du médecin';
COMMENT ON COLUMN mdm.mdm_medecin.nom IS 'Nom de famille du médecin';
COMMENT ON COLUMN mdm.mdm_medecin.prenom IS 'Prénom du médecin';
COMMENT ON COLUMN mdm.mdm_medecin.specialite IS 'Spécialité médicale (cardio, dermato, etc.)';
COMMENT ON COLUMN mdm.mdm_medecin.service_id IS 'Référence au service où le médecin exerce';
COMMENT ON COLUMN mdm.mdm_medecin.num_licence IS 'Numéro de licence professionnelle du médecin';
COMMENT ON COLUMN mdm.mdm_medecin.disponibilite IS 'Jours et horaires de disponibilité';
COMMENT ON COLUMN mdm.mdm_medecin.email IS 'Adresse e-mail du médecin';
COMMENT ON COLUMN mdm.mdm_medecin.telephone IS 'Numéro de téléphone du médecin';


-- ============================================================
-- TABLE 3: MDM_SERVICE (Selon spécifications utilisateur)
-- ============================================================
DROP TABLE IF EXISTS mdm.mdm_service CASCADE;

CREATE TABLE mdm.mdm_service (
    -- Clé primaire MDM
    service_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Nom du service
    nom_service VARCHAR(100) NOT NULL,
    
    -- Description du service
    description TEXT,
    
    -- Responsable du service (référence à un médecin)
    responsable_id UUID REFERENCES mdm.mdm_medecin(medecin_id),
    
    -- Localisation du service
    localisation VARCHAR(255),
    
    -- Horaires d'ouverture
    horaires VARCHAR(100),
    
    -- Date de création
    date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_service_nom ON mdm.mdm_service(nom_service);
CREATE INDEX idx_service_responsable ON mdm.mdm_service(responsable_id);

COMMENT ON TABLE mdm.mdm_service IS 'Référentiel Maître des Services et Départements';
COMMENT ON COLUMN mdm.mdm_service.service_id IS 'Identifiant unique du service';
COMMENT ON COLUMN mdm.mdm_service.nom_service IS 'Nom du service hospitalier';
COMMENT ON COLUMN mdm.mdm_service.description IS 'Description détaillée du service';
COMMENT ON COLUMN mdm.mdm_service.responsable_id IS 'Référence au médecin responsable du service';
COMMENT ON COLUMN mdm.mdm_service.localisation IS 'Localisation physique du service';
COMMENT ON COLUMN mdm.mdm_service.horaires IS 'Horaires d\'ouverture du service';
COMMENT ON COLUMN mdm.mdm_service.date_creation IS 'Date de création du service';


-- ============================================================
-- AJOUT DES CONTRAINTES DE CLÉ ÉTRANGÈRE (RELATIONS)
-- ============================================================

-- Relation: Médecin → Service
ALTER TABLE mdm.mdm_medecin
ADD CONSTRAINT fk_medecin_service
FOREIGN KEY (service_id)
REFERENCES mdm.mdm_service(service_id)
ON DELETE SET NULL;

-- Relation: Service → Médecin (responsable)
ALTER TABLE mdm.mdm_service
ADD CONSTRAINT fk_service_responsable_medecin
FOREIGN KEY (responsable_id)
REFERENCES mdm.mdm_medecin(medecin_id)
ON DELETE SET NULL;


-- ============================================================
-- VUE: Vue 360° Patient
-- ============================================================

CREATE OR REPLACE VIEW mdm.v_patient_360 AS
SELECT 
    p.patient_id,
    p.nom AS patient_nom,
    p.prenom AS patient_prenom,
    p.date_naissance,
    p.sexe,
    p.historique_medical,
    p.date_creation
FROM mdm.mdm_patient p;

COMMENT ON VIEW mdm.v_patient_360 IS 'Vue simplifiée du patient';


-- ============================================================
-- VUE: Vue Médecins par Service
-- ============================================================

CREATE OR REPLACE VIEW mdm.v_medecins_par_service AS
SELECT 
    s.service_id,
    s.nom_service,
    s.description,
    s.localisation,
    s.horaires,
    s.date_creation,
    COUNT(m.medecin_id) AS nombre_medecins,
    STRING_AGG(m.nom || ' ' || m.prenom, ', ' ORDER BY m.nom, m.prenom) AS liste_medecins,
    resp.nom || ' ' || resp.prenom AS responsable_nom
FROM mdm.mdm_service s
LEFT JOIN mdm.mdm_medecin m ON s.service_id = m.service_id
LEFT JOIN mdm.mdm_medecin resp ON s.responsable_id = resp.medecin_id
GROUP BY s.service_id, s.nom_service, s.description, s.localisation, s.horaires, s.date_creation, resp.nom, resp.prenom;

COMMENT ON VIEW mdm.v_medecins_par_service IS 'Agrégation des médecins par service';


-- ============================================================
-- FONCTION: Calcul du score de qualité Patient
-- ============================================================

CREATE OR REPLACE FUNCTION mdm.calculer_score_qualite_patient(p_patient_id UUID)
RETURNS DECIMAL(5,2) AS $$
DECLARE
    v_score DECIMAL(5,2) := 0;
    v_completude DECIMAL(5,2) := 0;
    v_nb_champs_total INT := 9;
    v_nb_champs_remplis INT := 0;
BEGIN
    SELECT 
        (CASE WHEN nom IS NOT NULL AND nom <> '' THEN 1 ELSE 0 END +
         CASE WHEN prenom IS NOT NULL AND prenom <> '' THEN 1 ELSE 0 END +
         CASE WHEN date_naissance IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN sexe IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN historique_medical IS NOT NULL AND historique_medical <> '' THEN 1 ELSE 0 END +
         CASE WHEN num_dossier IS NOT NULL AND num_dossier <> '' THEN 1 ELSE 0 END +
         CASE WHEN telephone IS NOT NULL AND telephone <> '' THEN 1 ELSE 0 END +
         CASE WHEN email IS NOT NULL AND email <> '' THEN 1 ELSE 0 END +
         CASE WHEN adresse IS NOT NULL AND adresse <> '' THEN 1 ELSE 0 END)
    INTO v_nb_champs_remplis
    FROM mdm.mdm_patient
    WHERE patient_id = p_patient_id;
    
    v_completude := (v_nb_champs_remplis::DECIMAL / v_nb_champs_total) * 100;
    v_score := v_completude;
    
    RETURN ROUND(v_score, 2);
END;
$$ LANGUAGE plpgsql;


-- ============================================================
-- GRANTS (Sécurité)
-- ============================================================

CREATE ROLE mdm_consumer;
GRANT USAGE ON SCHEMA mdm TO mdm_consumer;
GRANT SELECT ON ALL TABLES IN SCHEMA mdm TO mdm_consumer;

CREATE ROLE mdm_etl;
GRANT USAGE ON SCHEMA mdm TO mdm_etl;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA mdm TO mdm_etl;


-- ============================================================
-- CONFIRMATION
-- ============================================================

SELECT 'Tables MDM créées avec succès!' AS status,
       COUNT(*) AS nombre_tables
FROM information_schema.tables
WHERE table_schema = 'mdm';
