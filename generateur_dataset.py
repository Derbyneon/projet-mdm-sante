"""
Générateur de datasets sources pour la clinique - VERSION MISE À JOUR
Conforme aux spécifications MDM de la prof
Simule 3 systèmes sources : Rendez-vous, Laboratoire/ERP, Facturation
"""

import pandas as pd
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker('fr_FR')
random.seed(42)

# ============================================================
# SYSTÈME 1 : GESTION DES RENDEZ-VOUS
# ============================================================
print("Génération : Système Rendez-vous...")

# Liste de médecins avec variations de noms (problème qualité)
medecins_base = [
    {"nom": "Dubois", "prenom": "Marie", "specialite": "Cardiologie"},
    {"nom": "Martin", "prenom": "Jean", "specialite": "Pédiatrie"},
    {"nom": "Bernard", "prenom": "Sophie", "specialite": "Radiologie"},
    {"nom": "Petit", "prenom": "Pierre", "specialite": "Chirurgie"},
    {"nom": "Leroy", "prenom": "Émilie", "specialite": "Gynécologie"},
    {"nom": "Moreau", "prenom": "Laurent", "specialite": "Urgences"},
]

# Générer 80 patients uniques
patients_uniques = []
for i in range(80):
    sexe_biologique = random.choice(['Masculin', 'Féminin'])
    patients_uniques.append({
        'patient_id_rdv': f'RDV-P-{i+1:04d}',
        'nom': fake.last_name().upper(),
        'prenom': fake.first_name_male() if sexe_biologique == 'Masculin' else fake.first_name_female(),
        'sexe': sexe_biologique,
        'telephone': fake.phone_number(),
        'adresse': fake.address().replace('\n', ', ')
    })

# Ajouter 20 doublons avec variations (problème de qualité)
doublons = random.sample(patients_uniques, 20)
for patient in doublons:
    doublon = patient.copy()
    doublon['patient_id_rdv'] = f'RDV-P-{len(patients_uniques)+1:04d}'
    # Variations intentionnelles
    if random.random() > 0.5:
        doublon['nom'] = doublon['nom'].replace('E', 'É')
    if random.random() > 0.5:
        doublon['telephone'] = fake.phone_number()
    if random.random() > 0.3:
        doublon['adresse'] = fake.address().replace('\n', ', ')
    patients_uniques.append(doublon)

df_patients_rdv = pd.DataFrame(patients_uniques)

# Générer les rendez-vous
rendez_vous = []
for patient in patients_uniques:
    nb_rdv = random.randint(1, 5)
    for _ in range(nb_rdv):
        medecin = random.choice(medecins_base)
        date_rdv = datetime.now() - timedelta(days=random.randint(0, 365))
        rendez_vous.append({
            'rdv_id': f'RDV-{len(rendez_vous)+1:06d}',
            'patient_id': patient['patient_id_rdv'],
            'patient_nom': patient['nom'],
            'patient_prenom': patient['prenom'],
            'medecin_nom': medecin['nom'],
            'medecin_prenom': medecin['prenom'],
            'specialite': medecin['specialite'],
            'date_rdv': date_rdv.strftime('%Y-%m-%d'),
            'heure_rdv': f"{random.randint(8,18):02d}:{random.choice(['00','15','30','45'])}",
            'statut': random.choice(['Confirmé', 'Annulé', 'Terminé', 'En attente'])
        })

df_rdv = pd.DataFrame(rendez_vous)

# Médecins du système RDV (avec variations de format - problème!)
medecins_rdv = []
for med in medecins_base:
    medecins_rdv.append({
        'medecin_id_rdv': f'MED-RDV-{len(medecins_rdv)+1:03d}',
        'nom_complet': f"Dr. {med['prenom']} {med['nom']}",  # Format différent
        'specialite': med['specialite'],
        'telephone': fake.phone_number(),
        'email': f"{med['prenom'].lower()}.{med['nom'].lower()}@clinique-rdv.fr"
    })

df_medecins_rdv = pd.DataFrame(medecins_rdv)

# ============================================================
# SYSTÈME 2 : ERP (inclut Labo, Imagerie, Dossier Patient)
# ============================================================
print("Génération : Système ERP...")

erp_patients = []
dossiers_medicaux = []
analyses_labo = []

# Réutiliser certains patients avec d'autres IDs (PROBLÈME!)
for i, patient in enumerate(random.sample(patients_uniques, 60)):
    # Date de naissance (pas dans RDV, seulement dans ERP)
    date_naissance = fake.date_of_birth(minimum_age=1, maximum_age=90)
    
    erp_patient = {
        'patient_id_erp': f'ERP-P-{i+1:05d}',
        'nom': patient['nom'],
        'prenom': patient['prenom'],
        'date_naissance': date_naissance.strftime('%d/%m/%Y'),  # Format différent!
        'email': fake.email(),
        'adresse': patient['adresse'],
        'num_dossier': f'DOSS-{random.randint(100000, 999999)}'
    }
    erp_patients.append(erp_patient)
    
    # Dossier médical associé
    antecedents = random.sample([
        'Hypertension artérielle',
        'Diabète type 2',
        'Asthme',
        'Allergies (pénicilline)',
        'Cholestérol élevé',
        'Insuffisance rénale',
        'Aucun antécédent particulier'
    ], k=random.randint(1, 3))
    
    dossiers_medicaux.append({
        'num_dossier': erp_patient['num_dossier'],
        'patient_id_erp': erp_patient['patient_id_erp'],
        'historique_medical': ', '.join(antecedents),
        'allergies': random.choice(['Aucune', 'Pénicilline', 'Latex', 'Pollen', 'Arachides']),
        'date_creation_dossier': (datetime.now() - timedelta(days=random.randint(365, 3650))).strftime('%Y-%m-%d'),
        'medecin_referent': random.choice([m['nom'] for m in medecins_base])
    })
    
    # Analyses laboratoire pour ce patient
    nb_analyses = random.randint(1, 8)
    for _ in range(nb_analyses):
        date_analyse = datetime.now() - timedelta(days=random.randint(0, 730))
        analyses_labo.append({
            'analyse_id': f'LAB-{len(analyses_labo)+1:07d}',
            'patient_id_erp': erp_patient['patient_id_erp'],
            'num_dossier': erp_patient['num_dossier'],
            'type_analyse': random.choice([
                'Numération Formule Sanguine', 
                'Glycémie à jeun', 
                'Bilan lipidique complet', 
                'TSH (thyroïde)', 
                'Créatinine sanguine', 
                'Transaminases (ALAT/ASAT)',
                'CRP (inflammation)',
                'Hémoglobine glyquée (HbA1c)'
            ]),
            'date_prelevement': date_analyse.strftime('%d-%m-%Y'),  # Format différent!
            'date_resultat': (date_analyse + timedelta(days=random.randint(1, 5))).strftime('%d-%m-%Y'),
            'prescripteur_nom': random.choice([m['nom'] for m in medecins_base]),
            'service_realisation': random.choice(['Laboratoire Biochimie', 'Laboratoire Hématologie', 'Laboratoire Microbiologie']),
            'urgent': random.choice(['Oui', 'Non', 'OUI', 'NON'])  # Incohérence!
        })

df_patients_erp = pd.DataFrame(erp_patients)
df_dossiers = pd.DataFrame(dossiers_medicaux)
df_analyses = pd.DataFrame(analyses_labo)

# Médecins dans le système RH/ERP (données complètes)
medecins_erp = []
services = ['Cardiologie', 'Pédiatrie', 'Radiologie', 'Chirurgie', 'Gynécologie', 'Urgences']

for i, med in enumerate(medecins_base):
    medecins_erp.append({
        'medecin_id_erp': f'MED-ERP-{i+1:04d}',
        'nom': med['nom'].upper(),
        'prenom': med['prenom'],
        'specialite': med['specialite'],
        'num_licence': f'{random.randint(1,99):02d}{random.randint(1000000,9999999)}',  # Numéro RPPS fictif
        'email_pro': f"{med['prenom'].lower()}.{med['nom'].lower()}@clinique.fr",
        'telephone_pro': fake.phone_number(),
        'service_affecte': med['specialite'],  # Correspond au service
        'disponibilite_lundi': f"{random.randint(8,9)}:00-{random.randint(17,19)}:00",
        'disponibilite_mardi': f"{random.randint(8,9)}:00-{random.randint(17,19)}:00",
        'disponibilite_mercredi': f"{random.randint(8,9)}:00-{random.randint(17,19)}:00",
        'date_embauche': (datetime.now() - timedelta(days=random.randint(365, 7300))).strftime('%Y-%m-%d')
    })

df_medecins_erp = pd.DataFrame(medecins_erp)

# Services hospitaliers dans l'ERP
services_erp = []
for i, service_nom in enumerate(services):
    responsable = medecins_base[i] if i < len(medecins_base) else random.choice(medecins_base)
    services_erp.append({
        'service_id_erp': f'SRV-{i+1:03d}',
        'nom_service': service_nom,
        'description': f'Service de {service_nom.lower()} - Consultations et soins spécialisés',
        'responsable_nom': f'Dr. {responsable["prenom"]} {responsable["nom"]}',
        'localisation': f'Bâtiment {random.choice(["A", "B", "C"])}, Étage {random.randint(1,5)}',
        'horaires_ouverture_lundi': '08:00-18:00',
        'horaires_ouverture_mardi': '08:00-18:00',
        'horaires_ouverture_mercredi': '08:00-18:00',
        'horaires_ouverture_jeudi': '08:00-18:00',
        'horaires_ouverture_vendredi': '08:00-17:00',
        'telephone_service': fake.phone_number(),
        'email_service': f'{service_nom.lower().replace(" ", "")}@clinique.fr'
    })

df_services_erp = pd.DataFrame(services_erp)

# ============================================================
# SYSTÈME 3 : FACTURATION
# ============================================================
print("Génération : Système Facturation...")

facturation_patients = []
factures = []

# Encore d'autres IDs pour les mêmes patients! (PROBLÈME)
for i, patient in enumerate(random.sample(patients_uniques, 70)):
    fact_patient = {
        'id_patient_fact': f'FACT-P-{i+1:06d}',
        'nom_famille': patient['nom'],
        'prenoms': patient['prenom'],  # Colonne nommée différemment
        'tel_contact': patient['telephone'][:10],  # Seulement 10 chiffres
        'email_contact': fake.email(),
        'adresse_facturation': patient['adresse'],
    }
    facturation_patients.append(fact_patient)
    
    # Factures pour ce patient
    nb_factures = random.randint(1, 10)
    for _ in range(nb_factures):
        date_fact = datetime.now() - timedelta(days=random.randint(0, 500))
        medecin = random.choice(medecins_base)
        service = random.choice(services)
        
        montant_total = round(random.uniform(25, 1500), 2)
        montant_rembourse = round(montant_total * random.uniform(0.5, 0.9), 2)
        
        factures.append({
            'facture_id': f'F-{len(factures)+1:08d}',
            'patient_id': fact_patient['id_patient_fact'],
            'date_facture': date_fact.strftime('%Y/%m/%d'),  # Encore un format différent!
            'medecin_facturation': f"{medecin['prenom'][0]}. {medecin['nom']}",  # Format abrégé
            'service_facture': service,
            'type_prestation': random.choice(['Consultation', 'Analyse', 'Imagerie', 'Chirurgie', 'Urgence']),
            'montant_total_euros': montant_total,
            'montant_rembourse_euros': montant_rembourse,
            'montant_reste_charge': round(montant_total - montant_rembourse, 2),
            'statut_paiement': random.choice(['Payé', 'En attente', 'Remboursé', 'Impayé', 'PAYE', 'Payée']),  # Incohérence
            'mode_paiement': random.choice(['CB', 'Espèces', 'Chèque', 'Virement', None, ''])  # Valeurs nulles
        })

df_patients_fact = pd.DataFrame(facturation_patients)
df_factures = pd.DataFrame(factures)

# ============================================================
# SAUVEGARDE DES FICHIERS CSV
# ============================================================
print("\nSauvegarde des fichiers CSV...")

# Système Rendez-vous (3 fichiers)
df_patients_rdv.to_csv('source_rdv_patients.csv', index=False, encoding='utf-8-sig')
df_rdv.to_csv('source_rdv_consultations.csv', index=False, encoding='utf-8-sig')
df_medecins_rdv.to_csv('source_rdv_medecins.csv', index=False, encoding='utf-8-sig')

# Système ERP (5 fichiers)
df_patients_erp.to_csv('source_erp_patients.csv', index=False, encoding='utf-8-sig')
df_dossiers.to_csv('source_erp_dossiers_medicaux.csv', index=False, encoding='utf-8-sig')
df_analyses.to_csv('source_erp_analyses_labo.csv', index=False, encoding='utf-8-sig')
df_medecins_erp.to_csv('source_erp_medecins.csv', index=False, encoding='utf-8-sig')
df_services_erp.to_csv('source_erp_services.csv', index=False, encoding='utf-8-sig')

# Système Facturation (2 fichiers)
df_patients_fact.to_csv('source_fact_patients.csv', index=False, encoding='utf-8-sig')
df_factures.to_csv('source_fact_factures.csv', index=False, encoding='utf-8-sig')

print(f"""
✅ Génération terminée !

FICHIERS CRÉÉS (10 fichiers CSV) :
===================================

📁 SYSTÈME RENDEZ-VOUS (3 fichiers)
   - source_rdv_patients.csv ({len(df_patients_rdv)} lignes)
     Colonnes: patient_id_rdv, nom, prenom, sexe, telephone, adresse
   
   - source_rdv_consultations.csv ({len(df_rdv)} lignes)
     Données des rendez-vous médicaux
   
   - source_rdv_medecins.csv ({len(df_medecins_rdv)} lignes)
     Colonnes: medecin_id_rdv, nom_complet, specialite, telephone, email

📁 SYSTÈME ERP (5 fichiers)
   - source_erp_patients.csv ({len(df_patients_erp)} lignes)
     Colonnes: patient_id_erp, nom, prenom, date_naissance, email, adresse, num_dossier
   
   - source_erp_dossiers_medicaux.csv ({len(df_dossiers)} lignes)
     Colonnes: num_dossier, patient_id_erp, historique_medical, allergies
   
   - source_erp_analyses_labo.csv ({len(df_analyses)} lignes)
     Analyses de laboratoire
   
   - source_erp_medecins.csv ({len(df_medecins_erp)} lignes)
     Colonnes: medecin_id_erp, nom, prenom, specialite, num_licence, email_pro, 
               telephone_pro, service_affecte, disponibilite_*
   
   - source_erp_services.csv ({len(df_services_erp)} lignes)
     Colonnes: service_id_erp, nom_service, description, responsable_nom,
               localisation, horaires_*, telephone_service, email_service

📁 SYSTÈME FACTURATION (2 fichiers)
   - source_fact_patients.csv ({len(df_patients_fact)} lignes)
     Colonnes: id_patient_fact, nom_famille, prenoms, tel_contact, 
               email_contact, adresse_facturation
   
   - source_fact_factures.csv ({len(df_factures)} lignes)
     Factures détaillées

🔍 PROBLÈMES DE QUALITÉ SIMULÉS (conformes au projet MDM):
============================================================
✗ Doublons patients avec variations (accents, orthographe)
✗ IDs différents pour les mêmes patients dans les 3 systèmes:
  - RDV-P-XXXX (Rendez-vous)
  - ERP-P-XXXXX (ERP)
  - FACT-P-XXXXXX (Facturation)
✗ Formats de dates incohérents:
  - RDV: YYYY-MM-DD
  - ERP: DD/MM/YYYY
  - FACT: YYYY/MM/DD
✗ Noms de colonnes différents:
  - nom vs nom_famille
  - prenom vs prenoms
  - telephone vs tel_contact
✗ Formats médecins incohérents:
  - "Dr. Prénom Nom" (RDV)
  - "NOM Prénom" (ERP)
  - "P. Nom" (Facturation)
✗ Valeurs nulles et manquantes
✗ Casse incohérente (Payé/PAYE/Payée)

MAPPING VERS LES MDM :
======================

MDM_PATIENT sources:
  - Nom, Prénom, Sexe, Téléphone, Adresse → source_rdv_patients
  - Date_Naissance, Email, Num_Dossier → source_erp_patients
  - Historique_Médical → source_erp_dossiers_medicaux
  - (Données supplémentaires) → source_fact_patients

MDM_MEDECIN sources:
  - Données partielles → source_rdv_medecins
  - Nom, Prénom, Spécialité, Num_Licence, Email, Téléphone,
    Service_ID, Disponibilité → source_erp_medecins

MDM_SERVICE sources:
  - Nom_Service, Description, Responsable_ID, Localisation,
    Horaires → source_erp_services

➡️  Ces datasets sont prêts pour démontrer la NÉCESSITÉ du MDM!
""")

print("\n📊 STATISTIQUES DES DONNÉES :")
print(f"Total patients uniques (réalité) : ~80")
print(f"Total enregistrements patients (3 systèmes) : {len(df_patients_rdv) + len(df_patients_erp) + len(df_patients_fact)}")
print(f"Total médecins : {len(medecins_base)} (avec {len(df_medecins_rdv) + len(df_medecins_erp)} enregistrements)")
print(f"Total services : {len(df_services_erp)}")
print(f"Total rendez-vous : {len(df_rdv)}")
print(f"Total analyses labo : {len(df_analyses)}")
print(f"Total factures : {len(df_factures)}")
print(f"Total dossiers médicaux : {len(df_dossiers)}")