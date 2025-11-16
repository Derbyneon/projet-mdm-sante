# ============================================================
# ETL KAFKA MDM CLINIQUE - VERSION SIMPLIFIÉE (Sans Consumer Groups)
# ============================================================
# Solution : Lecture directe des partitions sans consumer groups
# ============================================================

import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import pandas as pd
import psycopg2
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import TopicPartition
import re
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
}

DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'mdm_clinique',
    'user': 'mdm_user',
    'password': 'mdm_password'
}

# Topics simples (pas de timestamp, on va les réutiliser)
TOPICS = ['mdm_patients', 'mdm_medecins', 'mdm_services']


# ============================================================
# PARTIE 1: CRÉATION/NETTOYAGE DES TOPICS
# ============================================================

def reset_kafka_topics():
    """Supprime et recrée les topics pour repartir à zéro"""
    logging.info("🔧 Réinitialisation des topics Kafka...")
    
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'])
        
        # Supprimer les anciens topics
        existing_topics = admin_client.list_topics()
        topics_to_delete = [t for t in TOPICS if t in existing_topics]
        
        if topics_to_delete:
            admin_client.delete_topics(topics_to_delete)
            logging.info(f"  🗑️  Suppression de {len(topics_to_delete)} anciens topics...")
            time.sleep(3)  # Attendre la suppression
        
        # Recréer les topics
        new_topics = [
            NewTopic(name=topic, num_partitions=1, replication_factor=1)
            for topic in TOPICS
        ]
        
        admin_client.create_topics(new_topics=new_topics, validate_only=False)
        logging.info(f"✅ {len(TOPICS)} topics créés : {TOPICS}")
        time.sleep(2)  # Laisser le temps à Kafka de s'initialiser
        
    except Exception as e:
        logging.error(f"❌ Erreur topics: {e}")


# ============================================================
# PARTIE 2: PRODUCER
# ============================================================

class DataProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            acks='all',  # Attendre confirmation d'écriture
            retries=3
        )
        logging.info("✅ Producer Kafka initialisé")
    
    def send_services_to_kafka(self):
        logging.info("\n📤 Envoi SERVICES...")
        try:
            df = pd.read_csv('source_erp_services.csv')
            for _, row in df.iterrows():
                msg = {'source': 'ERP', 'data': row.to_dict()}
                future = self.producer.send(TOPICS[2], value=msg)
                future.get(timeout=10)  # Attendre confirmation
            self.producer.flush()
            logging.info(f"✅ {len(df)} services envoyés")
        except Exception as e:
            logging.error(f"❌ Erreur envoi services: {e}")
    
    def send_medecins_to_kafka(self):
        logging.info("📤 Envoi MÉDECINS...")
        total = 0
        for source, file in [('ERP', 'source_erp_medecins.csv'), ('RDV', 'source_rdv_medecins.csv')]:
            try:
                df = pd.read_csv(file)
                for _, row in df.iterrows():
                    msg = {'source': source, 'data': row.to_dict()}
                    future = self.producer.send(TOPICS[1], value=msg)
                    future.get(timeout=10)
                    total += 1
                logging.info(f"  ✓ {len(df)} depuis {source}")
            except Exception as e:
                logging.error(f"  ✗ Erreur {file}: {e}")
        self.producer.flush()
        logging.info(f"✅ {total} médecins envoyés")
    
    def send_patients_to_kafka(self):
        logging.info("📤 Envoi PATIENTS...")
        total = 0
        
        # Charger dossiers médicaux
        try:
            df_dossiers = pd.read_csv('source_erp_dossiers_medicaux.csv')
            dossier_lookup = df_dossiers.set_index('num_dossier')['historique_medical'].to_dict()
        except:
            dossier_lookup = {}
        
        for source, file in [('RDV', 'source_rdv_patients.csv'), 
                             ('ERP', 'source_erp_patients.csv'), 
                             ('FACT', 'source_fact_patients.csv')]:
            try:
                df = pd.read_csv(file)
                for _, row in df.iterrows():
                    data = row.to_dict()
                    if source == 'ERP' and data.get('num_dossier'):
                        data['historique_medical'] = dossier_lookup.get(data['num_dossier'], '')
                    
                    msg = {'source': source, 'data': data}
                    future = self.producer.send(TOPICS[0], value=msg)
                    future.get(timeout=10)
                    total += 1
                logging.info(f"  ✓ {len(df)} depuis {source}")
            except Exception as e:
                logging.error(f"  ✗ Erreur {file}: {e}")
        
        self.producer.flush()
        logging.info(f"✅ {total} patients envoyés")
    
    def close(self):
        self.producer.close()


# ============================================================
# PARTIE 3: CONSUMER SIMPLIFIÉ (Lecture directe partition)
# ============================================================

def lire_messages_topic(topic: str) -> List[Dict]:
    """Lit TOUS les messages d'un topic (méthode directe)"""
    logging.info(f"📥 Lecture du topic '{topic}'...")
    
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=5000
    )
    
    # Assigner manuellement la partition 0
    partition = TopicPartition(topic, 0)
    consumer.assign([partition])
    
    # Aller au début
    consumer.seek_to_beginning(partition)
    
    messages = []
    try:
        for msg in consumer:
            messages.append(msg.value)
    except StopIteration:
        pass
    
    consumer.close()
    logging.info(f"✅ {len(messages)} messages lus")
    return messages


# ============================================================
# PARTIE 4: TRANSFORMATION
# ============================================================

class DataTransformer:
    @staticmethod
    def normaliser_nom(nom: str) -> str:
        if pd.isna(nom): return ''
        nom = str(nom).upper()
        for old, new in {'É':'E','È':'E','Ê':'E','À':'A','Ù':'U','Ô':'O','Ç':'C'}.items():
            nom = nom.replace(old, new)
        return nom.strip()
    
    @staticmethod
    def normaliser_date(date_str: str) -> Optional[str]:
        if pd.isna(date_str): return None
        date_str = str(date_str).strip()
        try:
            if '/' in date_str:
                parts = date_str.split('/')
                if len(parts[2]) == 4:  # DD/MM/YYYY
                    return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
                if len(parts[0]) == 4:  # YYYY/MM/DD
                    return f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
            if '-' in date_str and len(date_str) == 10:
                return date_str  # YYYY-MM-DD
        except: pass
        return None
    
    @staticmethod
    def normaliser_telephone(tel: str) -> Optional[str]:
        if pd.isna(tel): return None
        tel = re.sub(r'[^0-9]', '', str(tel))
        return tel[-10:] if len(tel) >= 10 else None
    
    @staticmethod
    def normaliser_sexe(sexe: str) -> Optional[str]:
        if pd.isna(sexe): return None
        s = str(sexe).upper()
        if s in ['M','H','HOMME','MASCULIN']: return 'Masculin'
        if s in ['F','FEMME','FEMININ','FÉMININ']: return 'Féminin'
        return None
    
    @staticmethod
    def parse_medecin_name(nom_complet: str) -> Tuple[str, str]:
        if pd.isna(nom_complet): return ('', '')
        nom_complet = nom_complet.replace('Dr.', '').replace('Pr.', '').strip()
        parts = nom_complet.split()
        if len(parts) >= 2:
            return (DataTransformer.normaliser_nom(' '.join(parts[1:])), parts[0].capitalize())
        return (DataTransformer.normaliser_nom(nom_complet), '')


# ============================================================
# PARTIE 5: DÉDOUBLONNAGE
# ============================================================

def calculer_similarite(s1: str, s2: str) -> float:
    if not s1 or not s2: return 0.0
    s1, s2 = s1.lower(), s2.lower()
    if s1 == s2: return 1.0
    return sum(1 for a,b in zip(s1, s2) if a == b) / max(len(s1), len(s2))

def patients_sont_doublons(p1: Dict, p2: Dict) -> bool:
    sim_nom = calculer_similarite(p1.get('nom',''), p2.get('nom',''))
    sim_prenom = calculer_similarite(p1.get('prenom',''), p2.get('prenom',''))
    
    if (sim_nom > 0.85 and sim_prenom > 0.85 and 
        p1.get('date_naissance') == p2.get('date_naissance') and p1.get('date_naissance')):
        return True
    if (p1.get('telephone') and p2.get('telephone') and p1['telephone'] == p2['telephone']):
        return True
    if (p1.get('email') and p2.get('email') and p1['email'].lower() == p2['email'].lower()):
        return True
    if (p1.get('num_dossier') and p2.get('num_dossier') and p1['num_dossier'] == p2['num_dossier']):
        return True
    return False


# ============================================================
# PARTIE 6: TRAITEMENT MDM
# ============================================================

class MDMProcessor:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        logging.info("✅ Connexion PostgreSQL établie")
        self.transformer = DataTransformer()
        self.service_cache = {}
        self.medecin_cache = {}
    
    def traiter_services(self):
        logging.info("\n🔄 Traitement SERVICES...")
        messages = lire_messages_topic(TOPICS[2])
        
        if not messages:
            logging.warning("⚠️  Aucun service trouvé!")
            return {}
        
        cursor = self.conn.cursor()
        services_norm = {}
        
        for msg in messages:
            data = msg['data']
            nom = data.get('nom_service')
            if pd.isna(nom): continue
            
            services_norm[nom] = {
                'nom_service': nom.strip(),
                'description': data.get('description'),
                'localisation': data.get('localisation'),
                'horaires': data.get('horaires_ouverture_lundi', ''),
                'responsable_nom_source': data.get('responsable_nom')
            }
        
        logging.info(f"  💾 Insertion de {len(services_norm)} services...")
        nb = 0
        for s in services_norm.values():
            try:
                cursor.execute("""
                    INSERT INTO mdm.mdm_service (nom_service, description, localisation, horaires)
                    VALUES (%s, %s, %s, %s)
                    RETURNING service_id, nom_service
                """, (s['nom_service'], s['description'], s['localisation'], s['horaires']))
                result = cursor.fetchone()
                if result:
                    nb += 1
                    self.service_cache[result[1]] = result[0]
                    logging.info(f"    ✓ {result[1]}")
            except Exception as e:
                logging.error(f"    ✗ {s['nom_service']}: {e}")
                self.conn.rollback()
        
        self.conn.commit()
        cursor.close()
        logging.info(f"✅ {nb} services insérés")
        return services_norm
    
    def traiter_medecins(self):
        logging.info("\n🔄 Traitement MÉDECINS...")
        messages = lire_messages_topic(TOPICS[1])
        
        if not messages:
            logging.warning("⚠️  Aucun médecin trouvé!")
            return
        
        cursor = self.conn.cursor()
        medecins_norm = {}
        
        for msg in messages:
            data = msg['data']
            source = msg['source']
            
            if source == 'ERP':
                m = {
                    'nom': self.transformer.normaliser_nom(data.get('nom')),
                    'prenom': data.get('prenom', '').strip(),
                    'specialite': data.get('specialite'),
                    'num_licence': data.get('num_licence'),
                    'email': data.get('email_pro'),
                    'telephone': self.transformer.normaliser_telephone(data.get('telephone_pro')),
                    'service_nom': data.get('service_affecte'),
                    'disponibilite': data.get('disponibilite_lundi')
                }
            else:  # RDV
                nom, prenom = self.transformer.parse_medecin_name(data.get('nom_complet', ''))
                m = {
                    'nom': nom,
                    'prenom': prenom,
                    'specialite': data.get('specialite'),
                    'email': data.get('email'),
                    'telephone': self.transformer.normaliser_telephone(data.get('telephone'))
                }
            
            key = m.get('num_licence') or m.get('email')
            if key:
                if key not in medecins_norm:
                    medecins_norm[key] = m
                elif source == 'ERP':
                    medecins_norm[key].update(m)
        
        logging.info(f"  💾 Insertion de {len(medecins_norm)} médecins...")
        nb = 0
        for m in medecins_norm.values():
            service_id = self.service_cache.get(m.get('service_nom'))
            try:
                cursor.execute("""
                    INSERT INTO mdm.mdm_medecin 
                    (nom, prenom, specialite, service_id, num_licence, disponibilite, email, telephone)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING medecin_id, nom, prenom
                """, (m['nom'], m['prenom'], m['specialite'], service_id,
                      m.get('num_licence'), m.get('disponibilite'), m.get('email'), m.get('telephone')))
                result = cursor.fetchone()
                if result:
                    nb += 1
                    self.medecin_cache[f"Dr. {result[2]} {result[1]}"] = result[0]
                    logging.info(f"    ✓ Dr. {result[2]} {result[1]}")
            except Exception as e:
                logging.error(f"    ✗ {m['nom']}: {e}")
                self.conn.rollback()
        
        self.conn.commit()
        cursor.close()
        logging.info(f"✅ {nb} médecins insérés")
    
    def lier_responsables(self, services_norm):
        logging.info("\n🔗 Liaison responsables...")
        cursor = self.conn.cursor()
        nb = 0
        for nom_svc, s in services_norm.items():
            resp_nom = s.get('responsable_nom_source')
            if resp_nom:
                resp_id = self.medecin_cache.get(resp_nom)
                if resp_id:
                    try:
                        cursor.execute("UPDATE mdm.mdm_service SET responsable_id = %s WHERE nom_service = %s",
                                      (resp_id, nom_svc))
                        nb += 1
                    except Exception as e:
                        logging.error(f"  ✗ {resp_nom}: {e}")
        self.conn.commit()
        cursor.close()
        logging.info(f"✅ {nb} responsables liés")
    
    def traiter_patients(self):
        logging.info("\n🔄 Traitement PATIENTS...")
        messages = lire_messages_topic(TOPICS[0])
        
        if not messages:
            logging.warning("⚠️  Aucun patient trouvé!")
            return
        
        # Normaliser
        patients = []
        for msg in messages:
            data = msg['data']
            source = msg['source']
            p = {'source': source}
            
            if source == 'RDV':
                p.update({
                    'nom': self.transformer.normaliser_nom(data.get('nom')),
                    'prenom': data.get('prenom', '').strip(),
                    'sexe': self.transformer.normaliser_sexe(data.get('sexe')),
                    'telephone': self.transformer.normaliser_telephone(data.get('telephone')),
                    'adresse': data.get('adresse')
                })
            elif source == 'ERP':
                p.update({
                    'nom': self.transformer.normaliser_nom(data.get('nom')),
                    'prenom': data.get('prenom', '').strip(),
                    'date_naissance': self.transformer.normaliser_date(data.get('date_naissance')),
                    'email': data.get('email'),
                    'adresse': data.get('adresse'),
                    'num_dossier': data.get('num_dossier'),
                    'historique_medical': data.get('historique_medical')
                })
            elif source == 'FACT':
                p.update({
                    'nom': self.transformer.normaliser_nom(data.get('nom_famille')),
                    'prenom': data.get('prenoms', '').strip(),
                    'telephone': self.transformer.normaliser_telephone(data.get('tel_contact')),
                    'email': data.get('email_contact'),
                    'adresse': data.get('adresse_facturation')
                })
            
            patients.append(p)
        
        logging.info(f"  📋 {len(patients)} patients normalisés")
        
        # Dédoublonnage
        logging.info("  🔍 Dédoublonnage...")
        uniques = []
        traites = set()
        
        for i, p1 in enumerate(patients):
            if i in traites: continue
            golden = p1.copy()
            traites.add(i)
            
            for j in range(i+1, len(patients)):
                if j in traites: continue
                if patients_sont_doublons(golden, patients[j]):
                    # Fusionner
                    for k in ['email','telephone','adresse','sexe','date_naissance','num_dossier','historique_medical']:
                        if not golden.get(k) and patients[j].get(k):
                            golden[k] = patients[j][k]
                    traites.add(j)
            
            uniques.append(golden)
        
        logging.info(f"  ✅ {len(uniques)} patients uniques ({len(patients)-len(uniques)} doublons éliminés)")
        
        # Insertion
        logging.info("  💾 Insertion...")
        cursor = self.conn.cursor()
        nb = 0
        for p in uniques:
            if not p.get('nom') or not p.get('prenom') or not p.get('date_naissance'):
                continue
            
            try:
                cursor.execute("""
                    INSERT INTO mdm.mdm_patient 
                    (nom, prenom, date_naissance, sexe, adresse, telephone, email, num_dossier, historique_medical)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (p['nom'], p['prenom'], p['date_naissance'], p.get('sexe'),
                      p.get('adresse'), p.get('telephone'), p.get('email'), 
                      p.get('num_dossier'), p.get('historique_medical')))
                nb += 1
                if nb % 20 == 0:
                    logging.info(f"    ... {nb} insérés")
            except Exception as e:
                logging.error(f"    ✗ {p.get('nom')}: {e}")
        
        self.conn.commit()
        cursor.close()
        logging.info(f"✅ {nb} patients insérés")
    
    def close(self):
        self.conn.close()


# ============================================================
# MAIN
# ============================================================

def main():
    print("="*60)
    print("🏥 ETL MDM CLINIQUE - VERSION SIMPLIFIÉE")
    print("="*60)
    
    # Étape 1: Reset Kafka
    reset_kafka_topics()
    
    # Étape 2: Envoi
    print("\n" + "="*60)
    print("PHASE 1: ENVOI")
    print("="*60)
    
    producer = DataProducer()
    producer.send_services_to_kafka()
    producer.send_medecins_to_kafka()
    producer.send_patients_to_kafka()
    producer.close()
    
    print("\n⏳ Pause 3 secondes...\n")
    time.sleep(3)
    
    # Étape 3: Traitement
    print("="*60)
    print("PHASE 2: TRAITEMENT")
    print("="*60)
    
    processor = MDMProcessor()
    try:
        services_data = processor.traiter_services()
        processor.traiter_medecins()
        processor.lier_responsables(services_data)
        processor.traiter_patients()
    finally:
        processor.close()
    
    # Étape 4: Vérification
    print("\n" + "="*60)
    print("VÉRIFICATION")
    print("="*60)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM mdm.mdm_patient")
    print(f"✅ Patients: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM mdm.mdm_medecin")
    print(f"✅ Médecins: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM mdm.mdm_service")
    print(f"✅ Services: {cursor.fetchone()[0]}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "="*60)
    print("🎉 ETL TERMINÉ!")
    print("="*60)


if __name__ == "__main__":
    main()