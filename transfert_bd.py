import logging
import requests
import psycopg2
from flask import Flask, jsonify

app = Flask(__name__)

# Configuration PostgreSQL
DB_CONFIG = {
    'dbname': 'bd_ouaga_pdec',  # Nom de la base de données
    'user': 'postgres',         # Nom d'utilisateur PostgreSQL
    'password': '2023',         # Mot de passe
    'host': 'localhost',        # Hôte (ou adresse IP)
    'port': '5432'              # Port PostgreSQL par défaut
}

# Liste des clés nécessaires pour la validation
required_keys = [
    'OBJECTID', 'SECTION', 'LOT', 'PARCELLE', 'RANG', 'NUMÉRO_DU_COMPTEUR', 'NUMÉRO_D_ABONNÉ', 
    'POSITION_GPS', 'NUMÉRO_DE_TÉLÉPHONE', 'CODESTICKER', 'PUISSANCE_SOUSCRITE', 'AMPERAGE', 
    'EXPLOITATION', 'NATURE_CLIENT', 'USAGE_D_AUTRE_SOURCE_D_ÉNERGIE', 'AUTRE_SOURCE_D_ÉNERGIE', 
    'TYPE_DE_CLIENT', 'USAGE', 'CATÉGORIE_D_INSTITUTION', 'TYPE_DE_BÂTI', 'CATÉGORIE_ADMINISTRATION', 
    'ACTIVITÉS', 'GLOBALID', 'CREATED_USER', 'CREATED_DATE', 'LAST_EDITED_USER', 'LAST_EDITED_DATE', 
    'NOTES', 'LOT_1', 'USAGE_SECONDAIRE_POUR_ACTIVITÉ', 'NOM_D_UTILISATEUR', 'VALIDATEUR', 
    'QUALITÉ_VÉRIFIÉE', 'ACCESSIBILITÉ', 'NOM', 'N_POLICE', 'PRÉNOMS', 'SOCIOPROFESSIONALCATEGORY', 
    'ÉQUIPEMENT_ADMINISTRATIF', 'ÉQUIPEMENT_INSTITUTIONNEL', 'ÉQUIPEMENT_DE_MÉNAGE', 'POINT_X', 
    'POINT_Y', 'KWH_2023_2024', 'FCFA_2023_2024'
]

def get_db_connection():
    """Établit une connexion à la base de données."""
    return psycopg2.connect(**DB_CONFIG)

def create_table_if_not_exists():
    """Crée la table 'abonne_reçoit_api' si elle n'existe pas."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS abonne_reçoit_api (
        OBJECTID SERIAL PRIMARY KEY,
        SECTION VARCHAR(255),
        LOT VARCHAR(255),
        PARCELLE VARCHAR(255),
        RANG VARCHAR(255),
        NUMÉRO_DU_COMPTEUR VARCHAR(255),
        NUMÉRO_D_ABONNÉ VARCHAR(255),
        POSITION_GPS VARCHAR(255),
        NUMÉRO_DE_TÉLÉPHONE VARCHAR(255),
        CODESTICKER VARCHAR(255),
        PUISSANCE_SOUSCRITE VARCHAR(255),
        AMPERAGE VARCHAR(255),
        EXPLOITATION VARCHAR(255),
        NATURE_CLIENT VARCHAR(255),
        USAGE_D_AUTRE_SOURCE_D_ÉNERGIE VARCHAR(255),
        AUTRE_SOURCE_D_ÉNERGIE VARCHAR(255),
        TYPE_DE_CLIENT VARCHAR(255),
        USAGE VARCHAR(255),
        CATÉGORIE_D_INSTITUTION VARCHAR(255),
        TYPE_DE_BÂTI VARCHAR(255),
        CATÉGORIE_ADMINISTRATION VARCHAR(255),
        ACTIVITÉS VARCHAR(255),
        GLOBALID VARCHAR(255),
        CREATED_USER VARCHAR(255),
        CREATED_DATE VARCHAR(255),
        LAST_EDITED_USER VARCHAR(255),
        LAST_EDITED_DATE VARCHAR(255),
        NOTES VARCHAR(255),
        LOT_1 VARCHAR(255),
        USAGE_SECONDAIRE_POUR_ACTIVITÉ VARCHAR(255),
        NOM_D_UTILISATEUR VARCHAR(255),
        VALIDATEUR VARCHAR(255),
        QUALITÉ_VÉRIFIÉE VARCHAR(255),
        ACCESSIBILITÉ VARCHAR(255),
        NOM VARCHAR(255),
        N_POLICE VARCHAR(255),
        PRÉNOMS VARCHAR(255),
        SOCIOPROFESSIONALCATEGORY VARCHAR(255),
        ÉQUIPEMENT_ADMINISTRATIF VARCHAR(255),
        ÉQUIPEMENT_INSTITUTIONNEL VARCHAR(255),
        ÉQUIPEMENT_DE_MÉNAGE VARCHAR(255),
        POINT_X VARCHAR(255),
        POINT_Y VARCHAR(255),
        KWH_2023_2024 VARCHAR(255),
        FCFA_2023_2024 VARCHAR(255)
    );
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

# Normaliser les clés des données
def normalize_keys(row):
    """Convertit les clés d'un dictionnaire en majuscules."""
    return {key.upper(): value for key, value in row.items()}

# Normaliser les données et gérer les valeurs invalides
def normalize_row(row):
    """Normalise les clés et remplace les valeurs invalides par une valeur par défaut."""
    normalized_row = {}
    for key in row:
        value = row[key]
        if value in [None, 'NaN', 'nan']:
            normalized_row[key] = None  # ou vous pouvez mettre une valeur par défaut comme ''
        else:
            normalized_row[key] = value
    return normalized_row

@app.route('/api/transfer_abonne', methods=['POST'])
def transfer_abonne():
    conn = None  # Initialisation de conn à None
    try:
        logging.info('Début du transfert des abonnés...')
        
        # Créer la table si elle n'existe pas
        create_table_if_not_exists()

        # Étape 1 : Récupérer les données depuis l'API source
        source_url = 'http://localhost:5000/api/abonne'
        response = requests.get(source_url)
        response.raise_for_status()  # Lève une exception en cas de statut HTTP != 200
        data = response.json()

        logging.info(f"{len(data)} lignes reçues depuis l'API source.")

        # Normaliser les clés des données et gérer les valeurs invalides
        data = [normalize_row(normalize_keys(row)) for row in data]

        # Valider les données
        valid_data = []
        for row in data:
            valid_row = tuple(row.get(key, None) for key in required_keys)
            valid_data.append(valid_row)

        logging.info(f"{len(valid_data)} lignes valides prêtes pour l'insertion.")

        if not valid_data:
            logging.warning("Aucune ligne valide trouvée après validation.")
            return jsonify({'error': 'Aucune donnée valide trouvée'}), 400

        valid_columns = required_keys

        insert_query = f"""
            INSERT INTO abonne_reçoit_api ({', '.join(valid_columns)}) 
            VALUES ({', '.join(['%s'] * len(valid_columns))})
        """

        # Étape 3 : Insérer les données dans la table cible
        conn = get_db_connection()
        cursor = conn.cursor()

        logging.info(f"Structure des données : {valid_data[0]}")
        cursor.executemany(insert_query, valid_data)
        conn.commit()

        logging.info(f'{len(valid_data)} enregistrements transférés avec succès')
        return jsonify({'message': f'{len(valid_data)} enregistrements transférés avec succès'}), 201

    except requests.exceptions.RequestException as e:
        logging.error(f'Erreur API : {str(e)}')
        return jsonify({'error': 'Erreur avec l\'API source', 'details': str(e)}), 500
    except psycopg2.DatabaseError as e:
        logging.error(f'Erreur DB : {str(e)}')
        return jsonify({'error': 'Erreur avec la base de données', 'details': str(e)}), 500
    except Exception as e:
        logging.error(f'Erreur inconnue : {str(e)}')
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True, host='0.0.0.0', port=5001)
