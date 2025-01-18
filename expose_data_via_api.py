from flask import Flask, jsonify, request
import psycopg2

app = Flask(__name__)

# Configuration PostgreSQL
DB_CONFIG = {
    'dbname': 'bd_ouaga_pdec',  # Nom de la base de données
    'user': 'postgres',  # Nom d'utilisateur PostgreSQL
    'password': '2023',  # Mot de passe
    'host': 'localhost',  # Hôte (ou adresse IP)
    'port': '5432'  # Port PostgreSQL par défaut
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# Endpoint pour récupérer toutes les données
@app.route('/api/abonne', methods=['GET'])
def get_abonne():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM abonne_sonabel_pdec_spatial;")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        data = [dict(zip(columns, row)) for row in rows]
        return jsonify(data), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

# Endpoint pour ajouter une nouvelle entrée
@app.route('/api/abonne', methods=['POST'])
def add_abonne():
    conn = None  # Initialiser la variable conn
    try:
        data = request.json

        # Vérification des champs requis
        required_fields = [
            'OBJECTID', 'Section', 'Lot', 'Parcelle', 'Rang', 'Numéro_du_compteur', 
            'Numéro_d_abonné', 'Position_GPS', 'Numéro_de_téléphone', 'CodeSticker', 
            'Puissance_souscrite', 'Amperage', 'Exploitation', 'Nature_Client', 
            'Usage_d_autre_source_d_énergie', 'autre_source_d_énergie', 'Type_de_client', 
            'Usage', 'Catégorie_d_institution', 'Type_de_bâti', 'Catégorie_administration', 
            'Activités', 'GlobalID', 'created_user', 'created_date', 'last_edited_user', 
            'last_edited_date', 'Notes', 'Lot_1', 'Usage_secondaire_pour_activité', 
            'Nom_d_utilisateur', 'Validateur', 'Qualité_vérifiée', 'Accessibilité', 
            'Nom', 'N_Police', 'Prénoms', 'SocioProfessionalCategory', 
            'Équipement_administratif', 'Équipement_institutionnel', 'Équipement_de_ménage', 
            'POINT_X', 'POINT_Y', 'KWH_2023_2024', 'FCFA_2023_2024'
        ]
        
        # Construire une liste des valeurs à insérer
        values = [data.get(field) for field in required_fields]

        # Vérifier que toutes les valeurs requises sont présentes
        if None in values:
            missing_fields = [required_fields[i] for i, v in enumerate(values) if v is None]
            return jsonify({'error': f'Champs manquants : {", ".join(missing_fields)}'}), 400

        # Insérer les données dans la table
        conn = get_db_connection()
        cursor = conn.cursor()
        query = f"""
            INSERT INTO abonne (
                {', '.join(required_fields)}
            )
            VALUES ({', '.join(['%s'] * len(required_fields))})
        """
        cursor.execute(query, values)
        conn.commit()

        return jsonify({'message': 'Données insérées avec succès'}), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        # Fermer la connexion uniquement si elle a été établie
        if conn:
            conn.close()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
