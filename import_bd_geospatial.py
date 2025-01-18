import pandas as pd
import psycopg2
from psycopg2 import sql
import os

# Fonction pour vérifier si le fichier est un fichier Excel valide
def check_excel_sheets(file_name):
    try:
        # Vérifier si le fichier est Excel
        if file_name.lower().endswith(('.xlsx', '.xls')):
            # Lire les feuilles de calcul du fichier Excel
            file_path = os.path.join(os.getcwd(), file_name)
            xls = pd.ExcelFile(file_path)
            
            # Vérifier les noms de feuilles
            if len(xls.sheet_names) == 0:
                print("Le fichier Excel ne contient aucune feuille.")
                return False
            else:
                print(f"Feuilles disponibles : {xls.sheet_names}")
                return True
        else:
            return True  # Pour les fichiers CSV, il n'y a pas besoin de vérifier les feuilles
        
    except Exception as e:
        print(f"Une erreur s'est produite : {e}")
        return False

def identify_geometry_type(df):
    # Identifier les colonnes contenant des géométries (lat, long pour les points)
    geometry_columns = []
    for col in df.columns:
        if 'lat' in col.lower() and 'lon' in col.lower():
            geometry_columns.append(col)
    return geometry_columns

def create_geometry_column(geometry_columns, table_name, cursor):
    if geometry_columns:
        print("Ajout d'une colonne géométrique...")
        # On suppose qu'il y a 2 colonnes, latitude et longitude
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);")
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS geom_wkt text;")  # Ajouter une colonne pour la géométrie en WKT
        cursor.execute(f"UPDATE {table_name} SET geom = ST_SetSRID(ST_MakePoint(lat, lon), 4326);")
        cursor.execute(f"UPDATE {table_name} SET geom_wkt = ST_AsText(geom);")  # Convertir la géométrie en texte WKT
        print("Colonne géométrique en WKT ajoutée avec succès.")

def import_file_to_db(file_name, table_name, db_config):
    try:
        # Vérifier le type de fichier et charger le DataFrame approprié
        if file_name.lower().endswith(('.xlsx', '.xls')):
            if not check_excel_sheets(file_name):
                print("Impossible de continuer l'importation en raison d'un problème avec le fichier Excel.")
                return
            print("Lecture du fichier Excel...")
            file_path = os.path.join(os.getcwd(), file_name)
            df = pd.read_excel(file_path)
        elif file_name.lower().endswith('.csv'):
            print("Lecture du fichier CSV...")
            file_path = os.path.join(os.getcwd(), file_name)
            df = pd.read_csv(file_path)
        else:
            print("Format de fichier non pris en charge.")
            return
        
        print(f"Colonnes du fichier : {df.columns}")
        
        # Convertir les noms des colonnes en minuscules
        df.columns = [col.lower() for col in df.columns]

        # Connexion à la base de données PostgreSQL
        print("Connexion à la base de données...")
        conn = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )
        cursor = conn.cursor()

        # Vérifier et installer l'extension PostGIS si nécessaire
        print("Vérification de l'extension PostGIS...")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        conn.commit()

        # Créer la table si elle n'existe pas
        print(f"Création de la table {table_name} si elle n'existe pas...")
        create_table_query = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {table} ({fields})"
        ).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(', ').join(
                sql.SQL("{} {}").format(
                    sql.Identifier(col),
                    sql.SQL("VARCHAR(255)")  # Utilisation de VARCHAR(255) pour un typage générique
                ) for col in df.columns
            )
        )
        cursor.execute(create_table_query)

        # Vérifier et ajouter les colonnes manquantes
        print("Vérification des colonnes manquantes...")
        cursor.execute(
            sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_name = %s"),
            [table_name]
        )
        existing_columns = {row[0] for row in cursor.fetchall()}

        for col in df.columns:
            if col not in existing_columns:
                print(f"Ajout de la colonne manquante : {col}")
                alter_table_query = sql.SQL(
                    "ALTER TABLE {table} ADD COLUMN {column} VARCHAR(255)"
                ).format(
                    table=sql.Identifier(table_name),
                    column=sql.Identifier(col)
                )
                cursor.execute(alter_table_query)

        # Ajouter la colonne géométrique si nécessaire
        print("Ajout de la colonne géométrique...")
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);")

        # Insérer les données ligne par ligne
        print("Insertion des données dans la table...")
        for _, row in df.iterrows():
            columns = list(row.index)  # Colonnes du fichier
            values = [row[col] for col in columns]  # Valeurs de la ligne

            # Construire la requête d'insertion sans la colonne 'geom'
            insert_query = sql.SQL(
                "INSERT INTO {table} ({fields}) VALUES ({placeholders})"
            ).format(
                table=sql.Identifier(table_name),
                fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
                placeholders=sql.SQL(', ').join(sql.Placeholder() * len(values))
            )

            # Exécuter l'insertion sans la géométrie
            cursor.execute(insert_query, values)

                        # Mise à jour de la géométrie avec conversion explicite de type
            if 'latitude' in row and 'longitude' in row:
                update_geom_query = sql.SQL(
                    "UPDATE {table} SET geom = ST_SetSRID(ST_MakePoint(%s, %s), 4326) WHERE {id_column} = %s"
                ).format(
                    table=sql.Identifier(table_name),
                    id_column=sql.Identifier('objectid')  # Remplacez par la clé primaire appropriée
                )
                # Assurez-vous que l'objectid est bien converti en texte
                cursor.execute(update_geom_query, (row['POINT_X'], row['POINT_Y'], str(row['objectid'])))


        # Valider les transactions
        conn.commit()
        print(f"Les données ont été importées avec succès dans la table '{table_name}'.")

    except Exception as e:
        print(f"Une erreur s'est produite : {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# Configuration de la base de données
db_config = {
    'dbname': 'bd_ouaga_pdec',  # Nom de la base de données
    'user': 'postgres',  # Nom d'utilisateur PostgreSQL
    'password': '2023',  # Mot de passe
    'host': 'localhost',  # Hôte (ou adresse IP)
    'port': '5432'  # Port PostgreSQL par défaut
}

# Exemple d'utilisation
file_name = 'abonne_poc.xls'  # Nom du fichier CSV ou Excel
table_name = 'abonne_sonabel_pdec_spatial'  # Nom de la table PostgreSQL
import_file_to_db(file_name, table_name, db_config)
