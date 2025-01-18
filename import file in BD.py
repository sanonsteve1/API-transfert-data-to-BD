import pandas as pd
import psycopg2
from psycopg2 import sql
import os

def import_excel_to_db(file_name, table_name, db_config):
    try:
        # Lire le fichier Excel
        file_path = os.path.join(os.getcwd(), file_name)
        df = pd.read_excel(file_path)

        # Convertir les noms des colonnes en minuscules
        df.columns = [col.lower() for col in df.columns]

        # Connexion à la base de données PostgreSQL
        conn = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )
        cursor = conn.cursor()

        # Insérer les données ligne par ligne
        for _, row in df.iterrows():
            columns = list(row.index)  # Colonnes du fichier
            values = [row[col] for col in columns]  # Valeurs de la ligne
            insert_query = sql.SQL(
                "INSERT INTO {table} ({fields}) VALUES ({placeholders})"
            ).format(
                table=sql.Identifier(table_name),
                fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
                placeholders=sql.SQL(', ').join(sql.Placeholder() * len(values))
            )
            cursor.execute(insert_query, values)

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
file_name = 'Abonne_Sonabel_Ouaga_Janv_2025.xlsx'  # Nom du fichier Excel
table_name = 'abonne_sonabel_new_version'  # Nom de la table PostgreSQL
import_excel_to_db(file_name, table_name, db_config)
