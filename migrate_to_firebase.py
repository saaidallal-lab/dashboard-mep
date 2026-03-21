import os
import pandas as pd
from google.cloud import firestore
import math

# Configuration de l'accès Firebase
# (Assurez-vous que le fichier firebase-key.json est présent dans ce dossier)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "firebase-key.json"

db = firestore.Client()

def migrate_data():
    file_path = "Suivi heures mep.xlsx"
    sheet_name = "Kpi 2026"

    print(f"Chargement du fichier {file_path} - Feuille: {sheet_name}...")
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier Excel: {e}")
        return

    # Nettoyage basique
    df = df.dropna(how="all")
    df = df.fillna(0)  # Remplacer les NaN par 0

    print("Insertion dans Firestore en cours...")
    collection_ref = db.collection("kpi_2026")
    
    count = 0
    for index, row in df.iterrows():
        # Conversion de la ligne en dictionnaire standard
        data = row.to_dict()
        
        # S'assurer que les valeurs non sérialisables (comme NaN non capturés) sont propres
        clean_data = {}
        for k, v in data.items():
            if pd.isna(v) or v == "" or k.startswith("Unnamed"):
                clean_data[k] = 0 if isinstance(v, (int, float)) else ""
            else:
                clean_data[k] = v
                
        # Utiliser la colonne "Semaine" comme ID de document si possible, sinon un identifiant généré
        # Firestore n'accepte pas / dans les IDs, on remplace par _
        raw_id = str(clean_data.get("Semaine", f"row_{index}"))
        doc_id = raw_id.replace("/", "_").replace(".", "_").strip()
        
        # Enregistrer dans Firebase
        collection_ref.document(doc_id).set(clean_data)
        count += 1
        print(f"Ligne de la semaine {doc_id} ajoutée.")

    print(f"Migration terminée ! {count} lignes insérées dans la collection 'kpi_2026'.")

if __name__ == "__main__":
    if not os.path.exists("firebase-key.json"):
        print("Erreur : Le fichier 'firebase-key.json' est introuvable.")
        print("Veuillez générer la clé de l'Admin SDK dans la console Firebase et la placer ici sous le nom 'firebase-key.json'.")
    else:
        migrate_data()
