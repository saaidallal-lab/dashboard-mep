import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
import os
import base64
from google.cloud import firestore
from google.oauth2 import service_account
import json
import warnings
import uuid
from datetime import date
warnings.filterwarnings('ignore')

# Initialisation Firebase — st.secrets en production, firebase-key.json en local
FIRESTORE_COLLECTION = "kpi_2026"
COLLECTION_RECETTES   = "recettes"
COLLECTION_INGREDIENTS = "ingredients"
COLLECTION_FACTURES   = "factures"

def _get_firestore_client():
    """
    Authentification Firestore avec 3 stratégies :
    1. FIREBASE_CREDENTIALS (base64 du JSON) — le plus simple à coller dans Streamlit Cloud
    2. [gcp_service_account] en TOML dans st.secrets
    3. Fichier firebase-key.json local (dev)
    """
    import traceback

    try:
        # Méthode 1 : base64 (une seule ligne, facile à coller)
        if "FIREBASE_CREDENTIALS" in st.secrets:
            try:
                creds_json = base64.b64decode(st.secrets["FIREBASE_CREDENTIALS"]).decode("utf-8")
                key_dict = json.loads(creds_json)
                creds = service_account.Credentials.from_service_account_info(key_dict)
                return firestore.Client(project=key_dict["project_id"], credentials=creds)
            except Exception:
                pass

        # Méthode 2 : TOML [gcp_service_account] dans st.secrets
        if "gcp_service_account" in st.secrets:
            try:
                key_dict = json.loads(json.dumps(dict(st.secrets["gcp_service_account"])))
                creds = service_account.Credentials.from_service_account_info(key_dict)
                return firestore.Client(project=key_dict["project_id"], credentials=creds)
            except Exception:
                pass
    except Exception:
        pass

    # Méthode 3 : fichier local (dev)
    key_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "firebase-key.json")
    if os.path.exists(key_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
        return firestore.Client()

    raise RuntimeError(
        "Impossible de se connecter à Firestore. "
        "Configurez FIREBASE_CREDENTIALS (base64) dans les secrets Streamlit Cloud."
    )

_db = _get_firestore_client()

def _get_gcp_credentials():
    """Retourne les credentials GCP (scoped) pour Vision API et autres services."""
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    try:
        if "FIREBASE_CREDENTIALS" in st.secrets:
            key_dict = json.loads(base64.b64decode(st.secrets["FIREBASE_CREDENTIALS"]).decode("utf-8"))
            return service_account.Credentials.from_service_account_info(key_dict, scopes=scopes)
        if "gcp_service_account" in st.secrets:
            key_dict = json.loads(json.dumps(dict(st.secrets["gcp_service_account"])))
            return service_account.Credentials.from_service_account_info(key_dict, scopes=scopes)
    except Exception:
        pass
    key_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "firebase-key.json")
    if os.path.exists(key_path):
        return service_account.Credentials.from_service_account_file(key_path, scopes=scopes)
    return None

# Configuration globale de la police Plotly
pio.templates.default = "plotly"
pio.templates[pio.templates.default].layout.font.family = "Montserrat, sans-serif"

# Configuration de la page
st.set_page_config(page_title="Dashboard MEP", layout="wide")

# Injection de CSS personnalisé pour moderniser l'interface
st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@300;400;500;600;700;800&display=swap');
        
        /* Application de la police Montserrat */
        html, body, [class*="css"] {
            font-family: 'Montserrat', sans-serif !important;
        }
        
        /* Amélioration de l'apparence des KPI (Metric cards) */
        [data-testid="stMetric"] {
            background-color: #ffffff;
            border: 1px solid #e0e4eb;
            padding: 15px 20px;
            border-radius: 12px;
            box-shadow: 0 4px 10px rgba(0,0,0,0.04);
            transition: transform 0.2s ease-in-out;
        }
        
        [data-testid="stMetric"]:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 15px rgba(0,0,0,0.08);
        }
        
        [data-testid="stMetricValue"] {
            font-weight: 700 !important;
            color: #1f77b4; /* Une teinte bleue moderne */
        }
        
        /* Titres légèrement plus gras */
        h1, h2, h3 {
            font-weight: 700 !important;
        }
    </style>
""", unsafe_allow_html=True)

# --- FONCTION DE CHARGEMENT DEPUIS FIRESTORE ---
@st.cache_data(ttl=60)  # Rafraîchit les données toutes les minutes
def load_data():
    docs = _db.collection(FIRESTORE_COLLECTION).stream()
    rows = [doc.to_dict() for doc in docs]
    if not rows:
        return pd.DataFrame(columns=['Semaine', 'Commandes', 'Total heure', 'Chaud', 'Légumerie', 'Sushi', 'Découpe', 'UVC/H par ETP'])

    df = pd.DataFrame(rows)

    # Mapping des colonnes (pour rester compatible avec le reste du code)
    mapping = {
        'Heures Chaud': 'Chaud',
        'Heures Légumerie': 'Légumerie',
        'Heures Découpe': 'Découpe',
        'Heure Sushi': 'Sushi',
        'Chaud kg/H': 'Chaud_kg_h',
        'Légumerie KG/H': 'Légumerie_kg_h',
        'Découpe KG/H': 'Découpe_kg_h',
        'Kg/H Sushi': 'Sushi_kg_h',
        'Heures Mix': 'Mix',
        'Heures Mélange': 'Mélange',
        'Mix KG/H': 'Mix_kg_h',
        'Mélange KG/H': 'Mélange_kg_h',
        'Kg/H ': 'Global_kg_h',
        '€/kg (Mep global)': 'Euro_kilo_global',
        'Heures Désinfection': 'Désinfection',
        'Heures Traçabilité': 'Traçabilité',
        'Heures CF tampon': 'CF tampon',
        'Désinfection KG/H': 'Désinfection_kg_h',
        'Traçabilité KG/H': 'Traçabilité_kg_h',
        'CF tampon KG/H': 'CF tampon_kg_h'
    }
    df = df.rename(columns=mapping)
    
    # Sécurisation : retirer les colonnes dupliquées dues au renommage (ex: si le raw contenait déjà 'Désinfection_kg_h' et 'Désinfection KG/H')
    df = df.loc[:, ~df.columns.duplicated()]

    # Nettoyage : On ne garde que les lignes où 'Semaine' est un nombre
    df['Semaine'] = pd.to_numeric(df['Semaine'], errors='coerce')

    # Conversion en numérique pour être sûr des calculs
    cols_to_fix = ['Semaine', 'Total heure', 'Chaud', 'Légumerie', 'Sushi', 'Découpe', 'Mix', 'Mélange', 
                   'Désinfection', 'Traçabilité', 'CF tampon', 'UVC/H par ETP',
                   'Chaud_kg_h', 'Légumerie_kg_h', 'Découpe_kg_h', 'Sushi_kg_h', 'Mix_kg_h', 'Mélange_kg_h', 
                   'Désinfection_kg_h', 'Traçabilité_kg_h', 'CF tampon_kg_h',
                   'Global_kg_h', 'Kg produits global', 'Kg Sushi', 'Euro_kilo_global', 'Commandes']
    for col in cols_to_fix:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    return df.dropna(subset=['Semaine']).sort_values('Semaine').reset_index(drop=True)


# --- FONCTIONS RECETTES / INGRÉDIENTS / FACTURES ---
@st.cache_data(ttl=60)
def load_recettes():
    docs = _db.collection(COLLECTION_RECETTES).stream()
    return [{"id": d.id, **d.to_dict()} for d in docs]

@st.cache_data(ttl=60)
def load_ingredients():
    docs = _db.collection(COLLECTION_INGREDIENTS).stream()
    return {d.to_dict()["nom"]: {"id": d.id, **d.to_dict()} for d in docs}

@st.cache_data(ttl=60)
def load_factures():
    docs = _db.collection(COLLECTION_FACTURES).stream()
    return sorted([{"id": d.id, **d.to_dict()} for d in docs],
                  key=lambda x: x.get("date", ""), reverse=True)

def _calcul_fiche(recette, prix_dict):
    """Calcule les coûts d'une recette.
    Règle : coût = poids_brut × prix/kg  (on achète le brut, la perte est supportée)
    net = brut × (1 - taux_perte/100)
    """
    rows = []
    total_cout = 0.0
    for ing in recette.get("ingredients", []):
        brut = float(ing.get("poids_brut_kg", 0))
        # Taux de perte : priorité à taux_perte_pct, sinon calculé depuis poids_net_kg
        if "taux_perte_pct" in ing:
            perte_pct = float(ing["taux_perte_pct"])
            net = round(brut * (1 - perte_pct / 100), 4)
        elif "poids_net_kg" in ing and brut > 0:
            net = float(ing["poids_net_kg"])
            perte_pct = round((brut - net) / brut * 100, 1)
        else:
            net = brut
            perte_pct = 0.0
        prix = float(prix_dict.get(ing["nom"], {}).get("prix_unitaire", ing.get("prix_unitaire", 0)))
        cout = round(brut * prix, 2)   # on paye le poids brut
        total_cout += cout
        rows.append({
            "Ingrédient": ing["nom"],
            "Brut (kg)": round(brut, 3),
            "Perte %": f"{round(perte_pct, 1)}%",
            "Net final (kg)": round(net, 3),
            "Prix/kg brut (€)": round(prix, 2),
            "Coût (€)": cout,
        })
    nb = recette.get("nb_couverts", 1) or 1
    return rows, round(total_cout, 2), round(total_cout / nb, 2)

def seed_recettes_fictives():
    """Insère 10 recettes fictives si la collection est vide."""
    recettes = [
        {"nom": "Blanquette de veau", "categorie": "Chaud", "nb_couverts": 10,
         "ingredients": [
             {"nom": "Veau épaule", "poids_brut_kg": 2.5, "poids_net_kg": 2.1, "prix_unitaire": 18.5},
             {"nom": "Carottes", "poids_brut_kg": 0.5, "poids_net_kg": 0.42, "prix_unitaire": 1.2},
             {"nom": "Oignons", "poids_brut_kg": 0.3, "poids_net_kg": 0.25, "prix_unitaire": 0.9},
             {"nom": "Crème fraîche", "poids_brut_kg": 0.4, "poids_net_kg": 0.4, "prix_unitaire": 3.5},
             {"nom": "Champignons", "poids_brut_kg": 0.3, "poids_net_kg": 0.25, "prix_unitaire": 4.2},
         ]},
        {"nom": "Ratatouille provençale", "categorie": "Légumerie", "nb_couverts": 8,
         "ingredients": [
             {"nom": "Courgettes", "poids_brut_kg": 0.8, "poids_net_kg": 0.72, "prix_unitaire": 1.8},
             {"nom": "Aubergines", "poids_brut_kg": 0.6, "poids_net_kg": 0.52, "prix_unitaire": 2.1},
             {"nom": "Poivrons rouges", "poids_brut_kg": 0.5, "poids_net_kg": 0.42, "prix_unitaire": 2.8},
             {"nom": "Tomates", "poids_brut_kg": 0.8, "poids_net_kg": 0.72, "prix_unitaire": 1.5},
             {"nom": "Oignons", "poids_brut_kg": 0.3, "poids_net_kg": 0.25, "prix_unitaire": 0.9},
             {"nom": "Huile d'olive", "poids_brut_kg": 0.08, "poids_net_kg": 0.08, "prix_unitaire": 6.0},
         ]},
        {"nom": "California Roll", "categorie": "Sushi", "nb_couverts": 24,
         "ingredients": [
             {"nom": "Riz à sushi", "poids_brut_kg": 0.5, "poids_net_kg": 0.5, "prix_unitaire": 2.4},
             {"nom": "Surimi", "poids_brut_kg": 0.2, "poids_net_kg": 0.18, "prix_unitaire": 8.0},
             {"nom": "Avocat", "poids_brut_kg": 0.3, "poids_net_kg": 0.18, "prix_unitaire": 4.5},
             {"nom": "Concombre", "poids_brut_kg": 0.2, "poids_net_kg": 0.17, "prix_unitaire": 1.2},
             {"nom": "Feuilles de nori", "poids_brut_kg": 0.05, "poids_net_kg": 0.05, "prix_unitaire": 22.0},
         ]},
        {"nom": "Soupe à l'oignon gratinée", "categorie": "Chaud", "nb_couverts": 6,
         "ingredients": [
             {"nom": "Oignons", "poids_brut_kg": 1.2, "poids_net_kg": 1.0, "prix_unitaire": 0.9},
             {"nom": "Beurre", "poids_brut_kg": 0.08, "poids_net_kg": 0.08, "prix_unitaire": 9.0},
             {"nom": "Farine", "poids_brut_kg": 0.04, "poids_net_kg": 0.04, "prix_unitaire": 1.1},
             {"nom": "Bouillon bœuf", "poids_brut_kg": 1.5, "poids_net_kg": 1.5, "prix_unitaire": 0.8},
             {"nom": "Gruyère râpé", "poids_brut_kg": 0.15, "poids_net_kg": 0.15, "prix_unitaire": 12.0},
             {"nom": "Pain baguette", "poids_brut_kg": 0.2, "poids_net_kg": 0.2, "prix_unitaire": 3.5},
         ]},
        {"nom": "Salade niçoise", "categorie": "Légumerie", "nb_couverts": 6,
         "ingredients": [
             {"nom": "Thon en boîte", "poids_brut_kg": 0.3, "poids_net_kg": 0.28, "prix_unitaire": 10.0},
             {"nom": "Haricots verts", "poids_brut_kg": 0.4, "poids_net_kg": 0.35, "prix_unitaire": 3.2},
             {"nom": "Tomates", "poids_brut_kg": 0.5, "poids_net_kg": 0.45, "prix_unitaire": 1.5},
             {"nom": "Œufs", "poids_brut_kg": 0.18, "poids_net_kg": 0.15, "prix_unitaire": 5.5},
             {"nom": "Olives noires", "poids_brut_kg": 0.1, "poids_net_kg": 0.09, "prix_unitaire": 7.0},
             {"nom": "Anchois", "poids_brut_kg": 0.06, "poids_net_kg": 0.05, "prix_unitaire": 18.0},
         ]},
        {"nom": "Quiche lorraine", "categorie": "Chaud", "nb_couverts": 8,
         "ingredients": [
             {"nom": "Pâte brisée", "poids_brut_kg": 0.25, "poids_net_kg": 0.25, "prix_unitaire": 4.0},
             {"nom": "Lardons fumés", "poids_brut_kg": 0.2, "poids_net_kg": 0.18, "prix_unitaire": 7.5},
             {"nom": "Œufs", "poids_brut_kg": 0.25, "poids_net_kg": 0.22, "prix_unitaire": 5.5},
             {"nom": "Crème fraîche", "poids_brut_kg": 0.3, "poids_net_kg": 0.3, "prix_unitaire": 3.5},
             {"nom": "Gruyère râpé", "poids_brut_kg": 0.1, "poids_net_kg": 0.1, "prix_unitaire": 12.0},
         ]},
        {"nom": "Bœuf bourguignon", "categorie": "Chaud", "nb_couverts": 10,
         "ingredients": [
             {"nom": "Bœuf bourguignon", "poids_brut_kg": 2.0, "poids_net_kg": 1.7, "prix_unitaire": 14.0},
             {"nom": "Lardons fumés", "poids_brut_kg": 0.2, "poids_net_kg": 0.18, "prix_unitaire": 7.5},
             {"nom": "Carottes", "poids_brut_kg": 0.5, "poids_net_kg": 0.42, "prix_unitaire": 1.2},
             {"nom": "Oignons", "poids_brut_kg": 0.3, "poids_net_kg": 0.25, "prix_unitaire": 0.9},
             {"nom": "Champignons", "poids_brut_kg": 0.3, "poids_net_kg": 0.26, "prix_unitaire": 4.2},
             {"nom": "Vin rouge", "poids_brut_kg": 0.75, "poids_net_kg": 0.75, "prix_unitaire": 4.0},
         ]},
        {"nom": "Gratin dauphinois", "categorie": "Légumerie", "nb_couverts": 8,
         "ingredients": [
             {"nom": "Pommes de terre", "poids_brut_kg": 1.5, "poids_net_kg": 1.2, "prix_unitaire": 0.8},
             {"nom": "Crème fraîche", "poids_brut_kg": 0.5, "poids_net_kg": 0.5, "prix_unitaire": 3.5},
             {"nom": "Lait entier", "poids_brut_kg": 0.3, "poids_net_kg": 0.3, "prix_unitaire": 1.1},
             {"nom": "Gruyère râpé", "poids_brut_kg": 0.12, "poids_net_kg": 0.12, "prix_unitaire": 12.0},
             {"nom": "Ail", "poids_brut_kg": 0.02, "poids_net_kg": 0.015, "prix_unitaire": 5.0},
         ]},
        {"nom": "Taboulé libanais", "categorie": "Mix", "nb_couverts": 8,
         "ingredients": [
             {"nom": "Semoule fine", "poids_brut_kg": 0.3, "poids_net_kg": 0.3, "prix_unitaire": 1.5},
             {"nom": "Tomates", "poids_brut_kg": 0.6, "poids_net_kg": 0.54, "prix_unitaire": 1.5},
             {"nom": "Concombre", "poids_brut_kg": 0.3, "poids_net_kg": 0.25, "prix_unitaire": 1.2},
             {"nom": "Persil plat", "poids_brut_kg": 0.15, "poids_net_kg": 0.12, "prix_unitaire": 6.0},
             {"nom": "Menthe fraîche", "poids_brut_kg": 0.05, "poids_net_kg": 0.04, "prix_unitaire": 8.0},
             {"nom": "Citrons", "poids_brut_kg": 0.2, "poids_net_kg": 0.18, "prix_unitaire": 2.0},
             {"nom": "Huile d'olive", "poids_brut_kg": 0.06, "poids_net_kg": 0.06, "prix_unitaire": 6.0},
         ]},
        {"nom": "Velouté de légumes", "categorie": "Légumerie", "nb_couverts": 6,
         "ingredients": [
             {"nom": "Courgettes", "poids_brut_kg": 0.5, "poids_net_kg": 0.45, "prix_unitaire": 1.8},
             {"nom": "Carottes", "poids_brut_kg": 0.4, "poids_net_kg": 0.34, "prix_unitaire": 1.2},
             {"nom": "Pommes de terre", "poids_brut_kg": 0.3, "poids_net_kg": 0.24, "prix_unitaire": 0.8},
             {"nom": "Oignons", "poids_brut_kg": 0.2, "poids_net_kg": 0.17, "prix_unitaire": 0.9},
             {"nom": "Crème fraîche", "poids_brut_kg": 0.15, "poids_net_kg": 0.15, "prix_unitaire": 3.5},
             {"nom": "Bouillon légumes", "poids_brut_kg": 1.0, "poids_net_kg": 1.0, "prix_unitaire": 0.5},
         ]},
    ]
    today = str(date.today())
    # Seed ingrédients catalogue (prix uniques par ingrédient)
    prix_vus = {}
    for rec in recettes:
        for ing in rec["ingredients"]:
            if ing["nom"] not in prix_vus:
                prix_vus[ing["nom"]] = ing["prix_unitaire"]
    for nom, prix in prix_vus.items():
        ing_id = nom.lower().replace(" ", "_").replace("'", "").replace("é", "e").replace("è", "e").replace("ê", "e")
        _db.collection(COLLECTION_INGREDIENTS).document(ing_id).set({
            "nom": nom, "prix_unitaire": prix, "unite": "kg",
            "fournisseur": "", "updated_at": today
        })
    # Seed recettes — convertit poids_net_kg → taux_perte_pct
    for rec in recettes:
        for ing in rec["ingredients"]:
            brut = float(ing.get("poids_brut_kg", 0))
            net  = float(ing.pop("poids_net_kg", brut))
            ing["taux_perte_pct"] = round((brut - net) / brut * 100, 1) if brut > 0 else 0.0
        rec_id = str(uuid.uuid4())
        _db.collection(COLLECTION_RECETTES).document(rec_id).set({**rec, "created_at": today})
    st.cache_data.clear()

def _parse_facture_text(text: str) -> dict:
    """Parse heuristique du texte OCR d'une facture vers un dict structuré."""
    import re
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # Fournisseur = première ligne non vide significative
    fournisseur = lines[0] if lines else ""

    # Date : cherche patterns DD/MM/YYYY, DD-MM-YYYY, YYYY-MM-DD
    date_val = ""
    for pat, fmt in [
        (r'\b(\d{2})[/-](\d{2})[/-](\d{4})\b', lambda m: f"{m.group(3)}-{m.group(2)}-{m.group(1)}"),
        (r'\b(\d{4})-(\d{2})-(\d{2})\b',        lambda m: m.group(0)),
    ]:
        m = re.search(pat, text)
        if m:
            date_val = fmt(m)
            break

    # Numéro facture
    numero = ""
    for pat in [r'(?:N°|No|Facture|FAC|INV)[^\d]*(\w[\w\-/]+)', r'\b(FAC|INV|F)[-\s]?(\d{3,})\b']:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            numero = m.group(0)
            break

    # Lignes : cherche pattern "texte ... quantité ... prix"
    lignes = []
    line_pat = re.compile(
        r'^(.{3,40}?)\s+(\d+[,.]?\d*)\s*(?:kg|pce|l|un|x)?\s+(\d+[,.]?\d+)\s+(\d+[,.]?\d+)',
        re.IGNORECASE
    )
    for l in lines:
        m = line_pat.match(l)
        if m:
            try:
                lignes.append({
                    "article": m.group(1).strip(),
                    "quantite": float(m.group(2).replace(",", ".")),
                    "unite": "kg",
                    "prix_unitaire": float(m.group(3).replace(",", ".")),
                    "total_ht": float(m.group(4).replace(",", ".")),
                })
            except ValueError:
                pass

    # Totaux : cherche montants après mots-clés
    def find_amount(keywords):
        for kw in keywords:
            m = re.search(rf'{kw}[^\d]*(\d+[,. ]\d{{2}})', text, re.IGNORECASE)
            if m:
                return float(m.group(1).replace(" ", "").replace(",", "."))
        return 0.0

    total_ht  = find_amount(["total ht", "montant ht", "ht"])
    tva       = find_amount(["tva", "t\\.v\\.a", "taxe"])
    total_ttc = find_amount(["total ttc", "montant ttc", "ttc", "total"])

    return {
        "fournisseur": fournisseur,
        "date": date_val or str(date.today()),
        "numero": numero,
        "lignes": lignes,
        "total_ht": total_ht,
        "tva": tva,
        "total_ttc": total_ttc,
        "_texte_brut": text,  # conservé pour vérification manuelle
    }


def _extract_facture_vision(image_bytes: bytes, media_type: str = "image/jpeg") -> dict:
    """Utilise Google Cloud Vision (gratuit 1000 req/mois) pour extraire le texte d'une facture."""
    try:
        from google.cloud import vision as gvision
        creds = _get_gcp_credentials()
        if creds is None:
            return {"error": "Credentials GCP non trouvés."}
        vision_client = gvision.ImageAnnotatorClient(credentials=creds)
        image = gvision.Image(content=image_bytes)
        response = vision_client.document_text_detection(image=image)
        if response.error.message:
            return {"error": response.error.message}
        full_text = response.full_text_annotation.text if response.full_text_annotation else ""
        if not full_text:
            return {"error": "Aucun texte détecté dans l'image."}
        result = _parse_facture_text(full_text)
        return result
    except Exception as e:
        return {"error": str(e)}


# --- CHARGEMENT DES DONNÉES ---
try:
    data = load_data()
    # On récupère les 6 dernières semaines pour l'affichage (si assez de données)
    last_6_weeks = data.tail(6) if not data.empty else data
    latest_week = data.iloc[-1] if not data.empty else None
except Exception as e:
    st.error(f"Erreur lors de la lecture du fichier : {e}")
    st.stop()

# Seed recettes fictives si collection vide
try:
    if not _db.collection(COLLECTION_RECETTES).limit(1).get():
        seed_recettes_fictives()
except Exception:
    pass

# --- EMOJIS MAPPING ---
POSTES_EMOJIS = {
    "Dashboard Global": "🌍 Dashboard Global",
    "Chaud": "🔥 Chaud",
    "Légumerie": "🥦 Légumerie",
    "Sushi": "🍣 Sushi",
    "Découpe": "🔪 Découpe",
    "Mix": "🥣 Mix",
    "Mélange": "🥄 Mélange",
    "Désinfection": "🧼 Désinfection",
    "Traçabilité": "📋 Traçabilité",
    "CF tampon": "❄️ CF tampon",
    "Fiches Techniques": "📖 Fiches Techniques",
    "Factures": "🧾 Factures",
    "Saisie de données": "✏️ Saisie de données"
}

OBJECTIF_EURO_KG = 0.70
OBJECTIF_EURO_KG_ETAPE1 = 0.875  # Objectif étape 1 (80% du chemin estimé)

OBJECTIFS_KGH = {
    'Global': 39.0,
    'Chaud': 165.0,
    'Légumerie': 195.0,
    'Sushi': 3.0,
    'Découpe': 265.0,
    'Mix': 408.0,
    'Mélange': 418.0
}
OBJECTIFS_KGH_ETAPE1 = {k: v * 0.80 for k, v in OBJECTIFS_KGH.items()}

COULEURS_POSTES = {
    'Chaud': '#fd7f6f',      # Rouge pastel
    'Légumerie': '#b2e061',  # Vert pastel
    'Sushi': '#7eb0d5',      # Bleu pastel
    'Découpe': '#bd7ebe',    # Violet pastel
    'Mix': '#ffb55a',        # Orange pastel
    'Mélange': '#ffee65',    # Jaune pastel
    'Désinfection': '#8dd3c7',# Cyan pastel
    'Traçabilité': '#bebada', # Violet clair pastel
    'CF tampon': '#80b1d3'    # Bleu acier pastel
}

COULEURS_POSTES_FONCEES = {
    'Chaud': '#d93b2b',
    'Légumerie': '#6a9f1a',
    'Sushi': '#2d6a9f',
    'Découpe': '#814a81',
    'Mix': '#cc7a18',
    'Mélange': '#a69a19',
    'Désinfection': '#1b9e77',
    'Traçabilité': '#7570b3',
    'CF tampon': '#377eb8'
}

def _progress_bar_html(pct_achieved, label="🎯 Obj.", suffix=""):
    """Barre de progression colorée selon le % atteint (rouge→orange→vert)."""
    pct_display = min(max(pct_achieved, 0), 100)
    if pct_achieved >= 100:
        color, status = "#27ae60", "✅ Atteint !"
    elif pct_achieved >= 80:
        color, status = "#f1c40f", f"{pct_achieved:.0f}%{suffix}"
    elif pct_achieved >= 60:
        color, status = "#f39c12", f"{pct_achieved:.0f}%{suffix}"
    else:
        color, status = "#e74c3c", f"{pct_achieved:.0f}%{suffix}"
    return f"""<div style="margin:6px 0 4px 0;">
        <div style="display:flex;justify-content:space-between;font-size:0.72em;color:#888;margin-bottom:3px;">
            <span>{label}</span><span style="font-weight:700;color:{color};">{status}</span>
        </div>
        <div style="background:#e8eaf0;border-radius:6px;height:7px;overflow:hidden;">
            <div style="background:{color};width:{pct_display:.1f}%;height:100%;border-radius:6px;"></div>
        </div>
    </div>"""

def _podium_html(top3, emoji_fn, inverse=False):
    """Génère le HTML d'un podium visuel (marches) pour une liste de 1-3 items {poste, delta}."""
    if not top3:
        return ""
    medals = ["🥇", "🥈", "🥉"]
    bar_colors = [
        "linear-gradient(160deg,#FFD700,#FFA500)",
        "linear-gradient(160deg,#C8C8C8,#9e9e9e)",
        "linear-gradient(160deg,#CD7F32,#8B5E2A)"
    ]
    bar_heights = [115, 80, 55]
    font_sizes = [2.2, 1.85, 1.55]
    # Ordre visuel : 2e | 1er | 3e
    visual_slots = [1, 0, 2]

    html = '<div style="display:flex;align-items:flex-end;justify-content:center;gap:8px;padding:12px 0 0 0;">'
    for vi in visual_slots:
        if vi >= len(top3):
            # case vide pour maintenir l'alignement
            html += '<div style="width:31%;min-width:80px;"></div>'
            continue
        item = top3[vi]
        rank = vi
        sign = "+" if item['delta'] > 0 else ""
        delta_color = "#27ae60" if (item['delta'] < 0) == inverse else "#e74c3c"
        label = emoji_fn(item['poste'])
        html += f"""<div style="display:flex;flex-direction:column;align-items:center;width:31%;min-width:80px;">
            <div style="font-weight:700;font-size:0.78em;text-align:center;margin:3px 0 1px;line-height:1.2;">{label}</div>
            <div style="color:{delta_color};font-weight:800;font-size:0.88em;">{sign}{item['delta']:.1f}%</div>
            <div style="background:{bar_colors[rank]};width:100%;height:{bar_heights[rank]}px;
                        border-radius:8px 8px 0 0;margin-top:6px;
                        box-shadow:0 4px 14px rgba(0,0,0,0.18);"></div>
        </div>"""
    html += '</div>'
    return html

# --- BARRE LATÉRALE (NAVIGATION) ---
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Aller vers :", 
    list(POSTES_EMOJIS.keys()),
    format_func=lambda x: POSTES_EMOJIS.get(x, x)
)
# --- PAGE 1 : DASHBOARD GLOBAL ---
if page == "Dashboard Global":
    st.title("📊 Dashboard Global Production")
    
    if not data.empty:
        st.markdown("### 📅 Filtre de Semaine")
        semaines_dispos = sorted(data['Semaine'].dropna().unique(), reverse=True)
        selected_semaine = st.selectbox("Sélectionner la semaine à analyser :", semaines_dispos)
        
        # Mettre à jour latest_week et last_6_weeks en fonction de la sélection
        latest_week = data[data['Semaine'] == selected_semaine].iloc[0]
        data_jusqu_a_semaine = data[data['Semaine'] <= selected_semaine]
        last_6_weeks = data_jusqu_a_semaine.tail(6)
        
        # Récupération de la semaine précédente
        prev_semaine_df = data[data['Semaine'] == selected_semaine - 1]
        prev_week = prev_semaine_df.iloc[0] if not prev_semaine_df.empty else None
        
        def calculate_delta(curr, prev):
            if prev is None or pd.isna(prev) or prev == 0 or pd.isna(curr):
                return None
            return ((curr - prev) / prev) * 100

        # Indicateurs clés (KPI Cards)
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Semaine Sélectionnée", f"S{int(latest_week['Semaine'])}")
        
        # Total Heures
        th_val = latest_week['Total heure'] if pd.notna(latest_week['Total heure']) else 0
        th_delta = calculate_delta(th_val, prev_week['Total heure'] if prev_week is not None else None)
        col2.metric("Total Heures", f"{th_val:.1f} h", delta=f"{th_delta:.1f}%" if th_delta is not None else None, delta_color="inverse")
        
        # Euro/Kilo Global — calculé en premier pour dériver la cible Kg/H cohérente
        ek_val = latest_week['Euro_kilo_global'] if 'Euro_kilo_global' in latest_week and pd.notna(latest_week['Euro_kilo_global']) else 0
        ek_delta = calculate_delta(ek_val, prev_week['Euro_kilo_global'] if prev_week is not None and 'Euro_kilo_global' in prev_week else None)

        # Productivité Globale (Kg/H)
        prod_val = latest_week['Global_kg_h'] if 'Global_kg_h' in latest_week and pd.notna(latest_week['Global_kg_h']) else 0
        prod_delta = calculate_delta(prod_val, prev_week['Global_kg_h'] if prev_week is not None and 'Global_kg_h' in prev_week else None)
        col3.metric("Productivité", f"{prod_val:.2f} kg/h", delta=f"{prod_delta:.1f}%" if prod_delta is not None else None)
        if prod_val > 0 and ek_val > 0:
            # Cible Kg/H déduite : coût_horaire = ek × kgh  →  kgh_cible = coût_horaire / €/kg_cible
            kgh_etape1_derive = ek_val * prod_val / OBJECTIF_EURO_KG_ETAPE1
            pct_prod_atteint = min(prod_val / kgh_etape1_derive * 100, 100)  # = OBJECTIF_EURO_KG_ETAPE1/ek_val×100 → cohérent avec col4
            if prod_val < kgh_etape1_derive:
                heures_a_supprimer_prod = th_val * (1 - prod_val / kgh_etape1_derive)
                suffix_prod = f" · -{int(round(heures_a_supprimer_prod))} h · cible {kgh_etape1_derive:.1f} kg/h"
            else:
                suffix_prod = ""
            col3.markdown(_progress_bar_html(pct_prod_atteint, label="🎯 Obj. Kg/H", suffix=suffix_prod), unsafe_allow_html=True)

        # Euro/Kilo (affichage)
        col4.metric("Euro/Kilo", f"{ek_val:.2f} €/kg", delta=f"{ek_delta:.1f}%" if ek_delta is not None else None, delta_color="inverse")
        if ek_val > 0:
            pct_ek_atteint = min(OBJECTIF_EURO_KG_ETAPE1 / ek_val * 100, 100)
            if ek_val > OBJECTIF_EURO_KG_ETAPE1:
                heures_a_supprimer_ek = th_val * (ek_val - OBJECTIF_EURO_KG_ETAPE1) / ek_val
                suffix_ek = f" · -{int(round(heures_a_supprimer_ek))} h"
            else:
                suffix_ek = ""
            col4.markdown(_progress_bar_html(pct_ek_atteint, label="🎯 Obj. €/kg", suffix=suffix_ek), unsafe_allow_html=True)

        st.markdown("---")
        
        # Graphique de tendance combiné (6 dernières semaines)
        st.subheader(f"Comparaison Kilos vs Productivité (jusqu'à S{int(latest_week['Semaine'])})")
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Axe 1 : Kilos (Barres)
        fig.add_trace(
            go.Bar(
                x=last_6_weeks['Semaine'], 
                y=last_6_weeks['Kg produits global'], 
                name="Total Kilos", 
                marker_color='#1f77b4',
                text=last_6_weeks['Kg produits global'].fillna(0).round(0).astype(int).astype(str) + " <i>kg</i>",
                textposition='inside',
                insidetextanchor='middle',
                textfont=dict(color='white', weight='bold')
            ),
            secondary_y=False,
        )
        
        # Axe 2 : Kg/H (Ligne)
        fig.add_trace(
            go.Scatter(
                x=last_6_weeks['Semaine'], 
                y=last_6_weeks['Global_kg_h'], 
                name="Kg/H Global", 
                line=dict(color='#ff7f0e', width=3),
                mode='lines+markers+text',
                text=last_6_weeks['Global_kg_h'].round(1).astype(str) + " <i>kg/h</i>",
                textposition='top center',
                textfont=dict(weight='bold', size=13, color='#ffe0b2')
            ),
            secondary_y=True,
        )
        
        # Axe 2 : Objectif Kg/H (Ligne pointillée)
        val_etape1_g = OBJECTIFS_KGH_ETAPE1['Global']
        fig.add_trace(
            go.Scatter(
                x=[last_6_weeks['Semaine'].min(), last_6_weeks['Semaine'].max()],
                y=[val_etape1_g, val_etape1_g],
                name="Obj. Kg/H",
                mode='lines',
                line=dict(color='#ff7f0e', dash='dash', width=2)
            ),
            secondary_y=True,
        )
        
        fig.update_layout(
            xaxis_title="Semaine",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        # Échelles personnalisées pour le graphique global
        fig.update_yaxes(title_text="Kilos", secondary_y=False, range=[0, 60000])
        max_y2 = max(last_6_weeks['Global_kg_h'].max() * 1.2, OBJECTIFS_KGH_ETAPE1['Global'] * 1.2, 50)
        fig.update_yaxes(title_text="Kg/H", secondary_y=True, range=[0, max_y2])
        
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Graphique donut pour la répartition par poste
        st.subheader(f"Répartition des heures (S{int(latest_week['Semaine'])})")
        
        postes = ['Chaud', 'Légumerie', 'Sushi', 'Découpe', 'Mix', 'Mélange', 'Désinfection', 'Traçabilité', 'CF tampon']
        heures_postes = []
        labels_postes = []
        for p in postes:
            h_val = latest_week[p] if p in latest_week and pd.notna(latest_week[p]) else 0
            h_prev = prev_week[p] if prev_week is not None and p in prev_week else None
            delta_str = ""
            delta_val = calculate_delta(h_val, h_prev)
            if delta_val is not None:
                # Plus pour positif
                sign = "+" if delta_val > 0 else ""
                delta_str = f" ({sign}{delta_val:.1f}%)"
                
            heures_postes.append(h_val)
            # On ajoute l'emoji au label pour la gamification
            label_with_emoji = POSTES_EMOJIS.get(p, p)
            labels_postes.append(f"{label_with_emoji}{delta_str}")
        
        couleurs_pie = [COULEURS_POSTES.get(p, '#ccc') for p in postes]
        fig_pie = go.Figure(data=[go.Pie(
            labels=labels_postes,
            values=heures_postes,
            hole=0.4,
            textinfo='label+percent',
            textposition='inside',
            marker=dict(colors=couleurs_pie)
        )])
        fig_pie.update_layout(showlegend=True, margin=dict(t=30, b=0, l=0, r=0), 
                              legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5))
        
        col_pie_spacer1, col_pie, col_pie_spacer2 = st.columns([1, 2, 1])
        with col_pie:
            st.plotly_chart(fig_pie, use_container_width=True)

        # Graphique Évolution Euro/Kilo vs Kilos Produits
        st.markdown("---")
        st.subheader(f"Comparaison Kilos Totaux vs Évolution Euro/Kilo (jusqu'à S{int(latest_week['Semaine'])})")
        if 'Euro_kilo_global' in last_6_weeks.columns and not last_6_weeks['Euro_kilo_global'].dropna().empty:
            if 'Kg produits global' in data.columns:
                kilos_for_ek = last_6_weeks['Kg produits global']
            else:
                kilos_for_ek = last_6_weeks['Total heure'] * last_6_weeks['Global_kg_h'] # Estimation si manquant

            fig_ek = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Barres pour Kilos
            fig_ek.add_trace(
                go.Bar(
                    x=last_6_weeks['Semaine'],
                    y=kilos_for_ek,
                    name="Kilos Totaux",
                    marker_color='lightblue',
                    text=kilos_for_ek.fillna(0).round(0).astype(int).astype(str) + " kg",
                    textposition='inside',
                    insidetextanchor='middle',
                    textfont=dict(color='black', weight='bold') # Enforcing black bold
                ),
                secondary_y=False,
            )
            
            # Ligne pour Euro/Kilo
            fig_ek.add_trace(
                go.Scatter(
                    x=last_6_weeks['Semaine'],
                    y=last_6_weeks['Euro_kilo_global'],
                    name="Euro/Kilo (€/kg)",
                    mode='lines+markers+text',
                    text=last_6_weeks['Euro_kilo_global'].round(2).astype(str) + " €/<i>kg</i>",
                    textposition='top center',
                    textfont=dict(color='#2ca02c', weight='bold'),
                    line=dict(color='#2ca02c', width=3),
                    marker=dict(size=8)
                ),
                secondary_y=True,
            )
            
            # Axe 2 : Objectif €/kg (Ligne pointillée)
            fig_ek.add_trace(
                go.Scatter(
                    x=[last_6_weeks['Semaine'].min(), last_6_weeks['Semaine'].max()],
                    y=[OBJECTIF_EURO_KG_ETAPE1, OBJECTIF_EURO_KG_ETAPE1],
                    name="Obj. €/kg",
                    mode='lines',
                    line=dict(color='#2ca02c', dash='dash', width=2)
                ),
                secondary_y=True,
            )
            
            fig_ek.update_layout(
                xaxis_title="Semaine",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            
            # Mettre à jour les titres des axes Y
            fig_ek.update_yaxes(title_text="Kilos", secondary_y=False)
            
            # Dynamiser l'axe Y de l'Euro/Kilo pour accentuer visuellement les petites variations (ex: 3 centimes)
            min_ek = min(last_6_weeks['Euro_kilo_global'].min(), OBJECTIF_EURO_KG_ETAPE1)
            max_ek = max(last_6_weeks['Euro_kilo_global'].max(), OBJECTIF_EURO_KG_ETAPE1)
            margin_ek = (max_ek - min_ek) * 0.2 if max_ek > min_ek else 0.05
            fig_ek.update_yaxes(title_text="€ / Kg", secondary_y=True, range=[min_ek - margin_ek, max_ek + margin_ek])
            
            st.plotly_chart(fig_ek, use_container_width=True)

            # --- AJOUT DU PODIUM ---
            st.markdown("---")
            
            col_pod_title, col_pod_filter = st.columns([1, 1])
            with col_pod_title:
                st.subheader("🏆 Podium des Performances")
            with col_pod_filter:
                podium_period = st.radio("Comparer avec :", ["Semaine précédente (S-1)", "Mois précédent (S-4)"], horizontal=True, label_visibility="collapsed")
            
            if "Semaine" in podium_period:
                ref_week = prev_week
                ref_lbl = "S-1"
            else:
                ref_semaine_df = data[data['Semaine'] == selected_semaine - 4]
                ref_week = ref_semaine_df.iloc[0] if not ref_semaine_df.empty else None
                ref_lbl = "un mois (S-4)"
                
            postes = ['Chaud', 'Légumerie', 'Découpe', 'Mix', 'Mélange']
            
            if ref_week is not None:
                perfs_heures = []
                perfs_kgh = []
                
                for p in postes:
                    # Variation Heures
                    h_val = latest_week[p] if p in latest_week and pd.notna(latest_week[p]) else 0
                    h_prev = ref_week[p] if p in ref_week and pd.notna(ref_week[p]) else None
                    if h_prev and h_prev > 0:
                        delta_h = ((h_val - h_prev) / h_prev) * 100
                        perfs_heures.append({"poste": p, "delta": delta_h})
                        
                    # Variation Kg/H
                    kgh_col = f"{p}_kg_h"
                    kgh_val = latest_week[kgh_col] if kgh_col in latest_week and pd.notna(latest_week[kgh_col]) else 0
                    kgh_prev = ref_week[kgh_col] if kgh_col in ref_week and pd.notna(ref_week[kgh_col]) else None
                    if kgh_prev and kgh_prev > 0:
                        delta_kgh = ((kgh_val - kgh_prev) / kgh_prev) * 100
                        perfs_kgh.append({"poste": p, "delta": delta_kgh})
                
                # Tri: baisse d'heures = delta le plus bas (négatif)
                top_heures = sorted(perfs_heures, key=lambda x: x["delta"])[:3]
                # Tri: progression Kg/H = delta le plus haut (positif)
                top_kgh = sorted(perfs_kgh, key=lambda x: x["delta"], reverse=True)[:3]
                
                st.markdown("#### 📉 Top 3 Baisses d'Heures")
                st.markdown(
                    _podium_html(top_heures, lambda p: POSTES_EMOJIS.get(p, p), inverse=True),
                    unsafe_allow_html=True
                )
            else:
                st.info(f"Données insuffisantes pour comparer à {ref_lbl}.")
                
    else:
        st.info("Aucune donnée disponible. Veuillez saisir des données dans la section 'Saisie de données'.")

# --- PAGES PAR POSTE (MODULAIRE) ---
elif page in ["Chaud", "Légumerie", "Sushi", "Découpe", "Mix", "Mélange", "Désinfection", "Traçabilité", "CF tampon"]:
    st.title(f"{POSTES_EMOJIS.get(page, page)}")
    
    if latest_week is not None:
        # On définit la colonne correspondante dans le CSV
        col_name = page # Pour 'Chaud', 'Légumerie', 'Sushi', 'Mix', 'Mélange'
        if page == "Découpe": col_name = "Découpe"

        # --- CSS transparent pour navigation + slider ---
        st.markdown("""
        <style>
        div[data-testid="stHorizontalBlock"] button {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            color: inherit !important;
        }
        div[data-testid="stHorizontalBlock"] button:hover {
            background: rgba(128,128,128,0.1) !important;
        }
        div[data-testid="stSlider"] { opacity: 0.75; transition: opacity 0.2s; }
        div[data-testid="stSlider"]:hover { opacity: 1; }
        </style>
        """, unsafe_allow_html=True)

        # --- NAVIGATION PAR SEMAINE (flèches) ---
        semaines_dispos_poste = sorted(data['Semaine'].dropna().unique().astype(int).tolist())
        n_weeks_total = len(semaines_dispos_poste)
        week_key = f"week_idx_{page}"
        if week_key not in st.session_state:
            st.session_state[week_key] = n_weeks_total - 1  # Dernière semaine par défaut

        nav_c1, nav_c2, nav_c3, nav_c4, nav_c5 = st.columns([1, 1, 4, 1, 1])
        with nav_c1:
            if st.button("⏮", key=f"first_{page}", help="Première semaine"):
                st.session_state[week_key] = 0
        with nav_c2:
            if st.button("◀", key=f"prev_{page}", help="Semaine précédente"):
                st.session_state[week_key] = max(0, st.session_state[week_key] - 1)
        with nav_c3:
            sem_display = semaines_dispos_poste[st.session_state[week_key]]
            idx_display = st.session_state[week_key]
            st.markdown(
                f"<p style='text-align:center;font-size:1.25rem;font-weight:700;margin:0.3rem 0'>"
                f"📅 Semaine {sem_display} &nbsp;<span style='font-size:0.8rem;color:#888'>({idx_display + 1}/{n_weeks_total})</span></p>",
                unsafe_allow_html=True
            )
        with nav_c4:
            if st.button("▶", key=f"next_{page}", help="Semaine suivante"):
                st.session_state[week_key] = min(n_weeks_total - 1, st.session_state[week_key] + 1)
        with nav_c5:
            if st.button("⏭", key=f"last_{page}", help="Dernière semaine"):
                st.session_state[week_key] = n_weeks_total - 1

        selected_semaine_poste = semaines_dispos_poste[st.session_state[week_key]]

        n_semaines = st.slider("Semaines à afficher :", min_value=4, max_value=12, value=6, step=2, key=f"nsem_{page}")

        # Recalcul des données locales pour ce poste selon les filtres
        latest_week = data[data['Semaine'] == selected_semaine_poste].iloc[0]
        data_jusqu_a_poste = data[data['Semaine'] <= selected_semaine_poste]
        last_6_weeks = data_jusqu_a_poste.tail(n_semaines)
        prev_week = last_6_weeks.iloc[-2] if len(last_6_weeks) > 1 else None

        # Pré-calcul val_h + delta_h
        val_h = latest_week[col_name] if col_name in latest_week else 0
        delta_h = None
        if prev_week is not None and col_name in prev_week:
            prev_val_h = prev_week[col_name]
            if prev_val_h > 0:
                delta_h = f"{((val_h - prev_val_h) / prev_val_h) * 100:.1f} %"

        # Pré-calcul variables kg/h (pour le graphique uniquement)
        kgh_col = f"{col_name}_kg_h"
        val_kgh = None
        prev_val_kgh = None
        prev2_val_kgh = None
        if kgh_col in latest_week:
            val_kgh = latest_week[kgh_col]
            prev_val_kgh = prev_week[kgh_col] if prev_week is not None and kgh_col in prev_week else None
            prev2_week = last_6_weeks.iloc[-3] if len(last_6_weeks) >= 3 else None
            prev2_val_kgh = prev2_week[kgh_col] if prev2_week is not None and kgh_col in prev2_week else None

        # --- GRAPHIQUE PLEINE LARGEUR ---
        if col_name in last_6_weeks.columns:
            if page == "Sushi":
                st.subheader(f"Kilos Sushi vs Kg/H - {page}")
            else:
                st.subheader(f"Comparaison Heures vs Kg/H - {page}")

            fig_poste = make_subplots(specs=[[{"secondary_y": True}]])

            c_pastel = COULEURS_POSTES.get(page, '#1f77b4')
            c_fonce = COULEURS_POSTES_FONCEES.get(page, '#ff7f0e')
            COULEURS_TEXTES_FONCEES = {
                'Chaud': '#801509',
                'Légumerie': '#345209',
                'Sushi': '#133554',
                'Découpe': '#452345',
                'Mix': '#7a4608',
                'Mélange': '#524b07',
                'Désinfection': '#0f5c45',
                'Traçabilité': '#413e66',
                'CF tampon': '#1f4a6b'
            }
            c_texte_fonce = COULEURS_TEXTES_FONCEES.get(page, '#000000')

            if page == "Sushi" and 'Kg Sushi' in last_6_weeks.columns:
                fig_poste.add_trace(
                    go.Bar(
                        x=last_6_weeks['Semaine'],
                        y=last_6_weeks['Kg Sushi'],
                        name="Kilos Sushi",
                        marker_color=c_pastel,
                        text=last_6_weeks['Kg Sushi'].fillna(0).round(1).astype(str) + " <i>kg</i>",
                        textposition='inside',
                        insidetextanchor='middle',
                        textfont=dict(weight='bold', color='black')
                    ),
                    secondary_y=False,
                )
            else:
                fig_poste.add_trace(
                    go.Bar(
                        x=last_6_weeks['Semaine'],
                        y=last_6_weeks[col_name],
                        name="Heures",
                        marker_color=c_pastel,
                        text=last_6_weeks[col_name].fillna(0).round(0).astype(int).astype(str) + " <i>h</i>",
                        textposition='inside',
                        insidetextanchor='middle',
                        textfont=dict(weight='bold', color='black')
                    ),
                    secondary_y=False,
                )

            if kgh_col in last_6_weeks.columns:
                fig_poste.add_trace(
                    go.Scatter(
                        x=last_6_weeks['Semaine'],
                        y=last_6_weeks[kgh_col],
                        name="Kg/H",
                        line=dict(color=c_fonce, width=3),
                        mode='lines+markers+text',
                        text=last_6_weeks[kgh_col].round(1).astype(str) + " <i>kg/h</i>",
                        textposition='top center',
                        textfont=dict(weight='bold', size=13, color=c_texte_fonce)
                    ),
                    secondary_y=True,
                )

                obj_kgh = OBJECTIFS_KGH_ETAPE1.get(page)
                if obj_kgh:
                    fig_poste.add_trace(
                        go.Scatter(
                            x=[last_6_weeks['Semaine'].min(), last_6_weeks['Semaine'].max()],
                            y=[obj_kgh, obj_kgh],
                            name="Obj. Kg/H",
                            mode='lines',
                            line=dict(color=c_fonce, dash='dash', width=2)
                        ),
                        secondary_y=True,
                    )

            fig_poste.update_layout(
                xaxis_title="Semaine",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            if page == "Sushi" and 'Kg Sushi' in last_6_weeks.columns:
                fig_poste.update_yaxes(title_text="Kilos", secondary_y=False, rangemode="tozero")
            elif page in ["Mix", "Mélange"]:
                fig_poste.update_yaxes(title_text="Heures", secondary_y=False, range=[0, 160])
            else:
                fig_poste.update_yaxes(title_text="Heures", secondary_y=False, rangemode="tozero")

            obj_kgh_poste = OBJECTIFS_KGH_ETAPE1.get(page, 0)
            max_kgh = last_6_weeks[kgh_col].max() if kgh_col in last_6_weeks.columns else 0
            if page == "Sushi":
                max_y2_poste = max(max_kgh * 1.3, obj_kgh_poste * 1.5, 5)
            else:
                max_y2_poste = max(max_kgh * 1.2, obj_kgh_poste * 1.2, 10)
            fig_poste.update_yaxes(title_text="Kg/H", secondary_y=True, range=[0, max_y2_poste])

            st.plotly_chart(fig_poste, use_container_width=True)
        else:
            st.warning(f"La colonne '{col_name}' est introuvable.")

        # --- MÉTRIQUES + HISTORIQUE ---
        st.subheader("Derniers chiffres")
        st.metric(label="Heures consommées", value=f"{val_h:.1f} h", delta=delta_h, delta_color="inverse")

        if page == "Sushi" and 'Kg Sushi' in latest_week.index:
            kg_sushi_val = latest_week['Kg Sushi'] if pd.notna(latest_week['Kg Sushi']) else 0
            delta_kg_sushi = None
            if prev_week is not None and 'Kg Sushi' in prev_week and prev_week['Kg Sushi'] > 0:
                delta_kg_sushi = f"{((kg_sushi_val - prev_week['Kg Sushi']) / prev_week['Kg Sushi']) * 100:.1f} %"
            st.metric(label="Kilos Sushi", value=f"{kg_sushi_val:.1f} kg", delta=delta_kg_sushi)

        st.write("**Historique (6 sem.) :**")
        display_cols = ['Semaine']
        if col_name in last_6_weeks.columns:
            display_cols.append(col_name)
        if page == "Sushi" and 'Kg Sushi' in last_6_weeks.columns:
            display_cols.append('Kg Sushi')
        if f"{col_name}_kg_h" in last_6_weeks.columns:
            display_cols.append(f"{col_name}_kg_h")
        st.dataframe(last_6_weeks[display_cols])
    else:
        st.info("Aucune donnée disponible.")

# --- PAGE DE SAISIE (PORTE OUVERTE) ---
elif page == "Saisie de données":
    st.title("📝 Saisie de données")
    
    st.subheader("➕ Ajouter ou modifier une semaine")
    with st.form("new_data_form"):
        # Valeur par défaut pour la nouvelle semaine
        default_semaine = int(latest_week['Semaine']) + 1 if latest_week is not None else 1
        
        col_s, col_k, col_t = st.columns(3)
        with col_s:
            new_semaine = st.number_input("Numéro de la semaine", min_value=1, max_value=53, value=default_semaine)
        with col_k:
            new_kilos = st.number_input("Total Kilos Produits (incluant Sushi)", min_value=0.0, step=10.0)
        with col_t:
            new_taux = st.number_input("Taux horaire (€/h)", min_value=0.0, step=0.5, value=25.0)
            
        st.subheader("Heures consommées par poste")
        col1, col2, col3 = st.columns(3)
        with col1:
            new_chaud = st.number_input(f"{POSTES_EMOJIS['Chaud']} Heures", min_value=0.0, step=0.5)
            new_leg = st.number_input(f"{POSTES_EMOJIS['Légumerie']} Heures", min_value=0.0, step=0.5)
            new_desinfection = st.number_input(f"{POSTES_EMOJIS['Désinfection']} Heures", min_value=0.0, step=0.5)
        with col2:
            col_s_h, col_s_k = st.columns(2)
            with col_s_h:
                new_sushi = st.number_input(f"{POSTES_EMOJIS['Sushi']} Heures", min_value=0.0, step=0.5)
            with col_s_k:
                new_sushi_kg = st.number_input(f"{POSTES_EMOJIS['Sushi']} Kilos", min_value=0.0, step=0.5)
            new_decoupe = st.number_input(f"{POSTES_EMOJIS['Découpe']} Heures", min_value=0.0, step=0.5)
            new_tracabilite = st.number_input(f"{POSTES_EMOJIS['Traçabilité']} Heures", min_value=0.0, step=0.5)
        with col3:
            new_mix = st.number_input(f"{POSTES_EMOJIS['Mix']} Heures", min_value=0.0, step=0.5)
            new_melange = st.number_input(f"{POSTES_EMOJIS['Mélange']} Heures", min_value=0.0, step=0.5)
            new_cf_tampon = st.number_input(f"{POSTES_EMOJIS['CF tampon']} Heures", min_value=0.0, step=0.5)
        
        # Calcul dynamique avant la soumission pour l'affichage
        total_heure = new_chaud + new_leg + new_sushi + new_decoupe + new_mix + new_melange + new_desinfection + new_tracabilite + new_cf_tampon
        st.info(f"⏱️ **Total des heures renseignées : {total_heure} h**")
        
        submitted = st.form_submit_button("Enregistrer la semaine")
        if submitted:
            # Calculs de la productivité
            kilos_hors_sushi = new_kilos - new_sushi_kg
            
            doc_data = {
                'Semaine': int(new_semaine),
                'Total heure': float(total_heure),
                'Heures Chaud': float(new_chaud),
                'Heures Légumerie': float(new_leg),
                'Heure Sushi': float(new_sushi),
                'Heures Découpe': float(new_decoupe),
                'Heures Mix': float(new_mix),
                'Heures Mélange': float(new_melange),
                'Heures Désinfection': float(new_desinfection),
                'Heures Traçabilité': float(new_tracabilite),
                'Heures CF tampon': float(new_cf_tampon),
                
                # Nouveaux champs saisis & stockés
                'Kg produits global': float(new_kilos),
                'Kg Sushi': float(new_sushi_kg),
                'Taux horaire': float(new_taux),
                
                # Productivité globale
                'Kg/H ': float(new_kilos / total_heure) if total_heure > 0 else 0.0,
                '€/kg (Mep global)': float((total_heure * new_taux) / new_kilos) if new_kilos > 0 else 0.0,
                
                # Productivité par poste (déduction faite des kilos sushi pour les autres postes)
                'Chaud kg/H': float(kilos_hors_sushi / new_chaud) if new_chaud > 0 else 0.0,
                'Légumerie KG/H': float(kilos_hors_sushi / new_leg) if new_leg > 0 else 0.0,
                'Découpe KG/H': float(kilos_hors_sushi / new_decoupe) if new_decoupe > 0 else 0.0,
                'Kg/H Sushi': float(new_sushi_kg / new_sushi) if new_sushi > 0 else 0.0,
                'Mix KG/H': float(kilos_hors_sushi / new_mix) if new_mix > 0 else 0.0,
                'Mélange KG/H': float(kilos_hors_sushi / new_melange) if new_melange > 0 else 0.0,
                'Désinfection KG/H': float(kilos_hors_sushi / new_desinfection) if new_desinfection > 0 else 0.0,
                'Traçabilité KG/H': float(kilos_hors_sushi / new_tracabilite) if new_tracabilite > 0 else 0.0,
                'CF tampon KG/H': float(kilos_hors_sushi / new_cf_tampon) if new_cf_tampon > 0 else 0.0,
                
                # Ancien champ conservé à 0 pour la compatibilité
                'Commandes': 0,
                'UVC/H par ETP': 0.0
            }
            
            # --- Sauvegarde dans Firestore ---
            doc_id = str(int(new_semaine))
            _db.collection(FIRESTORE_COLLECTION).document(doc_id).set(doc_data)

            # Vider le cache pour forcer le rechargement des données sur le dashboard
            st.cache_data.clear()

            st.success(f"✅ Données pour la S{int(new_semaine)} enregistrées avec succès dans Firestore !")
            st.info("Retournez sur le 'Dashboard Global' pour voir la mise à jour.")

    st.divider()
    st.subheader("🗑️ Supprimer une semaine")
    with st.expander("Voir les options de suppression", expanded=False):
        exist_weeks = sorted(data['Semaine'].dropna().unique().astype(int).tolist()) if not data.empty else []
        if exist_weeks:
            semaine_a_supprimer = st.selectbox("Choisir la semaine à supprimer", options=exist_weeks)
            if st.button("Confirmer la suppression", type="primary"):
                _db.collection(FIRESTORE_COLLECTION).document(str(semaine_a_supprimer)).delete()
                st.cache_data.clear()
                st.success(f"Semaine {semaine_a_supprimer} supprimée avec succès !")
                st.rerun()
        else:
            st.info("Aucune semaine disponible à supprimer.")

# ─────────────────────────────────────────────────────────
# PAGE : FICHES TECHNIQUES RECETTES
# ─────────────────────────────────────────────────────────
elif page == "Fiches Techniques":
    st.title("📖 Fiches Techniques Recettes")

    recettes_list = load_recettes()
    prix_dict     = load_ingredients()

    tab_fiche, tab_prod, tab_ajout = st.tabs(["📋 Fiche Recette", "🏭 Plan de Production", "➕ Ajouter une recette"])

    # ══════════════════════════════════════════════════════
    # ONGLET 1 : FICHE RECETTE
    # ══════════════════════════════════════════════════════
    with tab_fiche:
        if not recettes_list:
            st.info("Aucune recette trouvée. Utilisez l'onglet 'Ajouter une recette'.")
        else:
            noms = [r["nom"] for r in recettes_list]
            nom_choisi = st.selectbox("Sélectionner une recette :", noms, key="sel_fiche")
            recette = next((r for r in recettes_list if r["nom"] == nom_choisi), None)

            if recette:
                rows, cout_total, cout_couvert = _calcul_fiche(recette, prix_dict)

                # ── KPIs ──────────────────────────────────
                k1, k2, k3, k4 = st.columns(4)
                k1.metric("Catégorie", recette.get("categorie", "—"))
                k2.metric("Couverts", recette.get("nb_couverts", "—"))
                k3.metric("Coût matière total", f"{cout_total:.2f} €")
                k4.metric("Coût / couvert", f"{cout_couvert:.2f} €")

                st.divider()

                # ── Tableau ingrédients ───────────────────
                st.subheader("Composition")
                df_ing = pd.DataFrame(rows)
                st.dataframe(
                    df_ing, use_container_width=True, hide_index=True,
                    column_config={
                        "Brut (kg)":          st.column_config.NumberColumn(format="%.3f kg"),
                        "Net final (kg)":     st.column_config.NumberColumn(format="%.3f kg"),
                        "Prix/kg brut (€)":   st.column_config.NumberColumn(format="%.2f €"),
                        "Coût (€)":           st.column_config.NumberColumn(format="%.2f €"),
                    }
                )

                # ── Répartition coûts ─────────────────────
                if rows:
                    fig_pie = px.pie(
                        df_ing, values="Coût (€)", names="Ingrédient",
                        title="Répartition du coût matière",
                        hole=0.4,
                        color_discrete_sequence=px.colors.qualitative.Pastel
                    )
                    fig_pie.update_layout(height=300, margin=dict(t=40, b=0))
                    _, col_pie, _ = st.columns([1, 2, 1])
                    with col_pie:
                        st.plotly_chart(fig_pie, use_container_width=True)

                st.divider()

                # ── Prix de vente + Marge dynamique ───────
                st.subheader("💰 Prix de vente & Marge")

                # Recommandations automatiques
                rec_eco     = cout_couvert / 0.35 if cout_couvert > 0 else 0
                rec_std     = cout_couvert / 0.28 if cout_couvert > 0 else 0
                rec_premium = cout_couvert / 0.20 if cout_couvert > 0 else 0

                rco1, rco2, rco3 = st.columns(3)
                rco1.metric("Éco (35% food cost)",     f"{rec_eco:.2f} €",     help="Prix minimum recommandé")
                rco2.metric("Standard (28% food cost)", f"{rec_std:.2f} €",    delta="Recommandé", help="Objectif standard restauration")
                rco3.metric("Premium (20% food cost)",  f"{rec_premium:.2f} €", help="Positionnement haut de gamme")

                # Saisie du prix de vente
                prix_vente_saved = float(recette.get("prix_vente_couvert", rec_std))
                prix_vente = st.number_input(
                    "Prix de vente (€ / couvert) :",
                    min_value=0.0, value=prix_vente_saved,
                    step=0.5, format="%.2f",
                    key="prix_vente_input"
                )

                if prix_vente > 0:
                    marge_brute  = prix_vente - cout_couvert
                    marge_pct    = (marge_brute / prix_vente) * 100
                    food_cost_pct = (cout_couvert / prix_vente) * 100

                    # Couleur food cost
                    if food_cost_pct < 28:
                        fc_color, fc_icon = "#27ae60", "🟢"
                    elif food_cost_pct < 35:
                        fc_color, fc_icon = "#f39c12", "🟡"
                    else:
                        fc_color, fc_icon = "#e74c3c", "🔴"

                    m1, m2, m3 = st.columns(3)
                    m1.metric("Coût matière", f"{cout_couvert:.2f} €", f"{food_cost_pct:.1f}% du PV")
                    m2.metric("Marge brute", f"{marge_brute:.2f} €",  f"{marge_pct:.1f}%")
                    m3.metric("Food cost", f"{food_cost_pct:.1f}%", delta=f"Objectif < 30%",
                              delta_color="inverse" if food_cost_pct > 30 else "normal")

                    st.markdown(
                        f"<div style='background:rgba(0,0,0,0.04);border-radius:8px;padding:10px 16px;margin:6px 0'>"
                        f"{fc_icon} <b>Food cost :</b> <span style='color:{fc_color};font-weight:800'>{food_cost_pct:.1f}%</span>"
                        f" &nbsp;|&nbsp; <b>Marge :</b> {marge_pct:.1f}%"
                        f" &nbsp;|&nbsp; <b>Marge / couverts ({recette.get('nb_couverts',1)}) :</b> {marge_brute * recette.get('nb_couverts',1):.2f} €"
                        f"</div>",
                        unsafe_allow_html=True
                    )

                if st.button("💾 Sauvegarder le prix de vente", key="save_pv"):
                    _db.collection(COLLECTION_RECETTES).document(recette["id"]).set(
                        {"prix_vente_couvert": prix_vente}, merge=True
                    )
                    st.cache_data.clear()
                    st.success("Prix de vente sauvegardé !")
                    st.rerun()

    # ══════════════════════════════════════════════════════
    # ONGLET 2 : PLAN DE PRODUCTION
    # ══════════════════════════════════════════════════════
    with tab_prod:
        st.subheader("🏭 Plan de Production")
        if not recettes_list:
            st.info("Ajoutez d'abord des recettes.")
        else:
            noms_prod = [r["nom"] for r in recettes_list]
            recettes_choisies = st.multiselect(
                "Sélectionner les recettes à produire :",
                noms_prod, default=noms_prod[:2] if len(noms_prod) >= 2 else noms_prod,
                key="prod_sel"
            )

            if recettes_choisies:
                st.markdown("**Quantités à produire (nb de couverts) :**")
                qtys = {}
                cols_q = st.columns(min(len(recettes_choisies), 4))
                for i, nom_r in enumerate(recettes_choisies):
                    rec_r = next((r for r in recettes_list if r["nom"] == nom_r), None)
                    nb_base = rec_r.get("nb_couverts", 1) if rec_r else 1
                    with cols_q[i % 4]:
                        qtys[nom_r] = st.number_input(
                            nom_r, min_value=1, value=nb_base, step=1, key=f"qty_{nom_r}"
                        )

                st.divider()

                # Calcul plan
                plan_rows = []
                ingredients_consolides = {}
                cout_total_prod = 0.0

                for nom_r in recettes_choisies:
                    rec_r = next((r for r in recettes_list if r["nom"] == nom_r), None)
                    if not rec_r:
                        continue
                    nb_base = rec_r.get("nb_couverts", 1) or 1
                    nb_prod = qtys[nom_r]
                    facteur = nb_prod / nb_base
                    _, cout_rec, cout_cov = _calcul_fiche(rec_r, prix_dict)
                    cout_total_r = round(cout_cov * nb_prod, 2)
                    cout_total_prod += cout_total_r

                    pv = float(rec_r.get("prix_vente_couvert", 0))
                    ca_r = round(pv * nb_prod, 2) if pv > 0 else 0.0
                    marge_r = round(ca_r - cout_total_r, 2) if pv > 0 else 0.0

                    plan_rows.append({
                        "Recette": nom_r,
                        "Couverts": nb_prod,
                        "Coût/couvert (€)": round(cout_cov, 2),
                        "Coût total (€)": cout_total_r,
                        "CA prévu (€)": ca_r if pv > 0 else "—",
                        "Marge (€)": marge_r if pv > 0 else "—",
                    })

                    # Consolidation ingrédients
                    for ing in rec_r.get("ingredients", []):
                        nom_i = ing["nom"]
                        brut_i = float(ing.get("poids_brut_kg", 0)) * facteur
                        prix_i = float(prix_dict.get(nom_i, {}).get("prix_unitaire", ing.get("prix_unitaire", 0)))
                        cout_i = round(brut_i * prix_i, 2)
                        if nom_i in ingredients_consolides:
                            ingredients_consolides[nom_i]["Quantité brute (kg)"] += brut_i
                            ingredients_consolides[nom_i]["Coût (€)"] += cout_i
                        else:
                            ingredients_consolides[nom_i] = {
                                "Ingrédient": nom_i,
                                "Quantité brute (kg)": brut_i,
                                "Prix/kg (€)": prix_i,
                                "Coût (€)": cout_i,
                            }

                # Tableau récapitulatif
                st.markdown("#### Récapitulatif par recette")
                df_plan = pd.DataFrame(plan_rows)
                st.dataframe(df_plan, use_container_width=True, hide_index=True)

                # KPIs production
                ca_total = sum(r.get("CA prévu (€)", 0) for r in plan_rows if isinstance(r.get("CA prévu (€)"), float))
                marge_total = sum(r.get("Marge (€)", 0) for r in plan_rows if isinstance(r.get("Marge (€)"), float))
                pk1, pk2, pk3 = st.columns(3)
                pk1.metric("Coût total production", f"{cout_total_prod:.2f} €")
                if ca_total > 0:
                    pk2.metric("CA total prévu", f"{ca_total:.2f} €")
                    pk3.metric("Marge totale prévue", f"{marge_total:.2f} €")

                st.divider()

                # Besoins ingrédients consolidés
                st.markdown("#### Besoins en ingrédients (total)")
                df_ing_cons = pd.DataFrame(list(ingredients_consolides.values()))
                df_ing_cons["Quantité brute (kg)"] = df_ing_cons["Quantité brute (kg)"].round(3)
                df_ing_cons["Coût (€)"] = df_ing_cons["Coût (€)"].round(2)
                df_ing_cons = df_ing_cons.sort_values("Coût (€)", ascending=False)
                st.dataframe(df_ing_cons, use_container_width=True, hide_index=True)

    # ══════════════════════════════════════════════════════
    # ONGLET 3 : AJOUTER UNE RECETTE
    # ══════════════════════════════════════════════════════
    with tab_ajout:
        st.subheader("Ajouter une nouvelle recette")
        with st.form("form_recette"):
            c1, c2, c3 = st.columns(3)
            with c1:
                f_nom = st.text_input("Nom de la recette")
            with c2:
                f_cat = st.selectbox("Catégorie", ["Chaud", "Légumerie", "Sushi", "Mix", "Mélange", "Désinfection", "Autre"])
            with c3:
                f_couverts = st.number_input("Nombre de couverts", min_value=1, value=6, step=1)

            st.markdown("**Ingrédients** — saisir le poids **brut** acheté + taux de perte")
            st.caption("Coût = poids brut × prix/kg  |  Net final = brut × (1 - perte%/100)")

            # En-têtes colonnes
            hc1, hc2, hc3, hc4 = st.columns([3, 1.5, 1.5, 1.5])
            hc1.markdown("**Ingrédient**")
            hc2.markdown("**Brut (kg)**")
            hc3.markdown("**Perte (%)**")
            hc4.markdown("**Prix/kg (€)**")

            ing_rows = []
            for i in range(10):
                ca, cb, cc, cd = st.columns([3, 1.5, 1.5, 1.5])
                with ca: nom_i   = st.text_input(f"ing_{i}", key=f"ing_nom_{i}",  label_visibility="collapsed")
                with cb: brut_i  = st.number_input("b", key=f"ing_brut_{i}",  min_value=0.0, step=0.01,  format="%.3f", label_visibility="collapsed")
                with cc: perte_i = st.number_input("p", key=f"ing_perte_{i}", min_value=0.0, max_value=100.0, step=0.5, format="%.1f", label_visibility="collapsed")
                with cd: prix_i  = st.number_input("px", key=f"ing_prix_{i}",  min_value=0.0, step=0.1,  format="%.2f", label_visibility="collapsed")
                if nom_i.strip():
                    net_calc = round(brut_i * (1 - perte_i / 100), 4)
                    ing_rows.append({
                        "nom": nom_i.strip(),
                        "poids_brut_kg": brut_i,
                        "taux_perte_pct": perte_i,
                        "prix_unitaire": prix_i,
                    })

            submitted_rec = st.form_submit_button("Enregistrer la recette", type="primary")
            if submitted_rec:
                if not f_nom.strip():
                    st.error("Le nom de la recette est obligatoire.")
                elif not ing_rows:
                    st.error("Ajoutez au moins un ingrédient.")
                else:
                    rec_id = str(uuid.uuid4())
                    _db.collection(COLLECTION_RECETTES).document(rec_id).set({
                        "nom": f_nom.strip(), "categorie": f_cat,
                        "nb_couverts": int(f_couverts),
                        "ingredients": ing_rows,
                        "created_at": str(date.today())
                    })
                    for ing in ing_rows:
                        ing_id = ing["nom"].lower().replace(" ", "_").replace("'", "")
                        _db.collection(COLLECTION_INGREDIENTS).document(ing_id).set({
                            "nom": ing["nom"], "prix_unitaire": ing["prix_unitaire"],
                            "unite": "kg", "fournisseur": "", "updated_at": str(date.today())
                        }, merge=True)
                    st.cache_data.clear()
                    st.success(f"Recette '{f_nom}' enregistrée !")
                    st.rerun()

# ─────────────────────────────────────────────────────────
# PAGE : FACTURES
# ─────────────────────────────────────────────────────────
elif page == "Factures":
    st.title("🧾 Gestion des Factures")

    factures_list = load_factures()
    prix_dict     = load_ingredients()

    # ── Onglets Scanner / Manuel / Liste ──────────────────
    tab_scan, tab_manuel, tab_liste = st.tabs(["📸 Scanner une facture", "✏️ Saisie manuelle", "📋 Liste des factures"])

    with tab_scan:
        st.subheader("Extraction automatique par photo")
        st.info("Prenez une photo ou importez l'image de votre facture. Claude Opus analysera et extraira les données automatiquement.")

        uploaded = st.file_uploader("Choisir une image de facture", type=["jpg", "jpeg", "png", "webp"], key="facture_upload")
        camera   = st.camera_input("Ou prendre une photo", key="facture_camera")
        img_src  = uploaded or camera

        if img_src and st.button("Analyser la facture", type="primary"):
            with st.spinner("Claude analyse la facture..."):
                media_type = "image/jpeg"
                if img_src.name.endswith(".png") if hasattr(img_src, "name") else False:
                    media_type = "image/png"
                result = _extract_facture_vision(img_src.getvalue(), media_type)

            if "error" in result:
                st.error(f"Erreur : {result['error']}")
            else:
                st.session_state["ocr_result"] = result
                st.success("Extraction réussie ! Vérifiez et validez les données ci-dessous.")

        # Formulaire de validation OCR
        if "ocr_result" in st.session_state:
            ocr = st.session_state["ocr_result"]
            st.divider()
            st.subheader("Vérification des données extraites")
            with st.form("form_ocr_validation"):
                co1, co2, co3 = st.columns(3)
                with co1: v_fourn  = st.text_input("Fournisseur", value=ocr.get("fournisseur", ""))
                with co2: v_date   = st.text_input("Date (YYYY-MM-DD)", value=ocr.get("date", str(date.today())))
                with co3: v_num    = st.text_input("N° facture", value=ocr.get("numero", ""))

                st.markdown("**Lignes de la facture**")
                lignes_valid = []
                for i, lg in enumerate(ocr.get("lignes", [])[:20]):
                    lc1, lc2, lc3, lc4 = st.columns([3, 1, 1.5, 1.5])
                    with lc1: art = st.text_input(f"Article {i+1}", value=lg.get("article",""), key=f"art_{i}")
                    with lc2: qty = st.number_input("Qté", value=float(lg.get("quantite",0)), key=f"qty_{i}", step=0.1)
                    with lc3: pu  = st.number_input("Prix unitaire", value=float(lg.get("prix_unitaire",0)), key=f"pu_{i}", step=0.01)
                    with lc4: tht = st.number_input("Total HT", value=float(lg.get("total_ht",0)), key=f"tht_{i}", step=0.01)
                    if art.strip():
                        lignes_valid.append({"article": art, "quantite": qty, "unite": lg.get("unite","kg"), "prix_unitaire": pu, "total_ht": tht})

                cf1, cf2, cf3 = st.columns(3)
                with cf1: v_tht = st.number_input("Total HT (€)", value=float(ocr.get("total_ht", 0)), step=0.01)
                with cf2: v_tva = st.number_input("TVA (€)",      value=float(ocr.get("tva", 0)), step=0.01)
                with cf3: v_ttc = st.number_input("Total TTC (€)",value=float(ocr.get("total_ttc", 0)), step=0.01)

                col_save, col_prix = st.columns(2)
                with col_save: save_btn  = st.form_submit_button("💾 Enregistrer la facture", type="primary")
                with col_prix: maj_btn   = st.form_submit_button("💰 Enregistrer + MAJ prix ingrédients")

                if save_btn or maj_btn:
                    fac_doc = {
                        "fournisseur": v_fourn, "date": v_date, "numero": v_num,
                        "lignes": lignes_valid,
                        "total_ht": v_tht, "tva": v_tva, "total_ttc": v_ttc,
                        "statut": "validée", "created_at": str(date.today())
                    }
                    _db.collection(COLLECTION_FACTURES).document(str(uuid.uuid4())).set(fac_doc)
                    if maj_btn:
                        for lg in lignes_valid:
                            if lg["article"].strip() and lg["prix_unitaire"] > 0:
                                ing_id = lg["article"].lower().replace(" ", "_").replace("'", "")
                                _db.collection(COLLECTION_INGREDIENTS).document(ing_id).set({
                                    "nom": lg["article"], "prix_unitaire": lg["prix_unitaire"],
                                    "unite": lg.get("unite", "kg"),
                                    "fournisseur": v_fourn, "updated_at": str(date.today())
                                }, merge=True)
                        st.success("Facture enregistrée et prix ingrédients mis à jour !")
                    else:
                        st.success("Facture enregistrée !")
                    del st.session_state["ocr_result"]
                    st.cache_data.clear()
                    st.rerun()

    with tab_manuel:
        st.subheader("Saisie manuelle d'une facture")
        with st.form("form_facture_manuelle"):
            m1, m2, m3 = st.columns(3)
            with m1: m_fourn = st.text_input("Fournisseur")
            with m2: m_date  = st.text_input("Date (YYYY-MM-DD)", value=str(date.today()))
            with m3: m_num   = st.text_input("N° facture")

            st.markdown("**Lignes**")
            m_lignes = []
            for i in range(8):
                ml1, ml2, ml3, ml4, ml5 = st.columns([3, 1, 1, 1.5, 1.5])
                with ml1: m_art = st.text_input(f"Article {i+1}", key=f"m_art_{i}", label_visibility="collapsed" if i>0 else "visible")
                with ml2: m_qty = st.number_input("Qté", key=f"m_qty_{i}", min_value=0.0, step=0.1, label_visibility="collapsed" if i>0 else "visible")
                with ml3: m_uni = st.text_input("Unité", key=f"m_uni_{i}", value="kg", label_visibility="collapsed" if i>0 else "visible")
                with ml4: m_pu  = st.number_input("PU €", key=f"m_pu_{i}",  min_value=0.0, step=0.01, label_visibility="collapsed" if i>0 else "visible")
                with ml5: m_tht = st.number_input("Total HT", key=f"m_tht_{i}", min_value=0.0, step=0.01, label_visibility="collapsed" if i>0 else "visible")
                if m_art.strip():
                    m_lignes.append({"article": m_art, "quantite": m_qty, "unite": m_uni, "prix_unitaire": m_pu, "total_ht": m_tht})

            mt1, mt2, mt3 = st.columns(3)
            with mt1: m_tht_tot = st.number_input("Total HT (€)", min_value=0.0, step=0.01, key="m_tht_tot")
            with mt2: m_tva_tot = st.number_input("TVA (€)",       min_value=0.0, step=0.01, key="m_tva_tot")
            with mt3: m_ttc_tot = st.number_input("Total TTC (€)", min_value=0.0, step=0.01, key="m_ttc_tot")

            m_sub = st.form_submit_button("Enregistrer", type="primary")
            if m_sub:
                if not m_fourn.strip():
                    st.error("Le fournisseur est obligatoire.")
                else:
                    _db.collection(COLLECTION_FACTURES).document(str(uuid.uuid4())).set({
                        "fournisseur": m_fourn, "date": m_date, "numero": m_num,
                        "lignes": m_lignes,
                        "total_ht": m_tht_tot, "tva": m_tva_tot, "total_ttc": m_ttc_tot,
                        "statut": "validée", "created_at": str(date.today())
                    })
                    st.cache_data.clear()
                    st.success("Facture enregistrée !")
                    st.rerun()

    with tab_liste:
        st.subheader("Historique des factures")
        if not factures_list:
            st.info("Aucune facture enregistrée.")
        else:
            # Tableau récapitulatif
            df_fac = pd.DataFrame([{
                "Date": f.get("date", ""),
                "Fournisseur": f.get("fournisseur", ""),
                "N° Facture": f.get("numero", ""),
                "Total HT (€)": f.get("total_ht", 0),
                "TVA (€)": f.get("tva", 0),
                "Total TTC (€)": f.get("total_ttc", 0),
                "Statut": f.get("statut", ""),
            } for f in factures_list])
            st.dataframe(df_fac, use_container_width=True, hide_index=True,
                column_config={
                    "Total HT (€)":  st.column_config.NumberColumn(format="%.2f €"),
                    "TVA (€)":       st.column_config.NumberColumn(format="%.2f €"),
                    "Total TTC (€)": st.column_config.NumberColumn(format="%.2f €"),
                })

            # KPIs achats
            st.divider()
            total_ht_all  = sum(f.get("total_ht", 0)  for f in factures_list)
            total_ttc_all = sum(f.get("total_ttc", 0) for f in factures_list)
            nb_fourn = len(set(f.get("fournisseur","") for f in factures_list))
            fk1, fk2, fk3 = st.columns(3)
            fk1.metric("Total achats HT", f"{total_ht_all:.2f} €")
            fk2.metric("Total achats TTC", f"{total_ttc_all:.2f} €")
            fk3.metric("Fournisseurs distincts", nb_fourn)

            # Détail facture sélectionnée
            st.divider()
            choix_fac = st.selectbox("Voir le détail d'une facture :",
                [f"{f.get('date','')} — {f.get('fournisseur','')} — {f.get('numero','')}" for f in factures_list])
            idx = [f"{f.get('date','')} — {f.get('fournisseur','')} — {f.get('numero','')}" for f in factures_list].index(choix_fac)
            fac_detail = factures_list[idx]
            lignes_detail = fac_detail.get("lignes", [])
            if lignes_detail:
                st.dataframe(pd.DataFrame(lignes_detail), use_container_width=True, hide_index=True)
