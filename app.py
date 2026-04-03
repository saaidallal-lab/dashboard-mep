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
COLLECTION_VENTES     = "ventes"
COLLECTION_CONNEXIONS = "connexions"

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
        .app-card {
            border-radius: 16px;
            padding: 22px 20px 18px;
            margin-bottom: 6px;
            transition: transform .15s;
        }
        .app-card:hover { transform: translateY(-2px); }
        .app-tag {
            display: inline-block;
            background: rgba(255,255,255,0.25);
            color: white;
            font-size: 0.7em;
            font-weight: 600;
            padding: 2px 9px;
            border-radius: 12px;
            margin-top: 6px;
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

@st.cache_data(ttl=60)
def load_ventes():
    docs = _db.collection(COLLECTION_VENTES).stream()
    return sorted([{"id": d.id, **d.to_dict()} for d in docs],
                  key=lambda x: x.get("date", ""), reverse=True)

@st.cache_data(ttl=60)
def load_connexions():
    docs = _db.collection(COLLECTION_CONNEXIONS).stream()
    return {d.id: d.to_dict() for d in docs}

def _get_semaine_iso(date_str):
    """Retourne le numéro de semaine ISO pour une date YYYY-MM-DD."""
    try:
        from datetime import datetime
        return datetime.strptime(date_str, "%Y-%m-%d").isocalendar()[1]
    except Exception:
        return None

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


def _extract_facture_claude(image_bytes: bytes, media_type: str = "image/jpeg") -> dict:
    """Utilise Claude Vision pour extraire TOUTES les lignes d'une facture/BL avec haute précision."""
    import re as _re
    import json as _json
    import base64 as _b64

    try:
        import anthropic as _anthropic
    except ImportError:
        return {"error": "Package 'anthropic' non installé. Ajoutez-le dans requirements.txt."}

    # Récupération de la clé API
    api_key = None
    try:
        api_key = st.secrets.get("ANTHROPIC_API_KEY")
    except Exception:
        pass
    if not api_key:
        import os
        api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return {"error": "Clé ANTHROPIC_API_KEY introuvable dans les secrets Streamlit."}

    try:
        client = _anthropic.Anthropic(api_key=api_key)
        img_b64 = _b64.standard_b64encode(image_bytes).decode("utf-8")

        prompt = """Analyse ce document (facture, bon de livraison, ou tout autre document commercial) et extrait TOUTES les données.

Retourne UNIQUEMENT un objet JSON valide (sans texte avant ni après, sans bloc markdown) avec cette structure :
{
  "fournisseur": "nom complet du fournisseur",
  "date": "YYYY-MM-DD",
  "numero": "numéro de facture ou BL",
  "lignes": [
    {
      "reference": "code/référence article si présent",
      "article": "description complète du produit",
      "quantite_commandee": 0.0,
      "unite_commande": "COL/KG/SAC/BTE/SEA/etc",
      "quantite_livree": 0.0,
      "unite_livree": "KG/SAC/etc",
      "prix_unitaire": 0.0,
      "total_ht": 0.0
    }
  ],
  "total_colis": 0,
  "total_kg": 0.0,
  "total_ht": 0.0,
  "tva": 0.0,
  "total_ttc": 0.0
}

Règles impératives :
- Extrait ABSOLUMENT TOUTES les lignes produits sans en oublier une seule
- La virgule est un séparateur décimal (ex : 6,800 = 6.8 en JSON)
- Si une valeur est absente du document, utilise 0 ou ""
- La date doit être au format YYYY-MM-DD (convertis si nécessaire)
- N'inclus pas les lignes de total ou d'en-tête, uniquement les lignes produits"""

        import time as _time
        payload = [{
            "role": "user",
            "content": [
                {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": img_b64}},
                {"type": "text", "text": prompt},
            ],
        }]
        message = None
        for attempt in range(3):
            try:
                message = client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=4096,
                    messages=payload,
                )
                break
            except Exception as _e:
                if attempt < 2 and "overloaded" in str(_e).lower():
                    _time.sleep(5 * (attempt + 1))
                else:
                    raise
        if message is None:
            return {"error": "Serveur Anthropic surchargé. Réessaie dans quelques secondes."}

        raw = message.content[0].text.strip()
        # Nettoyer les éventuels blocs markdown ```json ... ```
        raw = _re.sub(r'^```(?:json)?\s*', '', raw)
        raw = _re.sub(r'\s*```$', '', raw)

        data = _json.loads(raw)

        # Normaliser les lignes — ref séparée, total HT calculé si absent
        lignes_norm = []
        for lg in data.get("lignes", []):
            ref  = str(lg.get("reference", "")).strip()
            desc = str(lg.get("article", "")).strip()
            qty  = float(lg.get("quantite_livree") or lg.get("quantite_commandee") or 0)
            prix = float(lg.get("prix_unitaire") or 0)
            total_line = float(lg.get("total_ht") or 0)
            if total_line == 0 and qty > 0 and prix > 0:
                total_line = round(qty * prix, 2)
            lignes_norm.append({
                "reference":    ref,
                "article":      desc,
                "quantite":     qty,
                "unite":        str(lg.get("unite_livree") or lg.get("unite_commande") or "kg"),
                "prix_unitaire": prix,
                "total_ht":     total_line,
            })

        # Total facture : depuis l'extraction ou somme des lignes
        total_ht_extrait = float(data.get("total_ht") or 0)
        total_ht_calc    = round(sum(lg["total_ht"] for lg in lignes_norm), 2)
        total_ht_final   = total_ht_extrait if total_ht_extrait > 0 else total_ht_calc
        tva_val  = float(data.get("tva") or 0)
        ttc_val  = float(data.get("total_ttc") or 0)

        from datetime import date as _date
        return {
            "fournisseur": str(data.get("fournisseur", "")),
            "date":        str(data.get("date", "") or str(_date.today())),
            "numero":      str(data.get("numero", "")),
            "lignes":      lignes_norm,
            "total_ht":    total_ht_final,
            "tva":         tva_val,
            "total_ttc":   ttc_val,
        }

    except _json.JSONDecodeError as e:
        return {"error": f"Réponse Claude non parsable en JSON : {e}"}
    except Exception as e:
        return {"error": str(e)}


def _extract_facture_vision(image_bytes: bytes) -> dict:
    """Utilise Google Cloud Vision (gratuit 1000 req/mois) pour extraire le texte d'une facture."""
    try:
        from google.cloud import vision as gvision
    except ImportError:
        return {"error": "Package 'google-cloud-vision' non installé. Ajoutez-le dans requirements.txt."}
    try:
        creds = _get_gcp_credentials()
        vision_client = gvision.ImageAnnotatorClient(credentials=creds)
        image = gvision.Image(content=image_bytes)
        response = vision_client.document_text_detection(image=image)
        if response.error.message:
            return {"error": f"Vision API : {response.error.message}"}
        full_text = response.full_text_annotation.text
        if not full_text:
            return {"error": "Aucun texte détecté dans l'image."}
        return _parse_facture_text(full_text)
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


# ─────────────────────────────────────────────────────────
# FONCTIONS INTÉGRATIONS POS / UBER EATS
# ─────────────────────────────────────────────────────────
def _pos_request(method, url, headers=None, params=None, timeout=8):
    """Wrapper HTTP avec gestion d'erreurs. Retourne (ok, status_code, data_or_error)."""
    try:
        import requests as _req
        resp = _req.request(method, url, headers=headers, params=params, timeout=timeout)
        if resp.status_code < 400:
            try:
                return True, resp.status_code, resp.json()
            except Exception:
                return True, resp.status_code, {}
        return False, resp.status_code, resp.text[:300]
    except Exception as e:
        return False, 0, str(e)


def _test_pos_connection(pos_id: str, creds: dict):
    """Teste la connexion à un POS. Retourne (success: bool, message: str)."""
    if pos_id == "tiller":
        api_key = creds.get("api_key", "")
        if not api_key:
            return False, "Clé API manquante."
        ok, code, data = _pos_request(
            "GET", "https://api.sumup.com/v0.1/me",
            headers={"Authorization": f"Bearer {api_key}"}
        )
        if ok:
            name = data.get("personal_details", {}).get("first_name", "OK")
            return True, f"Connexion réussie — compte : {name}"
        return False, f"Erreur {code} : identifiants invalides ou expirés."

    if pos_id == "laddition":
        api_key = creds.get("api_key", "")
        if not api_key:
            return False, "Clé API manquante."
        ok, code, _ = _pos_request(
            "GET", "https://api.laddition.com/api/v1/check",
            headers={"X-Auth-Token": api_key, "Content-Type": "application/json"}
        )
        if ok:
            return True, "Connexion réussie à L'Addition."
        return False, f"Erreur {code} : vérifiez votre clé API."

    if pos_id == "lightspeed":
        account_id = creds.get("account_id", "")
        api_key = creds.get("api_key", "")
        if not account_id or not api_key:
            return False, "Account ID et clé API requis."
        ok, code, data = _pos_request(
            "GET", f"https://api.lightspeedapp.com/API/V3/Account/{account_id}.json",
            headers={"Authorization": f"Bearer {api_key}"}
        )
        if ok:
            name = data.get("Account", {}).get("name", "OK")
            return True, f"Connexion réussie — compte : {name}"
        return False, f"Erreur {code} : Account ID ou clé API invalide."

    if pos_id == "zelty":
        api_token = creds.get("api_token", "")
        if not api_token:
            return False, "Token API manquant."
        ok, code, data = _pos_request(
            "GET", "https://backoffice.zelty.fr/api/2/restaurants",
            headers={"Authorization": f"Token {api_token}"}
        )
        if ok:
            nb = len(data) if isinstance(data, list) else 1
            return True, f"Connexion réussie — {nb} restaurant(s) trouvé(s)."
        return False, f"Erreur {code} : token invalide ou droits insuffisants."

    if pos_id == "ubereats":
        client_id = creds.get("client_id", "")
        client_secret = creds.get("client_secret", "")
        if not client_id or not client_secret:
            return False, "Client ID et Client Secret requis."
        try:
            import requests as _req
            resp = _req.post(
                "https://login.uber.com/oauth/v2/token",
                data={"client_id": client_id, "client_secret": client_secret,
                      "grant_type": "client_credentials", "scope": "eats.report"},
                timeout=8,
            )
            if resp.status_code == 200:
                return True, "Connexion Uber Eats réussie — token OAuth obtenu."
            return False, f"Erreur {resp.status_code} : vérifiez vos identifiants Uber Eats for Restaurants."
        except Exception as e:
            return False, str(e)

    return False, "POS non reconnu."


def _sync_pos_data(pos_id: str, creds: dict):
    """Récupère les ventes des 7 derniers jours et les insère dans Firestore."""
    import datetime as _dt
    today = _dt.date.today()
    depuis = str(today - _dt.timedelta(days=7))
    synced = 0

    try:
        if pos_id == "tiller":
            api_key = creds.get("api_key", "")
            ok, code, data = _pos_request(
                "GET", "https://api.sumup.com/v0.1/me/transactions/history",
                headers={"Authorization": f"Bearer {api_key}"},
                params={"oldest_time": f"{depuis}T00:00:00Z", "statuses[]": "SUCCESSFUL"}
            )
            if not ok:
                return {"success": False, "message": f"Erreur {code}."}
            by_date: dict = {}
            for t in data.get("items", []):
                d = str(t.get("timestamp", ""))[:10]
                by_date[d] = by_date.get(d, 0.0) + float(t.get("amount", 0))
            for d, ca_ttc in by_date.items():
                date_obj = _dt.date.fromisoformat(d)
                ca_ht = round(ca_ttc / 1.055, 2)
                _db.collection(COLLECTION_VENTES).document(f"tiller_{d}").set({
                    "date": d, "semaine": int(date_obj.isocalendar()[1]),
                    "annee": int(date_obj.year), "canal": "Sur place",
                    "ca_ht": ca_ht, "tva": round(ca_ttc - ca_ht, 2),
                    "ca_ttc": round(ca_ttc, 2), "nb_couverts": 0, "nb_commandes": 0,
                    "lignes": [], "source": "tiller", "created_at": str(today)
                }, merge=True)
                synced += 1
            return {"success": True, "message": f"{synced} jour(s) synchronisé(s) depuis Tiller."}

        if pos_id == "zelty":
            api_token = creds.get("api_token", "")
            ok, code, data = _pos_request(
                "GET", "https://backoffice.zelty.fr/api/2/stats/daily",
                headers={"Authorization": f"Token {api_token}"},
                params={"start": depuis, "end": str(today)}
            )
            if not ok:
                return {"success": False, "message": f"Erreur {code}."}
            for row in (data if isinstance(data, list) else data.get("results", [])):
                d = str(row.get("date", ""))[:10]
                if not d:
                    continue
                date_obj = _dt.date.fromisoformat(d)
                ca_ht = float(row.get("revenue_ht", 0) or 0)
                ca_ttc = float(row.get("revenue_ttc", 0) or 0)
                _db.collection(COLLECTION_VENTES).document(f"zelty_{d}").set({
                    "date": d, "semaine": int(date_obj.isocalendar()[1]),
                    "annee": int(date_obj.year), "canal": "Sur place",
                    "ca_ht": round(ca_ht, 2), "tva": round(ca_ttc - ca_ht, 2),
                    "ca_ttc": round(ca_ttc, 2),
                    "nb_couverts": int(row.get("nb_couverts", 0) or 0),
                    "nb_commandes": int(row.get("nb_tickets", 0) or 0),
                    "lignes": [], "source": "zelty", "created_at": str(today)
                }, merge=True)
                synced += 1
            return {"success": True, "message": f"{synced} jour(s) synchronisé(s) depuis Zelty."}

        if pos_id == "ubereats":
            client_id = creds.get("client_id", "")
            client_secret = creds.get("client_secret", "")
            store_id = creds.get("store_id", "")
            import requests as _req
            token_resp = _req.post(
                "https://login.uber.com/oauth/v2/token",
                data={"client_id": client_id, "client_secret": client_secret,
                      "grant_type": "client_credentials", "scope": "eats.report"},
                timeout=8
            )
            if token_resp.status_code != 200:
                return {"success": False, "message": "Impossible d'obtenir le token Uber Eats."}
            access_token = token_resp.json().get("access_token", "")
            ok, code, data = _pos_request(
                "GET", f"https://api.uber.com/v1/eats/report/orders/store/{store_id}",
                headers={"Authorization": f"Bearer {access_token}"},
                params={"start_date": depuis, "end_date": str(today)}
            )
            if not ok:
                return {"success": False, "message": f"Erreur {code} Uber Eats."}
            by_date_orders: dict = {}
            for o in data.get("orders", []):
                d = str(o.get("created_at", ""))[:10]
                total = float(o.get("total", {}).get("price", 0)) / 100
                entry = by_date_orders.setdefault(d, {"ca_ttc": 0.0, "nb": 0})
                entry["ca_ttc"] += total
                entry["nb"] += 1
            for d, vals in by_date_orders.items():
                date_obj = _dt.date.fromisoformat(d)
                ca_ttc = vals["ca_ttc"]
                ca_ht = round(ca_ttc / 1.055, 2)
                _db.collection(COLLECTION_VENTES).document(f"ubereats_{d}").set({
                    "date": d, "semaine": int(date_obj.isocalendar()[1]),
                    "annee": int(date_obj.year), "canal": "Livraison",
                    "ca_ht": ca_ht, "tva": round(ca_ttc - ca_ht, 2),
                    "ca_ttc": round(ca_ttc, 2), "nb_couverts": 0,
                    "nb_commandes": int(vals["nb"]),
                    "lignes": [], "source": "ubereats", "created_at": str(today)
                }, merge=True)
                synced += 1
            return {"success": True, "message": f"{synced} jour(s) Uber Eats synchronisé(s)."}

        return {"success": False, "message": "Synchronisation non disponible pour ce POS."}

    except Exception as e:
        return {"success": False, "message": f"Erreur : {e}"}


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
    "Saisie de données": "✏️ Saisie de données",
    "Ventes & CA": "💰 Ventes & CA",
    "Intégrations": "🔌 Intégrations",
    "Mes Apps": "📱 Mes Apps",
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
    bar_colors = [
        "linear-gradient(160deg,#FFD700,#FFA500)",
        "linear-gradient(160deg,#C8C8C8,#9e9e9e)",
        "linear-gradient(160deg,#CD7F32,#8B5E2A)"
    ]
    bar_heights = [115, 80, 55]
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

st.sidebar.divider()
st.sidebar.subheader("📥 Exporter des données")

@st.cache_data(ttl=60, show_spinner=False)
def generate_csv_zip_from_db():
    import io, zipfile, pandas as pd
    try:
        dfs = {}
        # KPI 2026
        try:
            dfs["Kpi_2026"] = load_data()
        except Exception:
            pass
            
        # Recettes
        try:
            dfs["Recettes"] = pd.DataFrame(load_recettes())
        except Exception:
            pass
            
        # Ingredients
        try:
            dfs["Ingredients"] = pd.DataFrame(list(load_ingredients().values()))
        except Exception:
            pass
            
        # Factures
        try:
            dfs["Factures"] = pd.DataFrame(load_factures())
        except Exception:
            pass
            
        # Ventes
        try:
            dfs["Ventes"] = pd.DataFrame(load_ventes())
        except Exception:
            pass

        if not dfs:
            return None

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for sheet_name, df in dfs.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    safe_name = sheet_name.replace("/", "_").replace("\\", "_")
                    zf.writestr(f"Export_{safe_name}.csv", df.to_csv(index=False).encode('utf-8'))
        return zip_buffer.getvalue()
    except Exception as e:
        st.sidebar.error(f"Erreur d'export: {e}")
        return None

zip_data = generate_csv_zip_from_db()
if zip_data:
    st.sidebar.download_button(
        label="Extraire les données en CSV (ZIP)",
        data=zip_data,
        file_name="extractions_donnees.zip",
        mime="application/zip",
        use_container_width=True
    )
else:
    st.sidebar.warning("Aucune donnée à exporter ou base inaccessible.")
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
                textfont=dict(color='white')
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
                textfont=dict(size=13, color='#ffe0b2')
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
                    textfont=dict(color='black')
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
                    textfont=dict(color='#2ca02c'),
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
                        textfont=dict(color='black')
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
                        textfont=dict(color='black')
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
                        textfont=dict(size=13, color=c_texte_fonce)
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

    tab_fiche, tab_prod, tab_ajout, tab_catalogue = st.tabs(["📋 Fiche Recette", "🏭 Plan de Production", "➕ Ajouter une recette", "🧂 Catalogue ingrédients"])

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
                    marge_pct_r = round(marge_r / ca_r * 100, 1) if ca_r > 0 else None

                    plan_rows.append({
                        "Recette": nom_r,
                        "Couverts": nb_prod,
                        "Coût/couvert (€)": round(cout_cov, 2),
                        "Prix vente/couvert (€)": round(pv, 2) if pv > 0 else "—",
                        "Coût total (€)": cout_total_r,
                        "CA prévu (€)": ca_r if pv > 0 else "—",
                        "Marge (€)": marge_r if pv > 0 else "—",
                        "Marge (%)": f"{marge_pct_r:.1f}%" if marge_pct_r is not None else "—",
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
    # ONGLET 3 : AJOUTER / MODIFIER UNE RECETTE
    # ══════════════════════════════════════════════════════
    with tab_ajout:
        # ── Sélection recette à éditer (hors form → déclenche rerun) ──
        noms_edit = ["— Nouvelle recette —"] + [r["nom"] for r in recettes_list]
        choix_edit = st.selectbox("Créer ou modifier :", noms_edit, key="choix_edit")
        rec_edit = next((r for r in recettes_list if r["nom"] == choix_edit), None)

        # ── Mode de saisie (hors form → déclenche rerun) ──
        mode_saisie = st.radio(
            "Mode de saisie des poids :",
            ["Brut + Perte% → Net calculé", "Net + Perte% → Brut calculé"],
            horizontal=True, key="mode_saisie",
            help="Brut→Net : tu saisies ce que tu achètes  |  Net→Brut : tu saisies ce que tu utilises en recette"
        )
        net_vers_brut = (mode_saisie == "Net + Perte% → Brut calculé")
        if net_vers_brut:
            st.caption("**Formule :** Brut = Net ÷ (1 - Perte%/100)  |  Coût = Brut × Prix/kg")
        else:
            st.caption("**Formule :** Net = Brut × (1 - Perte%/100)  |  Coût = Brut × Prix/kg")

        # Clé dynamique : change quand on change de recette → réinitialise les champs
        form_key_suffix = choix_edit.replace(" ", "_").replace("—", "new")

        with st.form(f"form_recette_{form_key_suffix}"):
            c1, c2, c3 = st.columns(3)
            with c1:
                f_nom = st.text_input("Nom de la recette",
                    value=rec_edit["nom"] if rec_edit else "")
            with c2:
                cats = ["Chaud", "Légumerie", "Sushi", "Mix", "Mélange", "Désinfection", "Autre"]
                cat_default = cats.index(rec_edit["categorie"]) if rec_edit and rec_edit.get("categorie") in cats else 0
                f_cat = st.selectbox("Catégorie", cats, index=cat_default)
            with c3:
                f_couverts = st.number_input("Nb couverts", min_value=1,
                    value=int(rec_edit["nb_couverts"]) if rec_edit else 6, step=1)

            st.divider()

            # En-têtes
            if net_vers_brut:
                hc1, hc2, hc3, hc4, hc5 = st.columns([3, 1.5, 1.5, 1.8, 1.5])
                hc1.markdown("**Ingrédient**")
                hc2.markdown("**Net (kg)**")
                hc3.markdown("**Perte (%)**")
                hc4.markdown("**→ Brut (kg)**")
                hc5.markdown("**Prix/kg (€)**")
            else:
                hc1, hc2, hc3, hc4, hc5 = st.columns([3, 1.5, 1.5, 1.8, 1.5])
                hc1.markdown("**Ingrédient**")
                hc2.markdown("**Brut (kg)**")
                hc3.markdown("**Perte (%)**")
                hc4.markdown("**→ Net (kg)**")
                hc5.markdown("**Prix/kg (€)**")

            ing_rows = []
            ing_existants = rec_edit["ingredients"] if rec_edit else []
            cat_names_form = sorted(prix_dict.keys())
            for i in range(10):
                ex = ing_existants[i] if i < len(ing_existants) else {}
                ex_brut  = float(ex.get("poids_brut_kg", 0))
                ex_perte = float(ex.get("taux_perte_pct",
                    round((ex_brut - float(ex.get("poids_net_kg", ex_brut))) / ex_brut * 100, 1)
                    if ex_brut > 0 and "poids_net_kg" in ex else 0))
                ex_net   = round(ex_brut * (1 - ex_perte / 100), 4) if ex_brut > 0 else 0.0
                ex_prix  = float(ex.get("prix_unitaire", 0))
                current_nom = ex.get("nom", "")
                # Auto-fill price from catalog when price not set in recipe
                if ex_prix == 0 and current_nom and current_nom in prix_dict:
                    ex_prix = float(prix_dict[current_nom].get("prix_unitaire", 0))

                ca, cb, cc, cd, ce = st.columns([3, 1.5, 1.5, 1.8, 1.5])
                with ca:
                    ing_options = cat_names_form + ["— Saisie manuelle —"]
                    if current_nom in cat_names_form:
                        def_idx = cat_names_form.index(current_nom)
                    else:
                        def_idx = len(ing_options) - 1
                    sel_i = st.selectbox("nom", options=ing_options, index=def_idx,
                        key=f"r_sel_{i}_{form_key_suffix}", label_visibility="collapsed")
                    if sel_i == "— Saisie manuelle —":
                        nom_i = st.text_input("nom_libre", value=current_nom if current_nom not in cat_names_form else "",
                            key=f"r_nom_{i}_{form_key_suffix}", label_visibility="collapsed",
                            placeholder="Nom de l'ingrédient")
                    else:
                        nom_i = sel_i
                        # Pre-fill price from catalog for catalog selection if price not already set
                        if ex_prix == 0 and nom_i in prix_dict:
                            ex_prix = float(prix_dict[nom_i].get("prix_unitaire", 0))
                with cb:
                    if net_vers_brut:
                        val_b = st.number_input("net", value=ex_net,
                            min_value=0.0, step=0.001, format="%.3f",
                            key=f"r_b_{i}_{form_key_suffix}", label_visibility="collapsed")
                    else:
                        val_b = st.number_input("brut", value=ex_brut,
                            min_value=0.0, step=0.001, format="%.3f",
                            key=f"r_b_{i}_{form_key_suffix}", label_visibility="collapsed")
                with cc:
                    perte_i = st.number_input("perte", value=ex_perte,
                        min_value=0.0, max_value=99.9, step=0.5, format="%.1f",
                        key=f"r_p_{i}_{form_key_suffix}", label_visibility="collapsed")
                with ce:
                    prix_i = st.number_input("prix", value=ex_prix,
                        min_value=0.0, step=0.1, format="%.2f",
                        key=f"r_px_{i}_{form_key_suffix}", label_visibility="collapsed")

                # Calcul brut/net selon mode
                if net_vers_brut:
                    diviseur = (1 - perte_i / 100)
                    brut_final = round(val_b / diviseur, 4) if diviseur > 0 else val_b
                    net_final  = val_b
                else:
                    brut_final = val_b
                    net_final  = round(val_b * (1 - perte_i / 100), 4)

                with cd:
                    if net_vers_brut:
                        st.text_input("brut_calc", value=f"{brut_final:.3f} kg",
                            key=f"r_calc_{i}_{form_key_suffix}",
                            label_visibility="collapsed", disabled=True)
                    else:
                        st.text_input("net_calc", value=f"{net_final:.3f} kg",
                            key=f"r_calc_{i}_{form_key_suffix}",
                            label_visibility="collapsed", disabled=True)

                if nom_i.strip():
                    ing_rows.append({
                        "nom": nom_i.strip(),
                        "poids_brut_kg": brut_final,
                        "taux_perte_pct": perte_i,
                        "prix_unitaire": prix_i,
                    })

            st.divider()
            btn_label = "💾 Modifier la recette" if rec_edit else "➕ Enregistrer la recette"
            submitted_rec = st.form_submit_button(btn_label, type="primary")
            if submitted_rec:
                if not f_nom.strip():
                    st.error("Le nom est obligatoire.")
                elif not ing_rows:
                    st.error("Ajoutez au moins un ingrédient.")
                else:
                    doc_id = rec_edit["id"] if rec_edit else str(uuid.uuid4())
                    _db.collection(COLLECTION_RECETTES).document(doc_id).set({
                        "nom": f_nom.strip(), "categorie": f_cat,
                        "nb_couverts": int(f_couverts),
                        "ingredients": ing_rows,
                        "updated_at": str(date.today()),
                        **({"created_at": rec_edit.get("created_at", str(date.today()))} if rec_edit
                           else {"created_at": str(date.today())})
                    })
                    for ing in ing_rows:
                        ing_id = ing["nom"].lower().replace(" ", "_").replace("'", "")
                        _db.collection(COLLECTION_INGREDIENTS).document(ing_id).set({
                            "nom": ing["nom"], "prix_unitaire": ing["prix_unitaire"],
                            "unite": "kg", "fournisseur": "", "updated_at": str(date.today())
                        }, merge=True)
                    st.cache_data.clear()
                    action = "modifiée" if rec_edit else "enregistrée"
                    st.success(f"Recette '{f_nom}' {action} !")
                    st.rerun()

    # ══════════════════════════════════════════════════════
    # ONGLET 4 : CATALOGUE INGRÉDIENTS
    # ══════════════════════════════════════════════════════
    with tab_catalogue:
        st.subheader("🧂 Catalogue des ingrédients")

        # ── Tableau de tous les ingrédients ──
        if prix_dict:
            ing_cat_data = [
                {
                    "Ingrédient": nom,
                    "Prix/unité (€)": float(info.get("prix_unitaire", 0)),
                    "Unité": info.get("unite", "kg"),
                    "Fournisseur": info.get("fournisseur", ""),
                }
                for nom, info in sorted(prix_dict.items())
            ]
            st.dataframe(pd.DataFrame(ing_cat_data), use_container_width=True, hide_index=True)
        else:
            st.info("Aucun ingrédient dans le catalogue. Ajoutez-en ci-dessous.")

        st.divider()

        # ── Ajouter / Modifier ──
        st.markdown("#### Ajouter / Modifier un ingrédient")
        cat_edit_names = sorted(prix_dict.keys())
        cat_edit_options = ["— Nouvel ingrédient —"] + cat_edit_names
        cat_edit_sel = st.selectbox("Créer ou modifier :", cat_edit_options, key="cat_edit_sel")

        if cat_edit_sel == "— Nouvel ingrédient —":
            cat_existing = {}
            nom_default = ""
        else:
            cat_existing = prix_dict.get(cat_edit_sel, {})
            nom_default = cat_edit_sel

        unite_opts = ["kg", "L", "pièce", "colis", "boîte"]
        cur_unite = cat_existing.get("unite", "kg")

        with st.form("form_catalogue_add"):
            f_nom_c = st.text_input("Nom de l'ingrédient *", value=nom_default)
            col_ca, col_cb, col_cc = st.columns(3)
            with col_ca:
                f_prix_c = st.number_input("Prix (€/unité)", min_value=0.0,
                    value=float(cat_existing.get("prix_unitaire", 0.0)), step=0.01, format="%.2f")
            with col_cb:
                f_unite_c = st.selectbox("Unité",
                    unite_opts, index=unite_opts.index(cur_unite) if cur_unite in unite_opts else 0)
            with col_cc:
                f_fourn_c = st.text_input("Fournisseur", value=cat_existing.get("fournisseur", ""))

            if st.form_submit_button("💾 Enregistrer", type="primary"):
                if not f_nom_c.strip():
                    st.error("Le nom est obligatoire.")
                else:
                    ing_id_c = f_nom_c.strip().lower().replace(" ", "_").replace("'", "")
                    _db.collection(COLLECTION_INGREDIENTS).document(ing_id_c).set({
                        "nom": f_nom_c.strip(),
                        "prix_unitaire": f_prix_c,
                        "unite": f_unite_c,
                        "fournisseur": f_fourn_c,
                        "updated_at": str(date.today()),
                    }, merge=True)
                    st.cache_data.clear()
                    st.success(f"✅ '{f_nom_c.strip()}' enregistré dans le catalogue.")
                    st.rerun()

        # ── Supprimer ──
        if cat_edit_names:
            st.divider()
            st.markdown("#### Supprimer un ingrédient")
            with st.form("form_catalogue_del"):
                del_sel = st.selectbox("Ingrédient à supprimer :", cat_edit_names, key="del_sel_cat")
                if st.form_submit_button("🗑️ Supprimer", type="secondary"):
                    del_id = prix_dict.get(del_sel, {}).get("id", del_sel.lower().replace(" ", "_").replace("'", ""))
                    _db.collection(COLLECTION_INGREDIENTS).document(del_id).delete()
                    st.cache_data.clear()
                    st.warning(f"'{del_sel}' supprimé du catalogue.")
                    st.rerun()

# ─────────────────────────────────────────────────────────
# PAGE : FACTURES
# ─────────────────────────────────────────────────────────
elif page == "Factures":
    st.title("🧾 Gestion des Factures")

    factures_list = load_factures()
    prix_dict     = load_ingredients()

    # ── Onglets Liste / Scanner / Manuel ──────────────────
    tab_liste, tab_scan, tab_manuel = st.tabs(["📋 Toutes les factures", "📸 Scanner une facture", "✏️ Saisie manuelle"])

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
                st.caption("Réf · Article · Qté · Unité · Prix/unité · Total HT")
                lignes_valid = []
                for i, lg in enumerate(ocr.get("lignes", [])[:50]):
                    lc1, lc2, lc3, lc4, lc5, lc6 = st.columns([1, 3, 0.9, 0.9, 1.2, 1.2])
                    with lc1: ref = st.text_input("Réf",          value=lg.get("reference",""),    key=f"ref_{i}", label_visibility="collapsed", placeholder="Réf")
                    with lc2: art = st.text_input("Article",       value=lg.get("article",""),      key=f"art_{i}", label_visibility="collapsed", placeholder="Article")
                    with lc3: qty = st.number_input("Qté",         value=float(lg.get("quantite",0)),key=f"qty_{i}", step=0.001, format="%.3f", label_visibility="collapsed")
                    with lc4: uni = st.text_input("Unité",         value=lg.get("unite","kg"),      key=f"uni_{i}", label_visibility="collapsed", placeholder="Unité")
                    with lc5: pu  = st.number_input("Prix/u",      value=float(lg.get("prix_unitaire",0)), key=f"pu_{i}", step=0.001, format="%.3f", label_visibility="collapsed")
                    default_tht = round(qty * pu, 2) if lg.get("total_ht", 0) == 0 else float(lg.get("total_ht", 0))
                    with lc6: tht = st.number_input("Total HT",    value=default_tht,               key=f"tht_{i}", step=0.01, label_visibility="collapsed")
                    if art.strip():
                        lignes_valid.append({"reference": ref, "article": art, "quantite": qty, "unite": uni, "prix_unitaire": pu, "total_ht": tht})

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

                    # Sync ingrédients : créer les nouveaux, MAJ prix si bouton MAJ
                    nb_crees, nb_maj = 0, 0
                    existing_ids = {d.id for d in _db.collection(COLLECTION_INGREDIENTS).stream()}
                    for lg in lignes_valid:
                        nom = lg["article"].strip()
                        if not nom:
                            continue
                        ing_id = nom.lower().replace(" ", "_").replace("'", "").replace("é","e").replace("è","e").replace("ê","e")
                        ing_data = {
                            "nom": nom,
                            "reference": lg.get("reference", ""),
                            "unite": lg.get("unite", "kg"),
                            "fournisseur": v_fourn,
                            "updated_at": str(date.today())
                        }
                        if ing_id not in existing_ids:
                            # Nouvel ingrédient : toujours créer avec prix
                            ing_data["prix_unitaire"] = lg["prix_unitaire"]
                            _db.collection(COLLECTION_INGREDIENTS).document(ing_id).set(ing_data)
                            nb_crees += 1
                        elif maj_btn and lg["prix_unitaire"] > 0:
                            # Ingrédient existant : MAJ prix seulement si bouton MAJ
                            ing_data["prix_unitaire"] = lg["prix_unitaire"]
                            _db.collection(COLLECTION_INGREDIENTS).document(ing_id).set(ing_data, merge=True)
                            nb_maj += 1

                    msg = "Facture enregistrée !"
                    if nb_crees: msg += f" {nb_crees} nouvel(s) ingrédient(s) créé(s)."
                    if nb_maj:   msg += f" {nb_maj} prix mis à jour."
                    st.success(msg)
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
        st.subheader("Toutes les factures")
        if not factures_list:
            st.info("Aucune facture enregistrée. Scannez ou saisissez une facture.")
        else:
            # ── Filtres ────────────────────────────────────────
            all_fournisseurs = sorted(set(f.get("fournisseur", "") for f in factures_list if f.get("fournisseur")))
            fl1, fl2, fl3 = st.columns([2, 1, 1])
            with fl1:
                filtre_fourn = st.multiselect("Fournisseur(s)", all_fournisseurs, placeholder="Tous")
            with fl2:
                filtre_date_debut = st.text_input("Date début (YYYY-MM-DD)", placeholder="ex: 2025-01-01")
            with fl3:
                filtre_date_fin = st.text_input("Date fin (YYYY-MM-DD)", placeholder="ex: 2025-12-31")

            # Appliquer filtres
            fac_filtrées = factures_list
            if filtre_fourn:
                fac_filtrées = [f for f in fac_filtrées if f.get("fournisseur","") in filtre_fourn]
            if filtre_date_debut:
                fac_filtrées = [f for f in fac_filtrées if f.get("date","") >= filtre_date_debut]
            if filtre_date_fin:
                fac_filtrées = [f for f in fac_filtrées if f.get("date","") <= filtre_date_fin]

            # ── KPIs ───────────────────────────────────────────
            total_ht_all  = sum(f.get("total_ht", 0)  for f in fac_filtrées)
            total_ttc_all = sum(f.get("total_ttc", 0) for f in fac_filtrées)
            nb_fourn = len(set(f.get("fournisseur","") for f in fac_filtrées))
            fk1, fk2, fk3, fk4 = st.columns(4)
            fk1.metric("Factures", len(fac_filtrées))
            fk2.metric("Total achats HT", f"{total_ht_all:.2f} €")
            fk3.metric("Total achats TTC", f"{total_ttc_all:.2f} €")
            fk4.metric("Fournisseurs", nb_fourn)

            st.divider()

            # ── Tableau récapitulatif ──────────────────────────
            df_fac = pd.DataFrame([{
                "Date": f.get("date", ""),
                "Fournisseur": f.get("fournisseur", ""),
                "N° Facture": f.get("numero", ""),
                "Total HT (€)": float(f.get("total_ht", 0)),
                "TVA (€)": float(f.get("tva", 0)),
                "Total TTC (€)": float(f.get("total_ttc", 0)),
                "Statut": f.get("statut", "validée"),
                "Saisie le": f.get("created_at", ""),
                "_id": f.get("id", ""),
            } for f in fac_filtrées])

            st.dataframe(
                df_fac.drop(columns=["_id"]),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Total HT (€)":  st.column_config.NumberColumn(format="%.2f €"),
                    "TVA (€)":       st.column_config.NumberColumn(format="%.2f €"),
                    "Total TTC (€)": st.column_config.NumberColumn(format="%.2f €"),
                }
            )

            # ── Par fournisseur ────────────────────────────────
            if len(all_fournisseurs) > 1:
                st.divider()
                st.markdown("**Achats par fournisseur**")
                df_fourn = df_fac.groupby("Fournisseur")["Total HT (€)"].sum().reset_index()
                df_fourn = df_fourn.sort_values("Total HT (€)", ascending=False)
                df_fourn["Total HT (€)"] = df_fourn["Total HT (€)"].round(2)
                st.dataframe(df_fourn, use_container_width=True, hide_index=True,
                    column_config={"Total HT (€)": st.column_config.NumberColumn(format="%.2f €")})

            # ── Détail + suppression ───────────────────────────
            st.divider()
            labels_fac = [
                f"{f.get('date','')} — {f.get('fournisseur','')} — {f.get('numero','') or 'sans n°'}"
                for f in fac_filtrées
            ]
            if labels_fac:
                choix_fac = st.selectbox("Détail d'une facture :", labels_fac, key="detail_fac_select")
                idx = labels_fac.index(choix_fac)
                fac_detail = fac_filtrées[idx]

                dc1, dc2, dc3, dc4 = st.columns(4)
                dc1.markdown(f"**Fournisseur :** {fac_detail.get('fournisseur','')}")
                dc2.markdown(f"**Date :** {fac_detail.get('date','')}")
                dc3.markdown(f"**N° :** {fac_detail.get('numero','—')}")
                dc4.markdown(f"**TTC :** {float(fac_detail.get('total_ttc',0)):.2f} €")

                lignes_detail = fac_detail.get("lignes", [])
                if lignes_detail:
                    df_lig = pd.DataFrame(lignes_detail)
                    # Renommage colonnes si présentes
                    rename_map = {"article": "Article", "quantite": "Qté", "unite": "Unité",
                                  "prix_unitaire": "PU (€)", "total_ht": "Total HT (€)", "reference": "Réf"}
                    df_lig = df_lig.rename(columns={k: v for k, v in rename_map.items() if k in df_lig.columns})
                    col_cfg = {}
                    if "PU (€)" in df_lig.columns:
                        col_cfg["PU (€)"] = st.column_config.NumberColumn(format="%.3f €")
                    if "Total HT (€)" in df_lig.columns:
                        col_cfg["Total HT (€)"] = st.column_config.NumberColumn(format="%.2f €")
                    st.dataframe(df_lig, use_container_width=True, hide_index=True, column_config=col_cfg)
                else:
                    st.caption("Aucune ligne de détail enregistrée pour cette facture.")

                # Bouton de suppression
                fac_id = fac_detail.get("id", "")
                if fac_id:
                    with st.expander("⚠️ Supprimer cette facture"):
                        st.warning(f"Supprimer définitivement la facture {fac_detail.get('numero','sans n°')} de {fac_detail.get('fournisseur','')} ?")
                        if st.button("🗑️ Confirmer la suppression", type="primary", key="del_fac_btn"):
                            _db.collection(COLLECTION_FACTURES).document(fac_id).delete()
                            st.cache_data.clear()
                            st.success("Facture supprimée.")
                            st.rerun()

# ── PAGE : VENTES & CA ──────────────────────────────────────────────────────
elif page == "Ventes & CA":
    st.title("💰 Ventes & Chiffre d'Affaires")

    tab_saisie, tab_dashboard = st.tabs(["📝 Saisie du CA", "📊 Dashboard Croisé"])

    # ── Onglet 1 : Saisie ───────────────────────────────────────────────────
    with tab_saisie:
        st.markdown("### Saisir le CA du jour")

        recettes_list = load_recettes()
        noms_recettes = sorted([r["nom"] for r in recettes_list]) if recettes_list else []

        with st.form("form_saisie_ca", clear_on_submit=True):
            col_d1, col_d2, col_d3 = st.columns([2, 1, 1])
            with col_d1:
                date_vente = st.date_input("Date", value=date.today())
            with col_d2:
                canal = st.selectbox("Canal", ["Livraison", "Sur place", "Click & Collect", "Mixte"])
            with col_d3:
                nb_commandes_v = st.number_input("Nb commandes", min_value=0, step=1)

            st.divider()
            col_ca1, col_ca2, col_ca3 = st.columns(3)
            with col_ca1:
                ca_ht = st.number_input("CA HT (€)", min_value=0.0, step=10.0, format="%.2f")
            with col_ca2:
                taux_tva = st.number_input("TVA (%)", min_value=0.0, max_value=25.0, value=5.5, step=0.5)
            with col_ca3:
                nb_couverts_v = st.number_input("Nb couverts", min_value=0, step=1)
            ca_ttc = ca_ht * (1 + taux_tva / 100)
            if ca_ht > 0:
                st.caption(f"CA TTC calculé : **{ca_ttc:,.2f} €**")

            st.divider()
            st.markdown("#### Détail des ventes par recette *(optionnel)*")
            st.caption("Renseignez les plats vendus pour enrichir l'analyse par recette.")

            lignes_vente = []
            nb_lignes = st.number_input("Nombre de lignes", min_value=1, max_value=20, value=3, step=1)
            for i in range(int(nb_lignes)):
                lc1, lc2, lc3 = st.columns([3, 1, 1])
                with lc1:
                    recette_sel = st.selectbox(
                        f"Recette {i+1}",
                        ["— sélectionner —"] + noms_recettes,
                        key=f"lig_rec_{i}"
                    )
                with lc2:
                    qty = st.number_input("Qté (couverts)", min_value=0, step=1, key=f"lig_qty_{i}")
                with lc3:
                    prix_v = st.number_input("Prix vente/couvert (€)", min_value=0.0, step=0.10,
                                             format="%.2f", key=f"lig_prix_{i}")
                if recette_sel != "— sélectionner —" and qty > 0:
                    recette_obj = next((r for r in recettes_list if r["nom"] == recette_sel), {})
                    lignes_vente.append({
                        "recette_nom": recette_sel,
                        "categorie": recette_obj.get("categorie", ""),
                        "quantite": int(qty),
                        "prix_vente_couvert": float(prix_v),
                        "ca_ligne": round(float(qty) * float(prix_v), 2)
                    })

            notes = st.text_area("Notes", placeholder="Événements, commentaires...")

            submitted = st.form_submit_button("💾 Enregistrer", type="primary")
            if submitted:
                if ca_ht <= 0:
                    st.error("Veuillez saisir un CA HT supérieur à 0.")
                else:
                    doc = {
                        "date": str(date_vente),
                        "semaine": int(date_vente.isocalendar()[1]),
                        "annee": int(date_vente.year),
                        "canal": canal,
                        "ca_ht": round(float(ca_ht), 2),
                        "tva": round(float(ca_ht * taux_tva / 100), 2),
                        "ca_ttc": round(float(ca_ttc), 2),
                        "nb_couverts": int(nb_couverts_v),
                        "nb_commandes": int(nb_commandes_v),
                        "lignes": lignes_vente,
                        "notes": notes,
                        "created_at": str(date.today())
                    }
                    _db.collection(COLLECTION_VENTES).document(str(uuid.uuid4())).set(doc)
                    st.cache_data.clear()
                    st.success(f"CA du {date_vente} enregistré : **{ca_ht:,.2f} € HT**")

        # ── Historique rapide ────────────────────────────────────────────────
        ventes_list = load_ventes()
        if ventes_list:
            st.divider()
            st.markdown("#### Dernières saisies")
            df_hist = pd.DataFrame([{
                "Date": v.get("date", ""),
                "Canal": v.get("canal", ""),
                "CA HT (€)": v.get("ca_ht", 0),
                "CA TTC (€)": v.get("ca_ttc", 0),
                "Couverts": v.get("nb_couverts", 0),
                "Commandes": v.get("nb_commandes", 0),
            } for v in ventes_list[:15]])
            st.dataframe(df_hist, use_container_width=True, hide_index=True,
                column_config={
                    "CA HT (€)":  st.column_config.NumberColumn(format="%.2f €"),
                    "CA TTC (€)": st.column_config.NumberColumn(format="%.2f €"),
                })

            # Suppression
            with st.expander("⚠️ Supprimer une saisie"):
                labels_v = [f"{v.get('date','')} — {v.get('ca_ht',0):,.2f} € HT" for v in ventes_list]
                sel_v = st.selectbox("Choisir la saisie à supprimer", labels_v, key="del_vente_sel")
                if st.button("🗑️ Supprimer", key="del_vente_btn"):
                    idx_v = labels_v.index(sel_v)
                    _db.collection(COLLECTION_VENTES).document(ventes_list[idx_v]["id"]).delete()
                    st.cache_data.clear()
                    st.success("Saisie supprimée.")
                    st.rerun()

    # ── Onglet 2 : Dashboard Croisé ──────────────────────────────────────────
    with tab_dashboard:
        ventes_list = load_ventes()
        factures_list = load_factures()

        if not ventes_list:
            st.info("Aucune donnée de vente enregistrée. Commencez par saisir le CA dans l'onglet **Saisie du CA**.")
        else:
            df_v = pd.DataFrame(ventes_list)
            df_v["ca_ht"] = pd.to_numeric(df_v.get("ca_ht", 0), errors="coerce").fillna(0)
            df_v["ca_ttc"] = pd.to_numeric(df_v.get("ca_ttc", 0), errors="coerce").fillna(0)
            df_v["nb_couverts"] = pd.to_numeric(df_v.get("nb_couverts", 0), errors="coerce").fillna(0)
            df_v["nb_commandes"] = pd.to_numeric(df_v.get("nb_commandes", 0), errors="coerce").fillna(0)
            if "semaine" not in df_v.columns:
                df_v["semaine"] = df_v["date"].apply(_get_semaine_iso)
            df_v["semaine"] = pd.to_numeric(df_v["semaine"], errors="coerce").fillna(0).astype(int)

            # ── Sélecteur de période ────────────────────────────────────────
            toutes_semaines = sorted(df_v["semaine"].unique(), reverse=True)
            sem_options = ["Toutes"] + [f"Semaine {s}" for s in toutes_semaines]
            filtre_sem = st.selectbox("Filtrer par semaine", sem_options, key="dash_vente_sem")
            if filtre_sem != "Toutes":
                sem_val = int(filtre_sem.replace("Semaine ", ""))
                df_filtre = df_v[df_v["semaine"] == sem_val]
            else:
                df_filtre = df_v

            # ── Coûts achats depuis factures ────────────────────────────────
            factures_par_sem: dict = {}
            for f in factures_list:
                s = _get_semaine_iso(f.get("date", ""))
                if s:
                    factures_par_sem[s] = factures_par_sem.get(s, 0.0) + float(f.get("total_ht", 0))
            cout_achats_total = sum(
                factures_par_sem.get(s, 0)
                for s in df_filtre["semaine"].unique()
            )

            # ── KPIs ────────────────────────────────────────────────────────
            ca_total     = df_filtre["ca_ht"].sum()
            nb_jours     = df_filtre["date"].nunique()
            ca_moyen_j   = ca_total / nb_jours if nb_jours > 0 else 0
            marge_brute  = ca_total - cout_achats_total
            food_cost_pct = (cout_achats_total / ca_total * 100) if ca_total > 0 else 0
            total_couverts = df_filtre["nb_couverts"].sum()
            ca_par_couvert = ca_total / total_couverts if total_couverts > 0 else 0

            kc1, kc2, kc3, kc4, kc5 = st.columns(5)
            kc1.metric("CA HT total", f"{ca_total:,.0f} €")
            kc2.metric("CA moyen / jour", f"{ca_moyen_j:,.0f} €")
            kc3.metric("Marge brute", f"{marge_brute:,.0f} €",
                       delta=f"{(marge_brute/ca_total*100):.1f} %" if ca_total > 0 else None)
            kc4.metric("Food Cost %", f"{food_cost_pct:.1f} %",
                       delta=f"cible < 30 %", delta_color="inverse")
            kc5.metric("CA / couvert", f"{ca_par_couvert:.2f} €" if total_couverts > 0 else "—")

            st.divider()

            # ── Graphique 1 : CA vs Coûts achats par semaine ─────────────────
            semaines_communes = sorted(
                set(df_v["semaine"].unique()) | set(factures_par_sem.keys())
            )
            df_croise = pd.DataFrame({
                "Semaine": semaines_communes,
                "CA HT (€)": [df_v[df_v["semaine"]==s]["ca_ht"].sum() for s in semaines_communes],
                "Coûts achats (€)": [factures_par_sem.get(s, 0) for s in semaines_communes],
            })
            df_croise["Marge brute (€)"] = df_croise["CA HT (€)"] - df_croise["Coûts achats (€)"]
            df_croise["Semaine"] = df_croise["Semaine"].astype(str).apply(lambda x: f"S{x}")

            col_g1, col_g2 = st.columns(2)
            with col_g1:
                st.markdown("##### CA vs Coûts achats par semaine")
                fig1 = go.Figure()
                fig1.add_trace(go.Bar(name="CA HT", x=df_croise["Semaine"],
                                      y=df_croise["CA HT (€)"], marker_color="#2ecc71"))
                fig1.add_trace(go.Bar(name="Coûts achats", x=df_croise["Semaine"],
                                      y=df_croise["Coûts achats (€)"], marker_color="#e74c3c"))
                fig1.add_trace(go.Scatter(name="Marge brute", x=df_croise["Semaine"],
                                          y=df_croise["Marge brute (€)"],
                                          mode="lines+markers", line=dict(color="#f39c12", width=2)))
                fig1.update_layout(barmode="group", height=350, legend=dict(orientation="h"),
                                   yaxis_ticksuffix=" €", xaxis_title="Semaine")
                st.plotly_chart(fig1, use_container_width=True)

            # ── Graphique 2 : Evolution CA journalier ────────────────────────
            with col_g2:
                st.markdown("##### Evolution du CA journalier")
                df_daily = df_v.groupby("date")["ca_ht"].sum().reset_index().sort_values("date")
                fig2 = px.area(df_daily, x="date", y="ca_ht",
                               labels={"date": "Date", "ca_ht": "CA HT (€)"},
                               color_discrete_sequence=["#3498db"])
                fig2.update_layout(height=350, yaxis_ticksuffix=" €")
                st.plotly_chart(fig2, use_container_width=True)

            # ── Croisement production : CA / kg produit ──────────────────────
            if not data.empty:
                st.divider()
                st.markdown("##### Croisement CA × Production — Valeur ajoutée / kg")
                ca_par_sem_prod = df_v.groupby("semaine")["ca_ht"].sum().reset_index()
                ca_par_sem_prod.columns = ["Semaine", "CA HT"]
                prod_par_sem = data[["Semaine", "Kg produits global", "Commandes",
                                     "Euro_kilo_global"]].copy()
                prod_par_sem["Semaine"] = prod_par_sem["Semaine"].astype(int)
                df_merge = pd.merge(ca_par_sem_prod, prod_par_sem, on="Semaine", how="inner")
                df_merge["CA/kg produit (€)"] = (
                    df_merge["CA HT"] / df_merge["Kg produits global"]
                ).replace([float("inf"), float("-inf")], 0).fillna(0).round(3)
                df_merge["CA/Commande (€)"] = (
                    df_merge["CA HT"] / df_merge["Commandes"]
                ).replace([float("inf"), float("-inf")], 0).fillna(0).round(2)

                col_g3, col_g4 = st.columns(2)
                with col_g3:
                    fig3 = make_subplots(specs=[[{"secondary_y": True}]])
                    sem_labels = df_merge["Semaine"].astype(str).apply(lambda x: f"S{x}")
                    fig3.add_trace(go.Bar(name="CA HT", x=sem_labels,
                                          y=df_merge["CA HT"], marker_color="#2ecc71"), secondary_y=False)
                    fig3.add_trace(go.Scatter(name="CA/kg prod.", x=sem_labels,
                                              y=df_merge["CA/kg produit (€)"],
                                              mode="lines+markers",
                                              line=dict(color="#9b59b6", width=2)), secondary_y=True)
                    fig3.update_layout(height=320, title_text="CA HT vs CA/kg produit",
                                       legend=dict(orientation="h"))
                    fig3.update_yaxes(title_text="CA HT (€)", secondary_y=False)
                    fig3.update_yaxes(title_text="€/kg", secondary_y=True)
                    st.plotly_chart(fig3, use_container_width=True)

                with col_g4:
                    fig4 = make_subplots(specs=[[{"secondary_y": True}]])
                    fig4.add_trace(go.Bar(name="CA HT", x=sem_labels,
                                          y=df_merge["CA HT"], marker_color="#2ecc71"), secondary_y=False)
                    fig4.add_trace(go.Scatter(name="CA/Commande", x=sem_labels,
                                              y=df_merge["CA/Commande (€)"],
                                              mode="lines+markers",
                                              line=dict(color="#e67e22", width=2)), secondary_y=True)
                    fig4.update_layout(height=320, title_text="CA HT vs CA / Commande",
                                       legend=dict(orientation="h"))
                    fig4.update_yaxes(title_text="CA HT (€)", secondary_y=False)
                    fig4.update_yaxes(title_text="€/Commande", secondary_y=True)
                    st.plotly_chart(fig4, use_container_width=True)

                # Tableau récap croisé
                st.markdown("**Tableau récapitulatif croisé**")
                df_recap = df_merge[["Semaine", "CA HT", "Kg produits global",
                                     "Commandes", "CA/kg produit (€)", "CA/Commande (€)",
                                     "Euro_kilo_global"]].copy()
                df_recap.columns = ["Semaine", "CA HT (€)", "Kg produits", "Commandes",
                                    "CA/kg prod. (€)", "CA/Commande (€)", "Coût/kg (€)"]
                st.dataframe(df_recap.sort_values("Semaine", ascending=False),
                             use_container_width=True, hide_index=True,
                             column_config={
                                 "CA HT (€)": st.column_config.NumberColumn(format="%.2f €"),
                                 "CA/kg prod. (€)": st.column_config.NumberColumn(format="%.3f €"),
                                 "CA/Commande (€)": st.column_config.NumberColumn(format="%.2f €"),
                                 "Coût/kg (€)": st.column_config.NumberColumn(format="%.3f €"),
                             })

            # ── Top recettes vendues ─────────────────────────────────────────
            all_lignes = []
            for v in ventes_list:
                for lig in v.get("lignes", []):
                    all_lignes.append({
                        "recette": lig.get("recette_nom", ""),
                        "categorie": lig.get("categorie", ""),
                        "quantite": float(lig.get("quantite", 0)),
                        "ca_ligne": float(lig.get("ca_ligne", 0)),
                    })
            if all_lignes:
                st.divider()
                st.markdown("##### Top recettes vendues")
                df_rec = pd.DataFrame(all_lignes)
                df_top = (df_rec.groupby("recette")
                               .agg(couverts=("quantite", "sum"), ca=("ca_ligne", "sum"))
                               .reset_index()
                               .sort_values("ca", ascending=False)
                               .head(10))
                df_top["prix_moyen_couvert"] = (df_top["ca"] / df_top["couverts"]).round(2)

                col_t1, col_t2 = st.columns(2)
                with col_t1:
                    fig5 = px.bar(df_top, x="ca", y="recette", orientation="h",
                                  labels={"ca": "CA (€)", "recette": ""},
                                  color="ca", color_continuous_scale="Greens")
                    fig5.update_layout(height=350, title="Top 10 recettes par CA",
                                       coloraxis_showscale=False)
                    fig5.update_xaxes(ticksuffix=" €")
                    st.plotly_chart(fig5, use_container_width=True)
                with col_t2:
                    fig6 = px.bar(df_top, x="couverts", y="recette", orientation="h",
                                  labels={"couverts": "Couverts", "recette": ""},
                                  color="couverts", color_continuous_scale="Blues")
                    fig6.update_layout(height=350, title="Top 10 recettes par couverts",
                                       coloraxis_showscale=False)
                    st.plotly_chart(fig6, use_container_width=True)

                # Croiser avec les coûts recettes pour food cost par recette
                recettes_list_dash = load_recettes()
                prix_dict_dash = load_ingredients()
                food_cost_rows = []
                for _, row in df_top.iterrows():
                    rec_obj = next((r for r in recettes_list_dash if r["nom"] == row["recette"]), None)
                    if rec_obj:
                        _, cout_total, _ = _calcul_fiche(rec_obj, prix_dict_dash)
                        nb_cvts = rec_obj.get("nb_couverts", 1) or 1
                        cout_couvert = cout_total / nb_cvts
                        ca_couvert = row["prix_moyen_couvert"]
                        marge = ca_couvert - cout_couvert
                        fc_pct = (cout_couvert / ca_couvert * 100) if ca_couvert > 0 else 0
                        food_cost_rows.append({
                            "Recette": row["recette"],
                            "CA total (€)": round(row["ca"], 2),
                            "Couverts": int(row["couverts"]),
                            "Prix vente/couvert (€)": round(ca_couvert, 2),
                            "Coût matière/couvert (€)": round(cout_couvert, 3),
                            "Marge/couvert (€)": round(marge, 2),
                            "Food Cost %": round(fc_pct, 1),
                        })
                if food_cost_rows:
                    st.markdown("**Analyse Food Cost par recette**")
                    df_fc = pd.DataFrame(food_cost_rows)
                    st.dataframe(df_fc, use_container_width=True, hide_index=True,
                        column_config={
                            "CA total (€)": st.column_config.NumberColumn(format="%.2f €"),
                            "Prix vente/couvert (€)": st.column_config.NumberColumn(format="%.2f €"),
                            "Coût matière/couvert (€)": st.column_config.NumberColumn(format="%.3f €"),
                            "Marge/couvert (€)": st.column_config.NumberColumn(format="%.2f €"),
                            "Food Cost %": st.column_config.ProgressColumn(
                                min_value=0, max_value=100, format="%.1f %%"),
                        })

# ─────────────────────────────────────────────────────────
# PAGE : INTÉGRATIONS CAISSE & LIVRAISON
# ─────────────────────────────────────────────────────────
elif page == "Intégrations":
    st.title("🔌 Intégrations Caisse & Livraison")
    st.caption("Connectez vos outils pour synchroniser automatiquement vos ventes dans le dashboard.")

    connexions = load_connexions()

    # Définition des systèmes
    POS_SYSTEMS = [
        {
            "id": "tiller",
            "nom": "Tiller by SumUp",
            "icon": "💳",
            "desc": "N°1 en France — +10 000 restaurants, tablette iPad, cloud",
            "color": "#1A73E8",
            "doc_url": "https://developer.sumup.com/docs",
            "fields": [
                {"key": "api_key", "label": "Bearer Token (API Key)", "secret": True,
                 "help": "Disponible dans votre espace SumUp Developer → Applications → Access Token"},
            ],
        },
        {
            "id": "laddition",
            "nom": "L'Addition",
            "icon": "🍽️",
            "desc": "100% française, spécialisée restauration, +12 000 établissements",
            "color": "#C0392B",
            "doc_url": "https://api.laddition.com",
            "fields": [
                {"key": "api_key", "label": "Clé API", "secret": True,
                 "help": "Paramètres → Intégrations → Clé API"},
                {"key": "restaurant_id", "label": "ID Restaurant", "secret": False,
                 "help": "Visible dans l'URL de votre back-office L'Addition"},
            ],
        },
        {
            "id": "lightspeed",
            "nom": "Lightspeed Restaurant",
            "icon": "⚡",
            "desc": "Solution premium cloud (ex-iKentoo), très utilisée à Paris",
            "color": "#FF6B35",
            "doc_url": "https://developers.lightspeedhq.com/restaurant",
            "fields": [
                {"key": "account_id", "label": "Account ID", "secret": False,
                 "help": "Visible dans Settings → Account Information"},
                {"key": "api_key", "label": "Clé API (Bearer Token)", "secret": True,
                 "help": "Générée dans Settings → API Access → Create Token"},
            ],
        },
        {
            "id": "zelty",
            "nom": "Zelty",
            "icon": "🟢",
            "desc": "Made in France, cloud natif, interface ultra-simple",
            "color": "#00A86B",
            "doc_url": "https://zelty.fr/api",
            "fields": [
                {"key": "api_token", "label": "Token API", "secret": True,
                 "help": "Réglages → Intégrations → API → Générer un token"},
            ],
        },
        {
            "id": "ubereats",
            "nom": "Uber Eats",
            "icon": "🛵",
            "desc": "Synchronisez vos commandes Uber Eats for Restaurants",
            "color": "#06C167",
            "doc_url": "https://developer.uber.com/docs/eats",
            "fields": [
                {"key": "client_id", "label": "Client ID", "secret": False,
                 "help": "developer.uber.com → My Apps → votre app → Credentials"},
                {"key": "client_secret", "label": "Client Secret", "secret": True,
                 "help": "developer.uber.com → My Apps → votre app → Credentials"},
                {"key": "store_id", "label": "Store UUID", "secret": False,
                 "help": "Disponible dans Uber Eats Manager → votre restaurant → UUID dans l'URL"},
            ],
        },
    ]

    # Afficher 2 cartes par ligne (5 systèmes = 2+2+1)
    for row_start in range(0, len(POS_SYSTEMS), 2):
        row_systems = POS_SYSTEMS[row_start:row_start + 2]
        cols = st.columns(len(row_systems), gap="large")

        for col_idx, pos in enumerate(row_systems):
            conn = connexions.get(pos["id"], {})
            is_connected = conn.get("connected", False)
            status_dot = "#22C55E" if is_connected else "#94A3B8"
            status_txt = "✅ Connecté" if is_connected else "⚫ Non connecté"
            last_sync = conn.get("updated_at", "")

            with cols[col_idx]:
                st.markdown(f"""
<div style="border:2px solid {pos['color']};border-radius:16px;padding:18px 20px 14px;
            background:linear-gradient(135deg,{pos['color']}18,#ffffff);">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
    <span style="font-size:1.9rem;line-height:1">{pos['icon']}</span>
    <span style="background:{status_dot};color:white;padding:3px 11px;
                 border-radius:20px;font-size:0.75em;font-weight:700;">{status_txt}</span>
  </div>
  <div style="font-size:1.05rem;font-weight:700;color:{pos['color']};margin-bottom:2px;">{pos['nom']}</div>
  <div style="font-size:0.8em;color:#64748B;">{pos['desc']}</div>
  {f'<div style="font-size:0.72em;color:#94A3B8;margin-top:4px;">Dernière sync : {last_sync}</div>' if last_sync else ''}
</div>""", unsafe_allow_html=True)

                st.markdown(f"[📖 Documentation API]({pos['doc_url']})")

                with st.expander("⚙️ Configurer", expanded=not is_connected):
                    with st.form(f"form_{pos['id']}"):
                        field_vals: dict = {}
                        for field in pos["fields"]:
                            field_vals[field["key"]] = st.text_input(
                                field["label"],
                                value=conn.get(field["key"], ""),
                                help=field["help"],
                                type="password" if field.get("secret") else "default",
                                key=f"inp_{pos['id']}_{field['key']}",
                            )
                        btn_save, btn_test = st.columns(2)
                        with btn_save:
                            do_save = st.form_submit_button("💾 Sauvegarder", use_container_width=True)
                        with btn_test:
                            do_test = st.form_submit_button("🔍 Tester", type="primary", use_container_width=True)

                        if do_save:
                            _db.collection(COLLECTION_CONNEXIONS).document(pos["id"]).set(
                                {**field_vals, "connected": False, "updated_at": str(date.today())},
                                merge=True
                            )
                            st.success("Identifiants sauvegardés.")
                            st.cache_data.clear()
                            st.rerun()

                        if do_test:
                            with st.spinner("Test en cours..."):
                                ok_t, msg_t = _test_pos_connection(pos["id"], field_vals)
                            if ok_t:
                                _db.collection(COLLECTION_CONNEXIONS).document(pos["id"]).set(
                                    {**field_vals, "connected": True, "updated_at": str(date.today())},
                                    merge=True
                                )
                                st.success(f"✅ {msg_t}")
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                _db.collection(COLLECTION_CONNEXIONS).document(pos["id"]).set(
                                    {"connected": False}, merge=True
                                )
                                st.error(f"❌ {msg_t}")

                if is_connected:
                    if st.button(f"🔄 Synchroniser les ventes", key=f"sync_{pos['id']}",
                                 use_container_width=True):
                        with st.spinner("Synchronisation en cours..."):
                            res = _sync_pos_data(pos["id"], conn)
                        if res["success"]:
                            st.success(f"✅ {res['message']}")
                            st.cache_data.clear()
                        else:
                            st.error(f"❌ {res['message']}")

        st.markdown("")  # espacement entre les rangées

    # ── Récapitulatif des connexions actives ─────────────────────────────────
    st.divider()
    actives = [(pos["nom"], pos["icon"]) for pos in POS_SYSTEMS
               if connexions.get(pos["id"], {}).get("connected", False)]
    if actives:
        st.markdown(f"**{len(actives)} intégration(s) active(s) :** "
                    + "  |  ".join(f"{icon} {nom}" for icon, nom in actives))
    else:
        st.info("Aucune intégration connectée. Configurez vos outils ci-dessus pour importer vos ventes automatiquement.")

# ─────────────────────────────────────────────────────────
# PAGE : MES APPS
# ─────────────────────────────────────────────────────────
elif page == "Mes Apps":
    st.title("📱 Mes Applications")
    st.caption("Tous vos outils au même endroit.")

    # ── Définition des apps ───────────────────────────────
    APPS = [
        {
            "nom": "Carte Fidélité",
            "icon": "🎁",
            "desc": "Programme de fidélité client — gestion des points, récompenses et historique.",
            "techno": "React · Gemini AI",
            "url": "https://ai.studio/apps/db088479-27fb-490e-863c-bc26ca729bc8",
            "color": "#7C3AED",
            "statut": "En ligne",
        },
        {
            "nom": "Restau 360",
            "icon": "🍽️",
            "desc": "Vue complète de la gestion restaurant — opérations, équipe, stocks.",
            "techno": "—",
            "url": "",  # À renseigner
            "color": "#EA580C",
            "statut": "URL à configurer",
        },
        {
            "nom": "Formation",
            "icon": "🎓",
            "desc": "Suivi des formations du personnel — HACCP, certifications, plannings.",
            "techno": "—",
            "url": "",  # À renseigner
            "color": "#0284C7",
            "statut": "URL à configurer",
        },
        {
            "nom": "Dashboard MEP",
            "icon": "📊",
            "desc": "Suivi de la production en cuisine — KPIs, fiches techniques, ventes.",
            "techno": "Python · Streamlit",
            "url": "",  # URL de ce dashboard
            "color": "#16A34A",
            "statut": "Cette app",
        },
    ]

    # Affichage 2 par ligne
    for i in range(0, len(APPS), 2):
        cols = st.columns(2, gap="large")
        for j, app in enumerate(APPS[i:i+2]):
            with cols[j]:
                has_url = bool(app["url"])
                st.markdown(f"""
<div class="app-card" style="background:linear-gradient(135deg,{app['color']},{app['color']}CC);
     border: none; box-shadow: 0 6px 20px {app['color']}44;">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;">
    <span style="font-size:2.2rem;line-height:1">{app['icon']}</span>
    <span style="background:rgba(255,255,255,0.2);color:white;padding:3px 10px;
                 border-radius:20px;font-size:0.72em;font-weight:700;">{app['statut']}</span>
  </div>
  <div style="color:white;font-size:1.1rem;font-weight:700;margin:10px 0 4px">{app['nom']}</div>
  <div style="color:rgba(255,255,255,0.85);font-size:0.82em;line-height:1.4">{app['desc']}</div>
  {f'<span class="app-tag">{app["techno"]}</span>' if app["techno"] != "—" else ""}
</div>""", unsafe_allow_html=True)

                if has_url and app["statut"] != "Cette app":
                    st.link_button(f"Ouvrir {app['nom']} →", app["url"], use_container_width=True)
                elif app["statut"] == "Cette app":
                    st.button("✅ Vous êtes ici", disabled=True, use_container_width=True, key=f"here_{i}_{j}")
                else:
                    st.button("🔗 URL à renseigner", disabled=True, use_container_width=True, key=f"todo_{i}_{j}")

    # ── Section "Ajouter une app" ─────────────────────────
    st.divider()
    with st.expander("➕ Ajouter / modifier une app dans cette page"):
        st.info(
            "Pour ajouter ou modifier une app, transmettez à votre développeur :\n"
            "- Le **nom** de l'app\n"
            "- L'**URL** de déploiement\n"
            "- Une **description** courte\n\n"
            "Les apps sont configurées directement dans le code (`APPS` liste dans la page 'Mes Apps')."
        )
