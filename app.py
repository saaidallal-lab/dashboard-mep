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
warnings.filterwarnings('ignore')

# Initialisation Firebase — st.secrets en production, firebase-key.json en local
FIRESTORE_COLLECTION = "kpi_2026"

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
                   'Global_kg_h', 'Kg produits global', 'Euro_kilo_global', 'Commandes']
    for col in cols_to_fix:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    return df.dropna(subset=['Semaine']).sort_values('Semaine').reset_index(drop=True)

# --- CHARGEMENT DES DONNÉES ---
try:
    data = load_data()
    # On récupère les 6 dernières semaines pour l'affichage (si assez de données)
    last_6_weeks = data.tail(6) if not data.empty else data
    latest_week = data.iloc[-1] if not data.empty else None
except Exception as e:
    st.error(f"Erreur lors de la lecture du fichier : {e}")
    st.stop()

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
        
        # Productivité Globale (Kg/H)
        prod_val = latest_week['Global_kg_h'] if 'Global_kg_h' in latest_week and pd.notna(latest_week['Global_kg_h']) else 0
        prod_delta = calculate_delta(prod_val, prev_week['Global_kg_h'] if prev_week is not None and 'Global_kg_h' in prev_week else None)
        col3.metric("Productivité", f"{prod_val:.2f} kg/h", delta=f"{prod_delta:.1f}%" if prod_delta is not None else None)
        if prod_val > 0:
            pct_to_do_prod = ((OBJECTIFS_KGH_ETAPE1['Global'] - prod_val) / prod_val) * 100
            if pct_to_do_prod > 0:
                heures_a_supprimer_prod = th_val - (prod_val * th_val / OBJECTIFS_KGH_ETAPE1['Global'])
                col3.caption(f"🎯 Reste pour Étape 1 : +{pct_to_do_prod:.1f}% (-{int(round(heures_a_supprimer_prod))} h)")
            else:
                col3.caption("🎯 Étape 1 atteinte ! 🎉")

        # Euro/Kilo Global
        ek_val = latest_week['Euro_kilo_global'] if 'Euro_kilo_global' in latest_week and pd.notna(latest_week['Euro_kilo_global']) else 0
        ek_delta = calculate_delta(ek_val, prev_week['Euro_kilo_global'] if prev_week is not None and 'Euro_kilo_global' in prev_week else None)
        col4.metric("Euro/Kilo", f"{ek_val:.2f} €/kg", delta=f"{ek_delta:.1f}%" if ek_delta is not None else None, delta_color="inverse")
        if ek_val > 0:
            pct_to_do_ek = ((ek_val - OBJECTIF_EURO_KG_ETAPE1) / ek_val) * 100
            if pct_to_do_ek > 0:
                heures_a_supprimer_ek = th_val * (ek_val - OBJECTIF_EURO_KG_ETAPE1) / ek_val
                col4.caption(f"🎯 Reste pour Étape 1 : -{pct_to_do_ek:.1f}% (-{int(round(heures_a_supprimer_ek))} h)")
            else:
                col4.caption("🎯 Étape 1 atteinte ! 🎉")

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
                name="Objectif Kg/H (Étape 1)",
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
                    name="Objectif €/Kg (Étape 1)",
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
                
            postes = ['Chaud', 'Légumerie', 'Sushi', 'Découpe', 'Mix', 'Mélange', 'Désinfection', 'Traçabilité', 'CF tampon']
            
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
                
                col_pod1, col_pod2 = st.columns(2)
                
                with col_pod1:
                    st.markdown("#### 📉 Top 3 Baisses d'Heures")
                    for i, perf in enumerate(top_heures):
                        emoji_medaille = ["🥇", "🥈", "🥉"][i]
                        sign = "+" if perf['delta'] > 0 else ""
                        st.info(f"{emoji_medaille} **{POSTES_EMOJIS.get(perf['poste'], perf['poste'])}** : {sign}{perf['delta']:.1f}%")

                with col_pod2:
                    st.markdown("#### 🚀 Top 3 Progressions Productivity (Kg/H)")
                    for i, perf in enumerate(top_kgh):
                        emoji_medaille = ["🥇", "🥈", "🥉"][i]
                        sign = "+" if perf['delta'] > 0 else ""
                        st.success(f"{emoji_medaille} **{POSTES_EMOJIS.get(perf['poste'], perf['poste'])}** : {sign}{perf['delta']:.1f}%")
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

        col1, col2 = st.columns([1, 2])

        with col1:
            st.subheader("Derniers chiffres")
            st.write(f"**Semaine actuelle (S{int(latest_week['Semaine'])}) :**")
            
            prev_week = last_6_weeks.iloc[-2] if len(last_6_weeks) > 1 else None
            
            # Heures consommées
            val_h = latest_week[col_name] if col_name in latest_week else 0
            delta_h = None
            if prev_week is not None and col_name in prev_week:
                prev_val_h = prev_week[col_name]
                if prev_val_h > 0:
                    delta_h = f"{((val_h - prev_val_h) / prev_val_h) * 100:.1f} %"
            st.metric(label="Heures consommées", value=f"{val_h:.1f} h", delta=delta_h, delta_color="inverse")
            
            # Productivité (Kg/H)
            kgh_col = f"{col_name}_kg_h"
            if kgh_col in latest_week:
                val_kgh = latest_week[kgh_col]
                delta_kgh = None
                if prev_week is not None and kgh_col in prev_week:
                    prev_val_kgh = prev_week[kgh_col]
                    if prev_val_kgh > 0:
                        delta_kgh = f"{((val_kgh - prev_val_kgh) / prev_val_kgh) * 100:.1f} %"
                st.metric(label="Productivité", value=f"{val_kgh:.1f} kg/h", delta=delta_kgh, delta_color="normal")
                if val_kgh > 0:
                    obj_kgh_etape1 = OBJECTIFS_KGH_ETAPE1.get(page)
                    if obj_kgh_etape1:
                        pct_to_do_poste = ((obj_kgh_etape1 - val_kgh) / val_kgh) * 100
                        if pct_to_do_poste > 0:
                            heures_a_supprimer_poste = val_h - (val_kgh * val_h / obj_kgh_etape1)
                            st.caption(f"🎯 Reste pour Étape 1 : +{pct_to_do_poste:.1f}% (-{int(round(heures_a_supprimer_poste))} h)")
                        else:
                            st.caption("🎯 Étape 1 atteinte ! 🎉")
            
            st.write("**Historique (6 sem.) :**")
            # N'afficher que les colonnes qui existent
            display_cols = ['Semaine']
            if col_name in last_6_weeks.columns:
                display_cols.append(col_name)
            if f"{col_name}_kg_h" in last_6_weeks.columns:
                display_cols.append(f"{col_name}_kg_h")
            st.dataframe(last_6_weeks[display_cols])

        with col2:
            st.subheader(f"Comparaison Heures vs Kg/H - {page}")
            
            kgh_col = f"{col_name}_kg_h"
            if col_name in last_6_weeks.columns:
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

                # Axe 1 : Heures (Barres)
                fig_poste.add_trace(
                    go.Bar(
                        x=last_6_weeks['Semaine'], 
                        y=last_6_weeks[col_name], 
                        name="Heures", 
                        marker_color=c_pastel,
                        text=last_6_weeks[col_name].fillna(0).round(0).astype(int).astype(str) + " <i>h</i>",
                        textposition='inside',
                        insidetextanchor='middle',
                        textfont=dict(weight='bold', color='black') # Noir pour bien contraster
                    ),
                    secondary_y=False,
                )
                
                # Axe 2 : Kg/H (Ligne)
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
                                name="Objectif Kg/H (Étape 1)",
                                mode='lines',
                                line=dict(color=c_fonce, dash='dash', width=2)
                            ),
                            secondary_y=True,
                        )
                
                fig_poste.update_layout(
                    xaxis_title="Semaine",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                # Forcer le départ à 0 pour éviter l'effet yoyo. Limiter à 160 les heures de Mix/Mélange.
                if page in ["Mix", "Mélange"]:
                    fig_poste.update_yaxes(title_text="Heures", secondary_y=False, range=[0, 160])
                else:
                    fig_poste.update_yaxes(title_text="Heures", secondary_y=False, rangemode="tozero")
                
                obj_kgh_poste = OBJECTIFS_KGH_ETAPE1.get(page, 0)
                max_kgh = last_6_weeks[kgh_col].max() if kgh_col in last_6_weeks.columns else 0
                max_y2_poste = max(max_kgh * 1.2, obj_kgh_poste * 1.2, 10)
                fig_poste.update_yaxes(title_text="Kg/H", secondary_y=True, range=[0, max_y2_poste])
                
                st.plotly_chart(fig_poste, use_container_width=True)
            else:
                st.warning(f"La colonne '{col_name}' est introuvable.")
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
            new_kilos = st.number_input("Kilos produits (total)", min_value=0.0, step=10.0)
        with col_t:
            new_taux = st.number_input("Taux horaire (€/h)", min_value=0.0, step=0.5, value=25.0)
            
        st.subheader("Heures consommées par poste")
        col1, col2, col3 = st.columns(3)
        with col1:
            new_chaud = st.number_input(f"{POSTES_EMOJIS['Chaud']} Heures", min_value=0.0, step=0.5)
            new_leg = st.number_input(f"{POSTES_EMOJIS['Légumerie']} Heures", min_value=0.0, step=0.5)
            new_desinfection = st.number_input(f"{POSTES_EMOJIS['Désinfection']} Heures", min_value=0.0, step=0.5)
        with col2:
            new_sushi = st.number_input(f"{POSTES_EMOJIS['Sushi']} Heures", min_value=0.0, step=0.5)
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
            # Utiliser les noms de colonnes originaux Firestore
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
                'Taux horaire': float(new_taux),
                
                # Productivité globale
                'Kg/H ': float(new_kilos / total_heure) if total_heure > 0 else 0.0,
                '€/kg (Mep global)': float((total_heure * new_taux) / new_kilos) if new_kilos > 0 else 0.0,
                
                # Productivité par poste
                'Chaud kg/H': float(new_kilos / new_chaud) if new_chaud > 0 else 0.0,
                'Légumerie KG/H': float(new_kilos / new_leg) if new_leg > 0 else 0.0,
                'Découpe KG/H': float(new_kilos / new_decoupe) if new_decoupe > 0 else 0.0,
                'Kg/H Sushi': float(new_kilos / new_sushi) if new_sushi > 0 else 0.0,
                'Mix KG/H': float(new_kilos / new_mix) if new_mix > 0 else 0.0,
                'Mélange KG/H': float(new_kilos / new_melange) if new_melange > 0 else 0.0,
                'Désinfection KG/H': float(new_kilos / new_desinfection) if new_desinfection > 0 else 0.0,
                'Traçabilité KG/H': float(new_kilos / new_tracabilite) if new_tracabilite > 0 else 0.0,
                'CF tampon KG/H': float(new_kilos / new_cf_tampon) if new_cf_tampon > 0 else 0.0,
                
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
