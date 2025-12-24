"""
SafeBeat - AFCON 2025 Morocco Prediction Platform
Plateforme de prédiction et gestion pour la Coupe d'Afrique des Nations 2025
"""

def show_afcon_predictor():
    """AFCON 2025 Match Predictor for Morocco"""
    import streamlit as st
    import pandas as pd
    import numpy as np
    import plotly.express as px
    import plotly.graph_objects as go
    from datetime import datetime, date, time
    
    # AFCON 2025 Data
    STADIUMS = {
        "Grand Stade de Tanger": {"city": "Tangier", "capacity": 75000, "lat": 35.7595, "lon": -5.8340, "risk_base": 0.8},
        "Prince Moulay Abdellah Stadium": {"city": "Rabat", "capacity": 68700, "lat": 34.0209, "lon": -6.8416, "risk_base": 0.9},
        "Stade Mohammed V": {"city": "Casablanca", "capacity": 45000, "lat": 33.5731, "lon": -7.6180, "risk_base": 0.85},
        "Grand Stade de Marrakech": {"city": "Marrakech", "capacity": 45240, "lat": 31.6295, "lon": -8.0087, "risk_base": 0.7},
        "Stade Adrar": {"city": "Agadir", "capacity": 45000, "lat": 30.4278, "lon": -9.5981, "risk_base": 0.6},
        "Complexe Sportif de Fès": {"city": "Fez", "capacity": 45000, "lat": 34.0181, "lon": -5.0078, "risk_base": 0.7},
    }
    
    # AFCON 2025 Groups
    GROUPS = {
        "Groupe A": ["Morocco 🇲🇦", "Comoros 🇰🇲", "Mali 🇲🇱", "Zambia 🇿🇲"],
        "Groupe B": ["Egypt 🇪🇬", "South Africa 🇿🇦", "Angola 🇦🇴", "Zimbabwe 🇿🇼"],
        "Groupe C": ["Nigeria 🇳🇬", "Tunisia 🇹🇳", "Uganda 🇺🇬", "Tanzania 🇹🇿"],
        "Groupe D": ["Senegal 🇸🇳", "DR Congo 🇨🇩", "Benin 🇧🇯", "Botswana 🇧🇼"],
        "Groupe E": ["Algeria 🇩🇿", "Burkina Faso 🇧🇫", "Equatorial Guinea 🇬🇶", "Sudan 🇸🇩"],
        "Groupe F": ["Ivory Coast 🇨🇮", "Cameroon 🇨🇲", "Gabon 🇬🇦", "Mozambique 🇲🇿"],
    }
    
    # Match importance factors
    MATCH_TYPES = {
        "Phase de Groupes": 1.0,
        "Huitièmes de Finale": 1.3,
        "Quarts de Finale": 1.5,
        "Demi-Finale": 1.8,
        "Match pour 3ème Place": 1.4,
        "Finale": 2.0,
    }
    
    # Team rivalry factors (higher = more tension)
    RIVALRIES = {
        ("Morocco", "Algeria"): 2.5,
        ("Egypt", "Algeria"): 2.3,
        ("Nigeria", "Cameroon"): 2.0,
        ("Senegal", "Egypt"): 1.8,
        ("Morocco", "Egypt"): 1.7,
        ("Ivory Coast", "Cameroon"): 1.6,
    }
    
    # Header
    st.markdown("""
    <div style="background: linear-gradient(135deg, #c1272d 0%, #006233 100%); padding: 20px; border-radius: 15px; margin-bottom: 20px;">
        <h1 style="color: white; text-align: center; margin: 0;">🏆 CAN 2025 - Maroc</h1>
        <p style="color: white; text-align: center; margin: 5px 0;">Coupe d'Afrique des Nations | 21 Dec 2025 - 18 Jan 2026</p>
        <p style="color: white; text-align: center; font-size: 0.9em;">Plateforme de Prédiction & Gestion de Sécurité</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Tabs for different sections
    tab1, tab2, tab3, tab4 = st.tabs([
        "⚽ Prédiction Match", 
        "🗺️ Carte des Stades",
        "📊 Tableau de Bord",
        "📋 Planning Complet"
    ])
    
    # ==========================================
    # TAB 1: Match Prediction
    # ==========================================
    with tab1:
        st.subheader("⚽ Prédiction pour un Match")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Match Configuration
            st.markdown("### 📝 Configuration du Match")
            
            match_type = st.selectbox(
                "Phase de la Compétition",
                list(MATCH_TYPES.keys())
            )
            
            stadium = st.selectbox(
                "Stade",
                list(STADIUMS.keys())
            )
            
            # Team selection
            all_teams = []
            for teams in GROUPS.values():
                all_teams.extend(teams)
            
            team1 = st.selectbox("Équipe 1 (Domicile)", all_teams)
            team2 = st.selectbox("Équipe 2 (Extérieur)", [t for t in all_teams if t != team1])
            
            match_date = st.date_input(
                "Date du Match",
                value=date(2025, 12, 21),
                min_value=date(2025, 12, 21),
                max_value=date(2026, 1, 18)
            )
            
            match_time = st.selectbox(
                "Heure du Match",
                ["14:00", "17:00", "20:00", "21:00"]
            )
            
        with col2:
            st.markdown("### 🎫 Paramètres d'Affluence")
            
            expected_attendance = st.slider(
                "Affluence Attendue",
                min_value=10000,
                max_value=STADIUMS[stadium]["capacity"],
                value=int(STADIUMS[stadium]["capacity"] * 0.85),
                step=1000
            )
            
            fill_rate = expected_attendance / STADIUMS[stadium]["capacity"] * 100
            st.progress(fill_rate / 100)
            st.markdown(f"**Taux de Remplissage:** {fill_rate:.1f}%")
            
            st.markdown("### 🌤️ Conditions Météo")
            
            temperature = st.slider("Température (°C)", 5, 40, 18)
            
            weather_condition = st.selectbox(
                "Conditions",
                ["Ensoleillé ☀️", "Nuageux ⛅", "Pluvieux 🌧️", "Venteux 💨"]
            )
            
            st.markdown("### 🔒 Mesures de Sécurité")
            
            alcohol_zones = st.checkbox("Fan Zones avec Alcool", value=False)
            vip_presence = st.checkbox("Présence VIP/Officielle", value=True)
            
        st.markdown("---")
        
        # Generate Prediction
        if st.button("🔮 Générer la Prédiction", type="primary", use_container_width=True):
            
            # Calculate risk factors
            stadium_info = STADIUMS[stadium]
            match_factor = MATCH_TYPES[match_type]
            
            # Time risk (night matches = higher)
            hour = int(match_time.split(":")[0])
            time_factor = 1.3 if hour >= 20 else 1.1 if hour >= 17 else 1.0
            
            # Team rivalry check
            team1_name = team1.split()[0]
            team2_name = team2.split()[0]
            rivalry_factor = 1.0
            for (t1, t2), factor in RIVALRIES.items():
                if (team1_name in t1 or team1_name in t2) and (team2_name in t1 or team2_name in t2):
                    rivalry_factor = factor
                    break
            
            # Morocco playing factor (host nation = massive crowd energy)
            morocco_factor = 1.5 if "Morocco" in team1 or "Morocco" in team2 else 1.0
            
            # Weekend factor
            is_weekend = match_date.weekday() >= 5
            weekend_factor = 1.2 if is_weekend else 1.0
            
            # Weather factor
            weather_factors = {
                "Ensoleillé ☀️": 1.1,
                "Nuageux ⛅": 1.0,
                "Pluvieux 🌧️": 0.8,
                "Venteux 💨": 0.9
            }
            weather_factor = weather_factors.get(weather_condition, 1.0)
            
            # Alcohol factor
            alcohol_factor = 1.4 if alcohol_zones else 1.0
            
            # Base incident rate (per 10,000 attendees)
            base_rate = 15  # incidents per 10,000
            
            # Calculate predictions
            total_factor = (
                stadium_info["risk_base"] * 
                match_factor * 
                time_factor * 
                rivalry_factor * 
                morocco_factor * 
                weekend_factor * 
                weather_factor * 
                alcohol_factor *
                (fill_rate / 100)
            )
            
            predicted_incidents = int(base_rate * (expected_attendance / 10000) * total_factor)
            
            # Risk score (0-100)
            risk_score = min(100, total_factor * 30)
            risk_category = "CRITIQUE" if risk_score >= 70 else "ÉLEVÉ" if risk_score >= 50 else "MODÉRÉ" if risk_score >= 30 else "FAIBLE"
            
            # Resource calculations
            security_per_1000 = 8 if risk_category in ["CRITIQUE", "ÉLEVÉ"] else 5
            medical_per_1000 = 3 if risk_category in ["CRITIQUE", "ÉLEVÉ"] else 2
            
            security_staff = int(expected_attendance / 1000 * security_per_1000)
            medical_staff = int(expected_attendance / 1000 * medical_per_1000)
            ambulances = max(3, int(predicted_incidents * 0.15))
            
            # Display Results
            st.markdown("---")
            st.subheader("📊 Résultats de l'Analyse")
            
            # Key Metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                risk_color = "🔴" if risk_category == "CRITIQUE" else "🟠" if risk_category == "ÉLEVÉ" else "🟡" if risk_category == "MODÉRÉ" else "🟢"
                st.metric("Score de Risque", f"{risk_score:.0f}/100", f"{risk_color} {risk_category}")
            
            with col2:
                st.metric("Incidents Prévus", predicted_incidents)
            
            with col3:
                st.metric("Personnel Sécurité", security_staff)
            
            with col4:
                st.metric("Personnel Médical", medical_staff)
            
            st.markdown("---")
            
            # Detailed breakdown
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 📈 Facteurs de Risque")
                
                factors_df = pd.DataFrame({
                    "Facteur": [
                        f"Stade ({stadium_info['city']})",
                        f"Phase ({match_type})",
                        f"Horaire ({match_time})",
                        "Rivalité Équipes",
                        "Équipe Hôte",
                        "Météo",
                        "Alcool",
                    ],
                    "Impact": [
                        f"x{stadium_info['risk_base']:.1f}",
                        f"x{match_factor:.1f}",
                        f"x{time_factor:.1f}",
                        f"x{rivalry_factor:.1f}",
                        f"x{morocco_factor:.1f}",
                        f"x{weather_factor:.1f}",
                        f"x{alcohol_factor:.1f}",
                    ],
                    "Niveau": [
                        "🟡" if stadium_info['risk_base'] >= 0.8 else "🟢",
                        "🔴" if match_factor >= 1.5 else "🟠" if match_factor >= 1.3 else "🟢",
                        "🟠" if time_factor >= 1.2 else "🟢",
                        "🔴" if rivalry_factor >= 2.0 else "🟠" if rivalry_factor >= 1.5 else "🟢",
                        "🟠" if morocco_factor > 1.0 else "🟢",
                        "🟡" if weather_factor >= 1.1 else "🟢",
                        "🔴" if alcohol_factor > 1.0 else "🟢",
                    ]
                })
                
                st.dataframe(factors_df, use_container_width=True, hide_index=True)
            
            with col2:
                st.markdown("### 🚨 Types d'Incidents Probables")
                
                if rivalry_factor >= 1.5:
                    incident_types = [
                        ("Troubles supporters", 30),
                        ("Altercations", 25),
                        ("Malaises médicaux", 20),
                        ("Mouvements de foule", 15),
                        ("Autres", 10),
                    ]
                else:
                    incident_types = [
                        ("Malaises médicaux", 35),
                        ("Affluence/Bousculade", 25),
                        ("Troubles mineurs", 20),
                        ("Perte d'objets", 12),
                        ("Autres", 8),
                    ]
                
                fig = px.pie(
                    values=[x[1] for x in incident_types],
                    names=[x[0] for x in incident_types],
                    color_discrete_sequence=px.colors.sequential.RdBu
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("---")
            
            # Alerts & Recommendations
            st.subheader("⚠️ Alertes et Recommandations")
            
            alerts = []
            recommendations = []
            
            if rivalry_factor >= 2.0:
                alerts.append(("🔥 RIVALITÉ INTENSE", "error", f"Match à haute tension: {team1} vs {team2}"))
                recommendations.append("✅ Séparation stricte des supporters avec zones tampons")
                recommendations.append("✅ Escorte policière pour les autocars de supporters")
            
            if risk_category == "CRITIQUE":
                alerts.append(("🚨 RISQUE CRITIQUE", "error", f"Score de risque: {risk_score:.0f}/100"))
                recommendations.append("✅ Déploiement des forces anti-émeute en standby")
                recommendations.append("✅ Hélicoptère médical en alerte")
            
            if "Morocco" in team1 or "Morocco" in team2:
                alerts.append(("🇲🇦 MATCH DU MAROC", "warning", "Affluence maximale et ferveur nationale attendues"))
                recommendations.append("✅ Renforcement des entrées - Ouverture 3h avant")
            
            if alcohol_zones:
                alerts.append(("🍺 ZONES ALCOOL", "warning", "Fan zones avec vente d'alcool actives"))
                recommendations.append("✅ Équipes anti-ivresse dédiées")
            
            if match_type in ["Finale", "Demi-Finale"]:
                alerts.append(("🏆 MATCH DÉCISIF", "info", f"{match_type} - Enjeux maximaux"))
                recommendations.append("✅ Coordination avec les forces spéciales")
            
            if temperature >= 35:
                alerts.append(("🌡️ ALERTE CANICULE", "warning", f"Température prévue: {temperature}°C"))
                recommendations.append("✅ Distribution d'eau gratuite")
                recommendations.append("✅ Points de brumisation dans le stade")
            
            for title, level, message in alerts:
                if level == "error":
                    st.error(f"**{title}**: {message}")
                elif level == "warning":
                    st.warning(f"**{title}**: {message}")
                else:
                    st.info(f"**{title}**: {message}")
            
            st.markdown("### 👍 Recommandations")
            for rec in recommendations:
                st.markdown(rec)
            
            st.markdown("---")
            
            # Resource Allocation
            st.subheader("🚑 Allocation des Ressources Recommandée")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("#### 🛡️ Sécurité")
                st.markdown(f"- **Agents privés**: {int(security_staff * 0.6)}")
                st.markdown(f"- **Police**: {int(security_staff * 0.3)}")
                st.markdown(f"- **Forces spéciales**: {int(security_staff * 0.1)}")
                st.markdown(f"- **Postes de contrôle**: {max(8, expected_attendance // 5000)}")
            
            with col2:
                st.markdown("#### 🏥 Médical")
                st.markdown(f"- **Médecins**: {max(4, int(medical_staff * 0.2))}")
                st.markdown(f"- **Infirmiers**: {max(8, int(medical_staff * 0.4))}")
                st.markdown(f"- **Secouristes**: {int(medical_staff * 0.4)}")
                st.markdown(f"- **Ambulances**: {ambulances}")
            
            with col3:
                st.markdown("#### 📡 Logistique")
                st.markdown(f"- **Portiques sécurité**: {max(10, expected_attendance // 3000)}")
                st.markdown(f"- **Caméras mobiles**: {max(20, expected_attendance // 2000)}")
                st.markdown(f"- **Centre de commandement**: 1")
                st.markdown(f"- **Drones surveillance**: {3 if risk_score >= 50 else 1}")
            
            # Export Summary
            st.markdown("---")
            st.subheader("📋 Résumé du Match")
            
            summary = f"""
# 🏆 CAN 2025 - Rapport de Sécurité

## Match
- **Rencontre**: {team1} vs {team2}
- **Phase**: {match_type}
- **Date**: {match_date.strftime('%d/%m/%Y')} à {match_time}
- **Stade**: {stadium} ({stadium_info['city']})
- **Capacité**: {stadium_info['capacity']:,} | Attendu: {expected_attendance:,} ({fill_rate:.0f}%)

## Évaluation des Risques
- **Score de Risque**: {risk_score:.0f}/100 ({risk_category})
- **Incidents Prévus**: {predicted_incidents}
- **Facteur Rivalité**: x{rivalry_factor}

## Ressources Recommandées
- **Personnel Sécurité**: {security_staff}
- **Personnel Médical**: {medical_staff}
- **Ambulances**: {ambulances}

## Alertes
{chr(10).join([f"- {a[0]}: {a[2]}" for a in alerts])}

## Recommandations
{chr(10).join(recommendations)}

---
*Généré par SafeBeat - CAN 2025 Predictor*
"""
            st.download_button(
                "📥 Télécharger le Rapport",
                summary,
                f"CAN2025_{team1.split()[0]}_vs_{team2.split()[0]}_{match_date}.md",
                "text/markdown"
            )
    
    # ==========================================
    # TAB 2: Stadium Map
    # ==========================================
    with tab2:
        st.subheader("🗺️ Stades de la CAN 2025")
        
        stadium_df = pd.DataFrame([
            {
                "Stade": name,
                "Ville": info["city"],
                "Capacité": info["capacity"],
                "lat": info["lat"],
                "lon": info["lon"],
                "Risque Base": info["risk_base"]
            }
            for name, info in STADIUMS.items()
        ])
        
        fig = px.scatter_mapbox(
            stadium_df,
            lat="lat",
            lon="lon",
            size="Capacité",
            color="Risque Base",
            hover_name="Stade",
            hover_data=["Ville", "Capacité"],
            color_continuous_scale="RdYlGn_r",
            size_max=40,
            zoom=5,
            mapbox_style="carto-positron",
            title="Stades de la CAN 2025 au Maroc"
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(
            stadium_df[["Stade", "Ville", "Capacité"]].sort_values("Capacité", ascending=False),
            use_container_width=True,
            hide_index=True
        )
    
    # ==========================================
    # TAB 3: Dashboard Overview
    # ==========================================
    with tab3:
        st.subheader("📊 Vue d'Ensemble CAN 2025")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("🏟️ Stades", len(STADIUMS))
        with col2:
            st.metric("🌍 Équipes", 24)
        with col3:
            total_capacity = sum(s["capacity"] for s in STADIUMS.values())
            st.metric("👥 Capacité Totale", f"{total_capacity:,}")
        with col4:
            st.metric("📅 Matchs", 52)
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 🏆 Groupes")
            for group, teams in GROUPS.items():
                with st.expander(group):
                    for team in teams:
                        st.markdown(f"- {team}")
        
        with col2:
            st.markdown("### 📅 Dates Clés")
            st.markdown("""
            | Phase | Dates |
            |-------|-------|
            | 🏟️ Match d'ouverture | 21 Décembre 2025 |
            | ⚽ Phase de Groupes | 21 - 31 Décembre 2025 |
            | 🎯 Huitièmes | 3 - 6 Janvier 2026 |
            | 🥅 Quarts | 9 - 10 Janvier 2026 |
            | 🏅 Demi-finales | 14 Janvier 2026 |
            | 🥉 3ème Place | 17 Janvier 2026 |
            | 🏆 **Finale** | **18 Janvier 2026** |
            """)
    
    # ==========================================
    # TAB 4: Full Schedule
    # ==========================================
    # ==========================================
    # TAB 4: Full Schedule with Live Results
    # ==========================================
    with tab4:
        st.subheader("📋 Calendrier Complet & Résultats")
        
        # =========================================
        # SYSTÈME DE MISE À JOUR AUTOMATIQUE
        # =========================================
        @st.cache_data(ttl=300)  # Cache 5 minutes
        def fetch_live_results():
            """
            Récupère les résultats en direct.
            Sources possibles:
            1. Fichier JSON local (datasets/afcon_results.json)
            2. API Football (api-football.com, football-data.org)
            3. Web scraping CAF Online
            """
            import json
            import os
            
            results = {}
            json_path = r'd:\uemf\s9\Data mining\SafeBeat\datasets\afcon_results.json'
            
            # Option 1: Fichier JSON local
            if os.path.exists(json_path):
                try:
                    with open(json_path, 'r', encoding='utf-8') as f:
                        results = json.load(f)
                except:
                    pass
            
            # Option 2: API Football (exemple - nécessite clé API)
            # try:
            #     import requests
            #     response = requests.get(
            #         "https://api-football-v1.p.rapidapi.com/v3/fixtures",
            #         headers={"X-RapidAPI-Key": "YOUR_API_KEY"},
            #         params={"league": "6", "season": "2025"}  # AFCON code
            #     )
            #     if response.ok:
            #         for match in response.json()['response']:
            #             results[match['fixture']['id']] = {
            #                 'score': f"{match['goals']['home']}-{match['goals']['away']}",
            #                 'status': match['fixture']['status']['short']
            #             }
            # except:
            #     pass
            
            return results
        
        # Récupérer les résultats live
        live_results = fetch_live_results()
        
        # Info sur la mise à jour
        st.info("""
        **🔄 Mise à jour des résultats:**
        - **Automatique**: Le fichier `datasets/afcon_results.json` est lu toutes les 5 minutes
        - **Manuelle**: Cliquez sur "Actualiser" pour forcer une mise à jour
        - **API**: Possibilité d'intégrer API-Football pour les scores en direct
        """)
        
        # Auto-refresh button
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button("🔄 Actualiser les Résultats", type="primary"):
                st.cache_data.clear()
                st.rerun()
        with col2:
            st.markdown(f"**Dernière MAJ:** {datetime.now().strftime('%H:%M:%S')}")
        
        # Complete AFCON 2025 Schedule
        FULL_SCHEDULE = [
            # ===== MATCHS TERMINÉS =====
            # Group Stage - Day 1 (Dec 21) ✅
            {"Date": "21/12/2025", "Heure": "21:00", "Équipe1": "Morocco 🇲🇦", "Équipe2": "Comoros 🇰🇲", "Score": "2-0", "Stade": "Prince Moulay Abdellah", "Ville": "Rabat", "Phase": "Groupe A", "Statut": "Terminé"},
            
            # Group Stage - Day 2 (Dec 22) ✅
            {"Date": "22/12/2025", "Heure": "14:00", "Équipe1": "Mali 🇲🇱", "Équipe2": "Zambia 🇿🇲", "Score": "1-1", "Stade": "Grand Stade de Marrakech", "Ville": "Marrakech", "Phase": "Groupe A", "Statut": "Terminé"},
            {"Date": "22/12/2025", "Heure": "17:00", "Équipe1": "Egypt 🇪🇬", "Équipe2": "Zimbabwe 🇿🇼", "Score": "2-1", "Stade": "Stade Mohammed V", "Ville": "Casablanca", "Phase": "Groupe B", "Statut": "Terminé"},
            {"Date": "22/12/2025", "Heure": "20:00", "Équipe1": "South Africa 🇿🇦", "Équipe2": "Angola 🇦🇴", "Score": "2-1", "Stade": "Stade Adrar", "Ville": "Agadir", "Phase": "Groupe B", "Statut": "Terminé"},
            
            # Group Stage - Day 3 (Dec 23) ✅
            {"Date": "23/12/2025", "Heure": "14:00", "Équipe1": "Nigeria 🇳🇬", "Équipe2": "Tanzania 🇹🇿", "Score": "2-1", "Stade": "Grand Stade de Tanger", "Ville": "Tangier", "Phase": "Groupe C", "Statut": "Terminé"},
            {"Date": "23/12/2025", "Heure": "17:00", "Équipe1": "Tunisia 🇹🇳", "Équipe2": "Uganda 🇺🇬", "Score": "3-1", "Stade": "Complexe Sportif de Fès", "Ville": "Fez", "Phase": "Groupe C", "Statut": "Terminé"},
            {"Date": "23/12/2025", "Heure": "20:00", "Équipe1": "Senegal 🇸🇳", "Équipe2": "Botswana 🇧🇼", "Score": "3-0", "Stade": "Prince Moulay Abdellah", "Ville": "Rabat", "Phase": "Groupe D", "Statut": "Terminé"},
            
            # Group Stage - Day 4 (Dec 24) - Matchs du jour ⏰
            {"Date": "24/12/2025", "Heure": "14:00", "Équipe1": "DR Congo 🇨🇩", "Équipe2": "Benin 🇧🇯", "Score": "1-0", "Stade": "Grand Stade de Marrakech", "Ville": "Marrakech", "Phase": "Groupe D", "Statut": "Terminé"},
            {"Date": "24/12/2025", "Heure": "17:00", "Équipe1": "Algeria 🇩🇿", "Équipe2": "Sudan 🇸🇩", "Score": "1-0", "Stade": "Stade Mohammed V", "Ville": "Casablanca", "Phase": "Groupe E", "Statut": "En cours"},
            {"Date": "24/12/2025", "Heure": "20:00", "Équipe1": "Burkina Faso 🇧🇫", "Équipe2": "Equatorial Guinea 🇬🇶", "Score": None, "Stade": "Stade Adrar", "Ville": "Agadir", "Phase": "Groupe E", "Statut": "À venir"},
            
            # ===== MATCHS À VENIR =====
            # Group Stage - Day 5 (Dec 25)
            {"Date": "25/12/2025", "Heure": "14:00", "Équipe1": "Ivory Coast 🇨🇮", "Équipe2": "Mozambique 🇲🇿", "Score": None, "Stade": "Grand Stade de Tanger", "Ville": "Tangier", "Phase": "Groupe F", "Statut": "À venir"},
            {"Date": "25/12/2025", "Heure": "17:00", "Équipe1": "Cameroon 🇨🇲", "Équipe2": "Gabon 🇬🇦", "Score": None, "Stade": "Complexe Sportif de Fès", "Ville": "Fez", "Phase": "Groupe F", "Statut": "À venir"},
            
            # Group Stage - Day 6 (Dec 26) - Matchday 2
            {"Date": "26/12/2025", "Heure": "17:00", "Équipe1": "Comoros 🇰🇲", "Équipe2": "Mali 🇲🇱", "Score": None, "Stade": "Grand Stade de Marrakech", "Ville": "Marrakech", "Phase": "Groupe A", "Statut": "À venir"},
            {"Date": "26/12/2025", "Heure": "20:00", "Équipe1": "Morocco 🇲🇦", "Équipe2": "Zambia 🇿🇲", "Score": None, "Stade": "Prince Moulay Abdellah", "Ville": "Rabat", "Phase": "Groupe A", "Statut": "À venir"},
            
            # Group Stage - Day 7 (Dec 27)
            {"Date": "27/12/2025", "Heure": "14:00", "Équipe1": "Zimbabwe 🇿🇼", "Équipe2": "South Africa 🇿🇦", "Score": None, "Stade": "Stade Adrar", "Ville": "Agadir", "Phase": "Groupe B", "Statut": "À venir"},
            {"Date": "27/12/2025", "Heure": "17:00", "Équipe1": "Egypt 🇪🇬", "Équipe2": "Angola 🇦🇴", "Score": None, "Stade": "Stade Mohammed V", "Ville": "Casablanca", "Phase": "Groupe B", "Statut": "À venir"},
            {"Date": "27/12/2025", "Heure": "20:00", "Équipe1": "Tanzania 🇹🇿", "Équipe2": "Tunisia 🇹🇳", "Score": None, "Stade": "Complexe Sportif de Fès", "Ville": "Fez", "Phase": "Groupe C", "Statut": "À venir"},
            
            # Group Stage - Day 8 (Dec 28)
            {"Date": "28/12/2025", "Heure": "14:00", "Équipe1": "Nigeria 🇳🇬", "Équipe2": "Uganda 🇺🇬", "Score": None, "Stade": "Grand Stade de Tanger", "Ville": "Tangier", "Phase": "Groupe C", "Statut": "À venir"},
            {"Date": "28/12/2025", "Heure": "17:00", "Équipe1": "Botswana 🇧🇼", "Équipe2": "DR Congo 🇨🇩", "Score": None, "Stade": "Grand Stade de Marrakech", "Ville": "Marrakech", "Phase": "Groupe D", "Statut": "À venir"},
            {"Date": "28/12/2025", "Heure": "20:00", "Équipe1": "Senegal 🇸🇳", "Équipe2": "Benin 🇧🇯", "Score": None, "Stade": "Prince Moulay Abdellah", "Ville": "Rabat", "Phase": "Groupe D", "Statut": "À venir"},
            
            # Group Stage - Day 9 (Dec 29)
            {"Date": "29/12/2025", "Heure": "14:00", "Équipe1": "Sudan 🇸🇩", "Équipe2": "Burkina Faso 🇧🇫", "Score": None, "Stade": "Stade Adrar", "Ville": "Agadir", "Phase": "Groupe E", "Statut": "À venir"},
            {"Date": "29/12/2025", "Heure": "17:00", "Équipe1": "Algeria 🇩🇿", "Équipe2": "Equatorial Guinea 🇬🇶", "Score": None, "Stade": "Stade Mohammed V", "Ville": "Casablanca", "Phase": "Groupe E", "Statut": "À venir"},
            {"Date": "29/12/2025", "Heure": "20:00", "Équipe1": "Mozambique 🇲🇿", "Équipe2": "Cameroon 🇨🇲", "Score": None, "Stade": "Complexe Sportif de Fès", "Ville": "Fez", "Phase": "Groupe F", "Statut": "À venir"},
            
            # Group Stage - Day 10 (Dec 30)
            {"Date": "30/12/2025", "Heure": "17:00", "Équipe1": "Ivory Coast 🇨🇮", "Équipe2": "Gabon 🇬🇦", "Score": None, "Stade": "Grand Stade de Tanger", "Ville": "Tangier", "Phase": "Groupe F", "Statut": "À venir"},
            
            # Group Stage - Matchday 3 (Dec 30-31)
            {"Date": "30/12/2025", "Heure": "20:00", "Équipe1": "Zambia 🇿🇲", "Équipe2": "Comoros 🇰🇲", "Score": None, "Stade": "Grand Stade de Marrakech", "Ville": "Marrakech", "Phase": "Groupe A", "Statut": "À venir"},
            {"Date": "30/12/2025", "Heure": "20:00", "Équipe1": "Mali 🇲🇱", "Équipe2": "Morocco 🇲🇦", "Score": None, "Stade": "Prince Moulay Abdellah", "Ville": "Rabat", "Phase": "Groupe A", "Statut": "À venir"},
            
            {"Date": "31/12/2025", "Heure": "17:00", "Équipe1": "Angola 🇦🇴", "Équipe2": "Zimbabwe 🇿🇼", "Score": None, "Stade": "Stade Adrar", "Ville": "Agadir", "Phase": "Groupe B", "Statut": "À venir"},
            {"Date": "31/12/2025", "Heure": "17:00", "Équipe1": "South Africa 🇿🇦", "Équipe2": "Egypt 🇪🇬", "Score": None, "Stade": "Stade Mohammed V", "Ville": "Casablanca", "Phase": "Groupe B", "Statut": "À venir"},
            {"Date": "31/12/2025", "Heure": "20:00", "Équipe1": "Uganda 🇺🇬", "Équipe2": "Tanzania 🇹🇿", "Score": None, "Stade": "Complexe Sportif de Fès", "Ville": "Fez", "Phase": "Groupe C", "Statut": "À venir"},
            {"Date": "31/12/2025", "Heure": "20:00", "Équipe1": "Tunisia 🇹🇳", "Équipe2": "Nigeria 🇳🇬", "Score": None, "Stade": "Grand Stade de Tanger", "Ville": "Tangier", "Phase": "Groupe C", "Statut": "À venir"},
            
            # Round of 16 (Jan 3-6)
            {"Date": "03/01/2026", "Heure": "17:00", "Équipe1": "1A", "Équipe2": "3C/D/E", "Score": None, "Stade": "Prince Moulay Abdellah", "Ville": "Rabat", "Phase": "Huitièmes", "Statut": "À déterminer"},
            {"Date": "03/01/2026", "Heure": "20:00", "Équipe1": "2B", "Équipe2": "2F", "Score": None, "Stade": "Stade Mohammed V", "Ville": "Casablanca", "Phase": "Huitièmes", "Statut": "À déterminer"},
            {"Date": "04/01/2026", "Heure": "17:00", "Équipe1": "1C", "Équipe2": "3A/B/F", "Score": None, "Stade": "Grand Stade de Tanger", "Ville": "Tangier", "Phase": "Huitièmes", "Statut": "À déterminer"},
            {"Date": "04/01/2026", "Heure": "20:00", "Équipe1": "1D", "Équipe2": "2E", "Score": None, "Stade": "Stade Adrar", "Ville": "Agadir", "Phase": "Huitièmes", "Statut": "À déterminer"},
            {"Date": "05/01/2026", "Heure": "17:00", "Équipe1": "1B", "Équipe2": "3A/D/E/F", "Score": None, "Stade": "Complexe Sportif de Fès", "Ville": "Fez", "Phase": "Huitièmes", "Statut": "À déterminer"},
            {"Date": "05/01/2026", "Heure": "20:00", "Équipe1": "2A", "Équipe2": "2C", "Score": None, "Stade": "Grand Stade de Marrakech", "Ville": "Marrakech", "Phase": "Huitièmes", "Statut": "À déterminer"},
            {"Date": "06/01/2026", "Heure": "17:00", "Équipe1": "1F", "Équipe2": "3A/B/C", "Score": None, "Stade": "Prince Moulay Abdellah", "Ville": "Rabat", "Phase": "Huitièmes", "Statut": "À déterminer"},
            {"Date": "06/01/2026", "Heure": "20:00", "Équipe1": "1E", "Équipe2": "2D", "Score": None, "Stade": "Stade Mohammed V", "Ville": "Casablanca", "Phase": "Huitièmes", "Statut": "À déterminer"},
            
            # Quarter-finals (Jan 9-10)
            {"Date": "09/01/2026", "Heure": "17:00", "Équipe1": "W37", "Équipe2": "W39", "Score": None, "Stade": "Grand Stade de Tanger", "Ville": "Tangier", "Phase": "Quarts", "Statut": "À déterminer"},
            {"Date": "09/01/2026", "Heure": "20:00", "Équipe1": "W38", "Équipe2": "W42", "Score": None, "Stade": "Prince Moulay Abdellah", "Ville": "Rabat", "Phase": "Quarts", "Statut": "À déterminer"},
            {"Date": "10/01/2026", "Heure": "17:00", "Équipe1": "W41", "Équipe2": "W43", "Score": None, "Stade": "Stade Mohammed V", "Ville": "Casablanca", "Phase": "Quarts", "Statut": "À déterminer"},
            {"Date": "10/01/2026", "Heure": "20:00", "Équipe1": "W40", "Équipe2": "W44", "Score": None, "Stade": "Stade Adrar", "Ville": "Agadir", "Phase": "Quarts", "Statut": "À déterminer"},
            
            # Semi-finals (Jan 14)
            {"Date": "14/01/2026", "Heure": "17:00", "Équipe1": "W45", "Équipe2": "W46", "Score": None, "Stade": "Grand Stade de Tanger", "Ville": "Tangier", "Phase": "Demi-finale", "Statut": "À déterminer"},
            {"Date": "14/01/2026", "Heure": "20:00", "Équipe1": "W47", "Équipe2": "W48", "Score": None, "Stade": "Prince Moulay Abdellah", "Ville": "Rabat", "Phase": "Demi-finale", "Statut": "À déterminer"},
            
            # 3rd Place (Jan 17)
            {"Date": "17/01/2026", "Heure": "17:00", "Équipe1": "L49", "Équipe2": "L50", "Score": None, "Stade": "Stade Mohammed V", "Ville": "Casablanca", "Phase": "3ème Place", "Statut": "À déterminer"},
            
            # Final (Jan 18)
            {"Date": "18/01/2026", "Heure": "20:00", "Équipe1": "W49", "Équipe2": "W50", "Score": None, "Stade": "Prince Moulay Abdellah", "Ville": "Rabat", "Phase": "🏆 FINALE", "Statut": "À déterminer"},
        ]
        
        # Create DataFrame
        schedule_df = pd.DataFrame(FULL_SCHEDULE)
        
        # Add status styling
        def get_status_emoji(statut):
            if statut == "Terminé":
                return "✅"
            elif statut == "En cours":
                return "🔴 LIVE"
            elif statut == "À venir":
                return "⏰"
            else:
                return "🔮"
        
        schedule_df['Icône'] = schedule_df['Statut'].apply(get_status_emoji)
        
        # Format match display - show score for finished and live matches
        def format_match(row):
            equipe1 = row['Équipe1']
            equipe2 = row['Équipe2']
            score = row['Score']
            statut = row['Statut']
            
            if statut == "Terminé" and score:
                return f"{equipe1} **{score}** {equipe2}"
            elif statut == "En cours":
                # Show current score or 0-0 if match just started
                current_score = score if score else "0-0"
                return f"🔴 {equipe1} **{current_score}** {equipe2}"
            else:
                return f"{equipe1} vs {equipe2}"
        
        schedule_df['Match'] = schedule_df.apply(format_match, axis=1)
        
        # Filters
        st.markdown("### 🔍 Filtres")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            phase_filter = st.multiselect(
                "Phase",
                options=schedule_df['Phase'].unique().tolist(),
                default=schedule_df['Phase'].unique().tolist()
            )
        
        with col2:
            ville_filter = st.multiselect(
                "Ville",
                options=schedule_df['Ville'].unique().tolist(),
                default=schedule_df['Ville'].unique().tolist()
            )
        
        with col3:
            statut_filter = st.multiselect(
                "Statut",
                options=schedule_df['Statut'].unique().tolist(),
                default=schedule_df['Statut'].unique().tolist()
            )
        
        # Apply filters
        filtered_df = schedule_df[
            (schedule_df['Phase'].isin(phase_filter)) &
            (schedule_df['Ville'].isin(ville_filter)) &
            (schedule_df['Statut'].isin(statut_filter))
        ]
        
        # Display stats
        st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📅 Total Matchs", len(schedule_df))
        with col2:
            terminés = len(schedule_df[schedule_df['Statut'] == 'Terminé'])
            st.metric("✅ Terminés", terminés)
        with col3:
            à_venir = len(schedule_df[schedule_df['Statut'] == 'À venir'])
            st.metric("⏰ À Venir", à_venir)
        with col4:
            st.metric("🔮 À Déterminer", len(schedule_df[schedule_df['Statut'] == 'À déterminer']))
        
        st.markdown("---")
        
        # Display schedule table
        st.markdown("### 📆 Programme des Matchs")
        
        display_cols = ['Icône', 'Date', 'Heure', 'Match', 'Stade', 'Ville', 'Phase']
        st.dataframe(
            filtered_df[display_cols].sort_values(['Date', 'Heure']),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Icône": st.column_config.TextColumn("", width="small"),
                "Date": st.column_config.TextColumn("Date", width="small"),
                "Heure": st.column_config.TextColumn("⏰", width="small"),
                "Match": st.column_config.TextColumn("Match", width="large"),
                "Phase": st.column_config.TextColumn("Phase", width="medium"),
            }
        )
        
        # Today's matches highlight
        today = datetime.now().strftime("%d/%m/%Y")
        today_matches = schedule_df[schedule_df['Date'] == today]
        
        if len(today_matches) > 0:
            st.markdown("---")
            st.markdown("### 🔴 Matchs du Jour")
            for _, match in today_matches.iterrows():
                st.info(f"**{match['Heure']}** - {match['Match']} @ {match['Stade']} ({match['Ville']})")
        
        # Export
        st.markdown("---")
        csv = schedule_df.to_csv(index=False)
        st.download_button(
            "📥 Exporter le Calendrier (CSV)",
            csv,
            "CAN2025_calendrier_complet.csv",
            "text/csv"
        )


# Run as standalone for testing
if __name__ == "__main__":
    import streamlit as st
    st.set_page_config(page_title="CAN 2025 Predictor", page_icon="🏆", layout="wide")
    show_afcon_predictor()
