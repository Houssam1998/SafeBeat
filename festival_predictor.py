"""
SafeBeat - Festival Event Predictor Page
Interactive platform for generating predictions based on user-defined festival parameters
"""

def show_festival_predictor():
    """Interactive Festival Event Predictor"""
    import streamlit as st
    import pandas as pd
    import numpy as np
    import pickle
    import sys
    sys.path.insert(0, 'D:\\python_packages')
    
    st.header("🎪 Festival Event Predictor")
    st.markdown("""
    **Plateforme de Prédiction d'Événements**  
    Entrez les paramètres de votre festival pour obtenir des prédictions et recommandations.
    """)
    
    MODELS_PATH = r'd:\uemf\s9\Data mining\SafeBeat\models'
    
    # Load models
    @st.cache_resource
    def load_models():
        models = {}
        try:
            with open(f"{MODELS_PATH}/rf_priority_model.pkl", 'rb') as f:
                models['priority'] = pickle.load(f)
        except:
            models['priority'] = None
        
        try:
            with open(f"{MODELS_PATH}/timeseries_model.pkl", 'rb') as f:
                models['forecast'] = pickle.load(f)
        except:
            models['forecast'] = None
        
        try:
            with open(f"{MODELS_PATH}/kmeans_model.pkl", 'rb') as f:
                models['clustering'] = pickle.load(f)
        except:
            models['clustering'] = None
            
        return models
    
    models = load_models()
    
    st.markdown("---")
    
    # Event Configuration Form
    st.subheader("📝 1. Configuration de l'Événement")
    
    col1, col2 = st.columns(2)
    
    with col1:
        event_name = st.text_input("Nom de l'événement", "Mon Festival")
        
        event_type = st.selectbox(
            "Type d'événement",
            ["Concert/Musique", "Festival Multi-jours", "Événement Sportif", 
             "Foire/Marché", "Parade/Défilé", "Autre"]
        )
        
        expected_attendance = st.slider(
            "Affluence attendue",
            min_value=100,
            max_value=100000,
            value=5000,
            step=100
        )
        
        has_alcohol = st.checkbox("Vente d'alcool", value=False)
        has_amplified_sound = st.checkbox("Son amplifié", value=True)
        
    with col2:
        event_date = st.date_input("Date de l'événement")
        
        start_time = st.time_input("Heure de début", value=pd.to_datetime("18:00").time())
        end_time = st.time_input("Heure de fin", value=pd.to_datetime("23:00").time())
        
        location = st.selectbox(
            "Zone de l'événement",
            ["Downtown/6th Street (HIGH RISK)", "East Austin (MEDIUM RISK)", 
             "North Austin (LOW RISK)", "South Austin (MEDIUM RISK)", 
             "University Area (MEDIUM RISK)", "Autre"]
        )
        
        road_closure = st.checkbox("Fermeture de route", value=False)
    
    st.markdown("---")
    
    # Weather Conditions
    st.subheader("🌤️ 2. Conditions Météo Prévues")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        temperature = st.slider("Température (°F)", 40, 110, 85)
        
    with col2:
        weather_condition = st.selectbox(
            "Condition météo",
            ["Clear (Clair)", "Partly Cloudy", "Cloudy", "Rain", "Thunderstorm"]
        )
        
    with col3:
        humidity = st.slider("Humidité (%)", 20, 100, 60)
    
    st.markdown("---")
    
    # Generate Predictions Button
    if st.button("🔮 Générer les Prédictions", type="primary", use_container_width=True):
        
        st.markdown("---")
        st.subheader("📊 3. Résultats de l'Analyse")
        
        # Calculate derived features
        hour = start_time.hour
        day_of_week = event_date.weekday()
        is_weekend = 1 if day_of_week >= 5 else 0
        month = event_date.month
        
        # Risk Zone mapping
        zone_risk = {
            "Downtown/6th Street (HIGH RISK)": ("HIGH", 30.27, -97.74),
            "East Austin (MEDIUM RISK)": ("MEDIUM", 30.26, -97.71),
            "North Austin (LOW RISK)": ("LOW", 30.35, -97.75),
            "South Austin (MEDIUM RISK)": ("MEDIUM", 30.22, -97.77),
            "University Area (MEDIUM RISK)": ("MEDIUM", 30.28, -97.73),
            "Autre": ("LOW", 30.30, -97.75)
        }
        
        zone_info = zone_risk.get(location, ("LOW", 30.30, -97.75))
        zone_category, lat, lon = zone_info
        
        # Weather risk
        weather_risk = {
            "Clear (Clair)": 1.2,
            "Partly Cloudy": 1.0,
            "Cloudy": 0.9,
            "Rain": 0.7,
            "Thunderstorm": 0.5
        }
        weather_factor = weather_risk.get(weather_condition, 1.0)
        
        # Calculate predicted incidents
        base_incidents = expected_attendance * 0.002  # 0.2% baseline
        
        # Apply modifiers
        if has_alcohol:
            base_incidents *= 1.315  # +31.5% from analysis
        if is_weekend:
            base_incidents *= 1.15   # +15% weekends
        if hour >= 20 or hour <= 4:  # Night hours
            base_incidents *= 1.25
        if zone_category == "HIGH":
            base_incidents *= 1.4
        elif zone_category == "MEDIUM":
            base_incidents *= 1.15
        
        base_incidents *= weather_factor
        
        predicted_incidents = int(base_incidents)
        
        # Priority distribution prediction
        if has_alcohol and is_weekend:
            priority_dist = {"Priority 0": 5, "Priority 1": 25, "Priority 2": 45, "Priority 3": 25}
        elif has_alcohol:
            priority_dist = {"Priority 0": 3, "Priority 1": 20, "Priority 2": 50, "Priority 3": 27}
        else:
            priority_dist = {"Priority 0": 2, "Priority 1": 15, "Priority 2": 48, "Priority 3": 35}
        
        # Calculate risk score
        risk_score = min(100, (
            (predicted_incidents / 50) * 0.4 +
            (1 if has_alcohol else 0) * 20 +
            (1 if zone_category == "HIGH" else 0.5 if zone_category == "MEDIUM" else 0) * 20 +
            (1 if is_weekend else 0) * 10 +
            (1 if hour >= 20 else 0) * 10
        ))
        
        risk_category = "HIGH" if risk_score >= 60 else "MEDIUM" if risk_score >= 30 else "LOW"
        
        # Display Results
        # Key Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Incidents Prévus", predicted_incidents)
        with col2:
            color = "🔴" if risk_category == "HIGH" else "🟠" if risk_category == "MEDIUM" else "🟢"
            st.metric("Score de Risque", f"{risk_score:.0f}/100")
        with col3:
            st.metric("Catégorie", f"{color} {risk_category}")
        with col4:
            # Calculate recommended staff
            medical_staff = max(2, int(predicted_incidents * 0.3))
            security_staff = max(4, int(predicted_incidents * 0.5))
            st.metric("Personnel Médical", f"{medical_staff}")
        
        st.markdown("---")
        
        # Detailed Predictions
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 🚨 Distribution des Priorités Prévue")
            import plotly.express as px
            
            priority_df = pd.DataFrame({
                'Priority': list(priority_dist.keys()),
                'Percentage': list(priority_dist.values())
            })
            fig = px.pie(priority_df, values='Percentage', names='Priority',
                        color='Priority',
                        color_discrete_map={
                            'Priority 0': '#ff4b4b',
                            'Priority 1': '#ff7f0e', 
                            'Priority 2': '#2ca02c',
                            'Priority 3': '#1f77b4'
                        })
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### 📋 Types d'Incidents Probables")
            if has_alcohol:
                incident_types = [
                    ("Intoxication/Ivresse", 35),
                    ("Troubles/Disturbances", 25),
                    ("Assistance Médicale", 20),
                    ("Agressions", 12),
                    ("Autres", 8)
                ]
            else:
                incident_types = [
                    ("Assistance Médicale", 30),
                    ("Troubles/Disturbances", 25),
                    ("Malaises (Chaleur)", 20),
                    ("Objets Perdus/Trouvés", 15),
                    ("Autres", 10)
                ]
            
            incident_df = pd.DataFrame(incident_types, columns=['Type', 'Probabilité (%)'])
            st.dataframe(incident_df, use_container_width=True)
        
        st.markdown("---")
        
        # Alerts & Recommendations
        st.subheader("⚠️ 4. Alertes et Recommandations")
        
        # Generate alerts based on conditions
        alerts = []
        recommendations = []
        
        if has_alcohol:
            alerts.append(("🍺 ALCOOL", "warning", "Événement avec alcool - Risque +31.5% incidents"))
            recommendations.append("✅ Déployer des équipes anti-intoxication dédiées")
            recommendations.append("✅ Installer des points d'eau et zones de repos")
        
        if zone_category == "HIGH":
            alerts.append(("📍 ZONE HIGH-RISK", "error", f"Localisation Downtown - Zone historiquement à haut risque"))
            recommendations.append("✅ Présence policière renforcée dès l'ouverture")
            recommendations.append("✅ Plan d'évacuation testé et communiqué")
        
        if temperature > 90:
            alerts.append(("🌡️ CANICULE", "warning", f"Température élevée ({temperature}°F) - Risque de malaises"))
            recommendations.append("✅ Doubler les équipes médicales pour insolation")
            recommendations.append("✅ Points de brumisation et distribution d'eau")
        
        if is_weekend and hour >= 20:
            alerts.append(("🌙 WEEKEND SOIR", "info", "Horaire à risque élevé (weekend + soirée)"))
            recommendations.append("✅ Équipes de nuit complètes (pas de roulement pendant pic)")
        
        if weather_condition in ["Thunderstorm", "Rain"]:
            alerts.append(("⛈️ MÉTÉO", "warning", f"Prévision: {weather_condition} - Plan intempéries"))
            recommendations.append("✅ Préparer zones abritées et annonces évacuation")
        
        if expected_attendance > 20000:
            alerts.append(("👥 GRANDE AFFLUENCE", "info", f"{expected_attendance:,} personnes attendues"))
            recommendations.append("✅ Points d'accès multiples pour éviter engorgement")
        
        # Display alerts
        for title, level, message in alerts:
            if level == "error":
                st.error(f"**{title}**: {message}")
            elif level == "warning":
                st.warning(f"**{title}**: {message}")
            else:
                st.info(f"**{title}**: {message}")
        
        # Display recommendations
        st.markdown("### 👍 Recommandations")
        for rec in recommendations:
            st.markdown(rec)
        
        st.markdown("---")
        
        # Resource Allocation
        st.subheader("🚑 5. Allocation des Ressources Recommandée")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("#### Personnel Médical")
            st.markdown(f"- **Médecins**: {max(1, int(medical_staff * 0.3))}")
            st.markdown(f"- **Infirmiers**: {max(2, int(medical_staff * 0.5))}")
            st.markdown(f"- **Secouristes**: {max(2, int(medical_staff * 0.2))}")
        
        with col2:
            st.markdown("#### Sécurité")
            st.markdown(f"- **Agents de sécurité**: {security_staff}")
            st.markdown(f"- **Forces de l'ordre**: {max(2, int(security_staff * 0.3))}")
            st.markdown(f"- **Postes fixes**: {max(2, int(expected_attendance / 3000))}")
        
        with col3:
            st.markdown("#### Équipements")
            st.markdown(f"- **Ambulances**: {max(1, int(predicted_incidents * 0.1))}")
            st.markdown(f"- **Points premiers soins**: {max(1, int(expected_attendance / 5000))}")
            st.markdown(f"- **Radios**: {medical_staff + security_staff}")
        
        st.markdown("---")
        
        # Summary Card
        st.subheader("📋 Résumé de l'Analyse")
        
        summary = f"""
| Paramètre | Valeur |
|-----------|--------|
| **Événement** | {event_name} |
| **Type** | {event_type} |
| **Date** | {event_date} ({start_time} - {end_time}) |
| **Affluence** | {expected_attendance:,} personnes |
| **Zone** | {location} |
| **Alcool** | {"Oui" if has_alcohol else "Non"} |
| **Météo** | {weather_condition}, {temperature}°F |
| **Incidents Prévus** | {predicted_incidents} |
| **Score de Risque** | {risk_score:.0f}/100 ({risk_category}) |
| **Personnel Recommandé** | {medical_staff + security_staff} personnes |
"""
        st.markdown(summary)
        
        # Export button
        st.download_button(
            "📥 Exporter le Rapport",
            summary,
            f"prediction_{event_name.replace(' ', '_')}.md",
            "text/markdown"
        )
