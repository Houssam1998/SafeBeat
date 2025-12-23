# 🎵 SafeBeat - Festival Safety Analytics Platform

> **Data Mining & AI for Festival Risk Management**  
> Analyse prédictive des incidents 911 lors d'événements/festivals à Austin, TX

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.52+-red.svg)](https://streamlit.io/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 📋 Description

SafeBeat est une plateforme d'analyse de données et d'intelligence artificielle qui :
- Analyse les corrélations entre festivals/événements et incidents 911
- Prédit les risques et la charge de travail des secours
- Génère des recommandations pour l'allocation des ressources

## 🚀 Fonctionnalités

### Analytics Dashboard (12 pages)
- 🏠 **Overview** - Métriques clés et tendances
- 🎯 **Risk Analysis** - Scores de risque par festival
- 🍺 **Alcohol Impact** - Analyse impact alcool (+31.5% incidents)
- 🌤️ **Weather Correlation** - Corrélations météo
- ⏱️ **Response Times** - Temps de réponse festival vs non-festival
- 🗺️ **Zone Clustering** - Carte interactive des zones à risque
- 📈 **Time Series Forecast** - Prévisions de charge de travail
- 🔗 **Association Rules** - Patterns découverts
- 🤖 **ML Models Summary** - Résumé des modèles
- 🎪 **Festival Predictor** - Simulateur d'événements
- 📑 **Report Gallery** - Visualisations et rapport
- 📊 **Raw Data** - Exploration des données

### Modèles ML Implémentés

| Modèle | Algorithme | Performance |
|--------|------------|-------------|
| Association Rules | Apriori | 2000+ règles, Lift max 3.2 |
| Zone Clustering | K-Means | 6 clusters, Silhouette 0.45 |
| Incident Classification | KNN (K=7) | Accuracy 26% (11 classes) |
| Workload Forecast | Gradient Boosting | R² = 0.78, MAPE = 12.3% |
| Priority Prediction | Random Forest | Accuracy 61% |

## 📊 Métriques de Performance des Modèles

### 1. Association Rules Mining
- **Itemsets fréquents** : 850+
- **Règles générées** : 2000+
- **Support minimum** : 0.5%
- **Confiance minimum** : 30%
- **Lift maximum** : 3.2

### 2. K-Means Clustering
- **Clusters optimaux** : 6
- **Score Silhouette** : 0.45
- **Zones HIGH_RISK** : 2 clusters
- **Inertie finale** : 12,450

### 3. KNN Classification
- **K optimal** : 7
- **Accuracy** : 26%
- **Classes** : 11 catégories d'incidents
- **F1-Score (Disturbance)** : 0.36

### 4. Time Series Forecast
- **Modèle** : Gradient Boosting Regressor
- **R²** : 0.78
- **MAE** : 45.2 calls/day
- **RMSE** : 62.8 calls/day
- **MAPE** : 12.3%

### 5. Random Forest Priority
- **Estimators** : 100
- **Max Depth** : 15
- **Accuracy** : 61%
- **CV Score** : 0.59 ± 0.02

## 🛠️ Installation

```bash
# Cloner le repository
git clone https://github.com/HamzaElyo/SafeBeat.git
cd SafeBeat

# Installer les dépendances
pip install -r requirements.txt

# Lancer le dashboard
streamlit run dashboard.py --global.developmentMode false
```

## 📁 Structure du Projet

```
SafeBeat/
├── dashboard.py              # Dashboard Streamlit principal
├── festival_predictor.py     # Module prédicteur d'événements
├── generate_report.py        # Générateur de rapports
├── RAPPORT_COMPLET.md        # Rapport détaillé
│
├── etl/                      # Scripts ETL
│   ├── dim_geo_lookup.py
│   ├── dim_venue.py
│   ├── clean_911_calls.py
│   ├── clean_events.py
│   ├── enrich_911_with_geo.py
│   ├── dim_weather.py
│   └── fact_festival_incidents.py
│
├── analysis/                 # Scripts d'analyse
│   ├── risk_analysis.py
│   ├── weather_correlation.py
│   ├── alcohol_impact.py
│   └── response_time_analysis.py
│
├── models/                   # Modèles ML
│   ├── association_rules.py
│   ├── kmeans_clustering.py
│   ├── knn_classification.py
│   ├── timeseries_forecast.py
│   ├── random_forest_priority.py
│   └── run_all_models.py
│
├── datasets/
│   ├── raw/                  # Données brutes
│   ├── cleaned/              # Données nettoyées
│   ├── enriched/             # Données enrichies
│   └── analysis/             # Résultats d'analyse
│
├── reports/                  # Visualisations générées
│   ├── association_rules_scatter.png
│   ├── risk_zone_map.html
│   ├── timeseries_forecast.png
│   └── ...
│
└── SafeBeat_Exploration.ipynb  # Notebook d'exploration
```

## 📈 Résultats Clés

### Impact de l'Alcool
- Événements avec alcool : **+31.5%** incidents
- Différence statistiquement significative (p < 0.05)

### Patterns Temporels
- Weekend + Soirée → Disturbances (Lift = 1.8)
- Nuit → Agressions (Lift = 2.1)

### Zones à Risque
- **Downtown/6th Street** : Zone HIGH_RISK principale
- **East Austin** : MEDIUM_RISK

## 🎪 Festival Predictor

Plateforme interactive pour simuler des événements :

1. **Entrez les paramètres** : nom, date, affluence, alcool, météo
2. **Obtenez les prédictions** : incidents prévus, score de risque
3. **Recevez les alertes** : conditions météo, zone à risque
4. **Recommandations** : personnel médical, sécurité, équipements

## 👥 Équipe

- **Hamza El Youbi** - [@HamzaElyo](https://github.com/HamzaElyo)
- **Houssam** - [@Houssam1998](https://github.com/Houssam1998)

## 📜 Licence

MIT License - voir [LICENSE](LICENSE)

## 🙏 Remerciements

- City of Austin Open Data Portal
- Foursquare Places API
- Open-Meteo Weather API
