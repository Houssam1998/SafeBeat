"""
SafeBeat Dashboard - Interactive Analytics Dashboard with ML Models
Run with: streamlit run dashboard.py
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Page config
st.set_page_config(
    page_title="SafeBeat Analytics",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 1rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
    }
    .risk-high { color: #ff4b4b; font-weight: bold; }
    .risk-medium { color: #ffa500; font-weight: bold; }
    .risk-low { color: #00cc00; font-weight: bold; }
    .model-card {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Data paths
DATA_CLEANED = r'd:\uemf\s9\Data mining\SafeBeat\datasets\cleaned'
DATA_ENRICHED = r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched'
DATA_ANALYSIS = r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis'
MODELS_PATH = r'd:\uemf\s9\Data mining\SafeBeat\models'

@st.cache_data
def load_data():
    """Load all datasets"""
    data = {}
    
    # Analytics data
    try:
        data['risk_scores'] = pd.read_csv(f"{DATA_ANALYSIS}/festival_risk_scores.csv")
    except:
        data['risk_scores'] = pd.DataFrame()
    
    try:
        data['fact'] = pd.read_parquet(f"{DATA_ENRICHED}/fact_festival_incidents.parquet")
    except:
        data['fact'] = pd.DataFrame()
    
    try:
        data['weather'] = pd.read_csv(f"{DATA_ANALYSIS}/incidents_by_weather.csv")
    except:
        data['weather'] = pd.DataFrame()
    
    try:
        data['alcohol'] = pd.read_csv(f"{DATA_ANALYSIS}/alcohol_impact_summary.csv")
    except:
        data['alcohol'] = pd.DataFrame()
    
    try:
        data['response_time'] = pd.read_csv(f"{DATA_ANALYSIS}/response_time_summary.csv")
    except:
        data['response_time'] = pd.DataFrame()
    
    try:
        data['events_summary'] = pd.read_parquet(f"{DATA_ENRICHED}/events_incident_summary.parquet")
    except:
        data['events_summary'] = pd.DataFrame()
    
    try:
        data['df_911'] = pd.read_parquet(f"{DATA_CLEANED}/911_calls_cleaned.parquet")
        data['df_911']['response_datetime'] = pd.to_datetime(data['df_911']['response_datetime'])
    except:
        data['df_911'] = pd.DataFrame()
    
    # ML Model outputs
    try:
        data['kmeans_clusters'] = pd.read_parquet(f"{MODELS_PATH}/kmeans_zone_clusters.parquet")
    except:
        data['kmeans_clusters'] = pd.DataFrame()
    
    try:
        data['kmeans_stats'] = pd.read_csv(f"{MODELS_PATH}/kmeans_cluster_stats.csv")
    except:
        data['kmeans_stats'] = pd.DataFrame()
    
    try:
        data['timeseries_forecast'] = pd.read_parquet(f"{MODELS_PATH}/timeseries_forecast.parquet")
    except:
        data['timeseries_forecast'] = pd.DataFrame()
    
    try:
        data['association_rules'] = pd.read_csv(f"{MODELS_PATH}/association_rules_results.csv")
    except:
        data['association_rules'] = pd.DataFrame()
    
    try:
        data['rf_importance'] = pd.read_csv(f"{MODELS_PATH}/rf_feature_importance.csv")
    except:
        data['rf_importance'] = pd.DataFrame()
    
    return data

def main():
    # Header
    st.markdown('<h1 class="main-header">🎵 SafeBeat Analytics Dashboard</h1>', unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: gray;'>Festival Safety & 911 Incident Analysis for Austin, TX</p>", unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading data..."):
        data = load_data()
    
    # Sidebar
    st.sidebar.image("https://img.icons8.com/color/96/000000/music-record.png", width=80)
    st.sidebar.title("Navigation")
    
    st.sidebar.markdown("### 📊 Analytics")
    page = st.sidebar.radio(
        "Select Page",
        ["🏠 Overview", "🎯 Risk Analysis", "🍺 Alcohol Impact", "🌤️ Weather Correlation", 
         "⏱️ Response Times", "🗺️ Zone Clustering", "📈 Time Series Forecast", 
         "🔗 Association Rules", "🤖 ML Models Summary", "🎪 Festival Predictor",
         "📑 Report Gallery", "📊 Raw Data"],
        label_visibility="collapsed"
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### About")
    st.sidebar.info("SafeBeat analyzes festivals and 911 incidents using Data Mining & AI.")
    
    # Pages
    if page == "🏠 Overview":
        show_overview(data)
    elif page == "🎯 Risk Analysis":
        show_risk_analysis(data)
    elif page == "🍺 Alcohol Impact":
        show_alcohol_impact(data)
    elif page == "🌤️ Weather Correlation":
        show_weather_correlation(data)
    elif page == "⏱️ Response Times":
        show_response_times(data)
    elif page == "🗺️ Zone Clustering":
        show_zone_clustering(data)
    elif page == "📈 Time Series Forecast":
        show_timeseries_forecast(data)
    elif page == "🔗 Association Rules":
        show_association_rules(data)
    elif page == "🤖 ML Models Summary":
        show_ml_summary(data)
    elif page == "🎪 Festival Predictor":
        from festival_predictor import show_festival_predictor
        show_festival_predictor()
    elif page == "📑 Report Gallery":
        show_report_gallery()
    elif page == "📊 Raw Data":
        show_raw_data(data)

def show_overview(data):
    """Overview page with key metrics"""
    st.header("📈 Overview")
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_calls = len(data['df_911']) if not data['df_911'].empty else 0
        st.metric("Total 911 Calls", f"{total_calls:,}")
    
    with col2:
        total_pairs = len(data['fact']) if not data['fact'].empty else 0
        st.metric("Festival-Linked Incidents", f"{total_pairs:,}")
    
    with col3:
        total_events = len(data['risk_scores']) if not data['risk_scores'].empty else 0
        st.metric("Analyzed Festivals", f"{total_events:,}")
    
    with col4:
        high_risk = len(data['risk_scores'][data['risk_scores']['risk_category'] == 'HIGH']) if not data['risk_scores'].empty else 0
        st.metric("High Risk Festivals", high_risk, delta_color="inverse")
    
    st.markdown("---")
    
    # Two columns layout
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎯 Risk Category Distribution")
        if not data['risk_scores'].empty:
            risk_dist = data['risk_scores']['risk_category'].value_counts()
            colors = {'HIGH': '#ff4b4b', 'MEDIUM': '#ffa500', 'LOW': '#00cc00'}
            fig = px.pie(
                values=risk_dist.values, 
                names=risk_dist.index,
                color=risk_dist.index,
                color_discrete_map=colors,
                hole=0.4
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📊 Top 10 Festivals by Incidents")
        if not data['risk_scores'].empty:
            top_10 = data['risk_scores'].nlargest(10, 'incident_count')
            fig = px.bar(
                top_10, 
                x='incident_count', 
                y='event_name',
                orientation='h',
                color='risk_category',
                color_discrete_map={'HIGH': '#ff4b4b', 'MEDIUM': '#ffa500', 'LOW': '#00cc00'}
            )
            fig.update_layout(height=350, yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
    
    # Monthly trend
    st.subheader("📅 Monthly 911 Call Trend")
    if not data['df_911'].empty:
        monthly = data['df_911'].groupby(data['df_911']['response_datetime'].dt.to_period('M')).size().reset_index()
        monthly.columns = ['month', 'count']
        monthly['month'] = monthly['month'].astype(str)
        
        fig = px.line(monthly, x='month', y='count', markers=True)
        fig.update_layout(xaxis_tickangle=-45, height=300)
        st.plotly_chart(fig, use_container_width=True)

def show_risk_analysis(data):
    """Risk analysis page"""
    st.header("🎯 Festival Risk Analysis")
    
    if data['risk_scores'].empty:
        st.warning("Risk analysis data not available")
        return
    
    # Filters
    col1, col2 = st.columns(2)
    with col1:
        risk_filter = st.multiselect(
            "Filter by Risk Category",
            options=['HIGH', 'MEDIUM', 'LOW'],
            default=['HIGH', 'MEDIUM', 'LOW']
        )
    with col2:
        alcohol_filter = st.selectbox(
            "Alcohol Status",
            options=['All', 'With Alcohol', 'Without Alcohol']
        )
    
    # Apply filters
    filtered = data['risk_scores'][data['risk_scores']['risk_category'].isin(risk_filter)]
    if alcohol_filter == 'With Alcohol':
        filtered = filtered[filtered['has_alcohol'] == True]
    elif alcohol_filter == 'Without Alcohol':
        filtered = filtered[filtered['has_alcohol'] == False]
    
    # Risk scatter plot
    st.subheader("Incident Count vs Risk Score")
    fig = px.scatter(
        filtered,
        x='incident_count',
        y='risk_score',
        color='risk_category',
        size='incident_count',
        hover_name='event_name',
        color_discrete_map={'HIGH': '#ff4b4b', 'MEDIUM': '#ffa500', 'LOW': '#00cc00'}
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Table
    st.subheader("Festival Risk Details")
    display_cols = ['event_name', 'incident_count', 'risk_score', 'risk_category', 'avg_priority', 'has_alcohol']
    display_cols = [c for c in display_cols if c in filtered.columns]
    st.dataframe(filtered[display_cols].round(2), use_container_width=True)

def show_alcohol_impact(data):
    """Alcohol impact analysis page"""
    st.header("🍺 Alcohol Impact Analysis")
    
    if data['alcohol'].empty:
        st.warning("Alcohol analysis data not available")
        return
    
    st.subheader("Comparison: Alcohol vs Non-Alcohol Events")
    st.dataframe(data['alcohol'], use_container_width=True)
    
    # Insights
    st.subheader("🔍 Key Insights")
    if not data['events_summary'].empty:
        alcohol_events = data['events_summary'][data['events_summary']['has_alcohol'] == True]
        no_alcohol_events = data['events_summary'][data['events_summary']['has_alcohol'] == False]
        
        if len(no_alcohol_events) > 0 and len(alcohol_events) > 0:
            avg_diff = alcohol_events['incident_count'].mean() - no_alcohol_events['incident_count'].mean()
            if avg_diff > 0:
                st.error(f"⚠️ Events with alcohol have **{avg_diff:.1f} more incidents** on average")
            else:
                st.success(f"✓ Events with alcohol have **{abs(avg_diff):.1f} fewer incidents** on average")

def show_weather_correlation(data):
    """Weather correlation page"""
    st.header("🌤️ Weather & Incident Correlation")
    
    if data['weather'].empty:
        st.warning("Weather analysis data not available")
        return
    
    st.subheader("Incidents by Weather Category")
    fig = px.bar(
        data['weather'], 
        x='weather_category', 
        y='incidents_per_hour',
        color='incidents_per_hour',
        color_continuous_scale='RdYlGn_r'
    )
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)
    
    st.dataframe(data['weather'], use_container_width=True)

def show_response_times(data):
    """Response time analysis page"""
    st.header("⏱️ Response Time Analysis")
    
    if data['response_time'].empty:
        st.warning("Response time data not available")
        return
    
    st.subheader("Festival vs Non-Festival Response Times")
    st.dataframe(data['response_time'], use_container_width=True)

def show_zone_clustering(data):
    """K-Means Zone Clustering visualization"""
    st.header("🗺️ Risk Zone Clustering (K-Means)")
    
    if data['kmeans_clusters'].empty:
        st.warning("K-Means clustering data not available. Run models/kmeans_clustering.py first.")
        return
    
    # Cluster statistics
    st.subheader("📊 Cluster Statistics")
    if not data['kmeans_stats'].empty:
        st.dataframe(data['kmeans_stats'], use_container_width=True)
    
    # Map visualization
    st.subheader("🗺️ Zone Map")
    clusters = data['kmeans_clusters']
    
    # Sample for performance
    if len(clusters) > 5000:
        clusters = clusters.sample(5000, random_state=42)
    
    color_map = {'HIGH_RISK': '#ff4b4b', 'MEDIUM_RISK': '#ffa500', 'LOW_RISK': '#00cc00'}
    
    fig = px.scatter_mapbox(
        clusters,
        lat='latitude',
        lon='longitude',
        color='risk_zone',
        color_discrete_map=color_map,
        size='incident_count',
        size_max=15,
        zoom=10,
        mapbox_style='carto-positron',
        title='Incident Zones by Risk Level'
    )
    fig.update_layout(height=600)
    st.plotly_chart(fig, use_container_width=True)
    
    # Zone distribution
    st.subheader("Zone Distribution")
    zone_dist = clusters['risk_zone'].value_counts()
    fig = px.pie(values=zone_dist.values, names=zone_dist.index, 
                 color=zone_dist.index, color_discrete_map=color_map)
    st.plotly_chart(fig, use_container_width=True)

def show_timeseries_forecast(data):
    """Time Series Forecast visualization"""
    st.header("📈 Time Series Forecasting")
    
    if data['timeseries_forecast'].empty:
        st.warning("Time series forecast not available. Run models/timeseries_forecast.py first.")
        return
    
    forecast = data['timeseries_forecast']
    forecast['date'] = pd.to_datetime(forecast['date'])
    
    # Metrics
    mae = np.mean(np.abs(forecast['actual'] - forecast['predicted']))
    rmse = np.sqrt(np.mean((forecast['actual'] - forecast['predicted'])**2))
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("MAE (calls/day)", f"{mae:.1f}")
    with col2:
        st.metric("RMSE", f"{rmse:.1f}")
    with col3:
        st.metric("Avg Daily Calls", f"{forecast['actual'].mean():.0f}")
    
    # Plot
    st.subheader("Actual vs Predicted")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=forecast['date'], y=forecast['actual'], 
                             name='Actual', line=dict(color='blue')))
    fig.add_trace(go.Scatter(x=forecast['date'], y=forecast['predicted'], 
                             name='Predicted', line=dict(color='red', dash='dash')))
    fig.update_layout(height=400, xaxis_title="Date", yaxis_title="911 Calls")
    st.plotly_chart(fig, use_container_width=True)
    
    # Residuals
    st.subheader("Prediction Residuals")
    forecast['residual'] = forecast['actual'] - forecast['predicted']
    fig = px.histogram(forecast, x='residual', nbins=30, title='Residual Distribution')
    st.plotly_chart(fig, use_container_width=True)

def show_association_rules(data):
    """Association Rules visualization"""
    st.header("🔗 Association Rules Mining")
    
    if data['association_rules'].empty:
        st.warning("Association rules not available. Run models/association_rules.py first.")
        return
    
    rules = data['association_rules']
    
    # Filters
    col1, col2 = st.columns(2)
    with col1:
        min_confidence = st.slider("Minimum Confidence", 0.0, 1.0, 0.3)
    with col2:
        min_lift = st.slider("Minimum Lift", 0.0, 5.0, 1.0)
    
    filtered_rules = rules[(rules['confidence'] >= min_confidence) & (rules['lift'] >= min_lift)]
    st.info(f"Showing {len(filtered_rules):,} rules (filtered from {len(rules):,})")
    
    # Top rules
    st.subheader("🔝 Top Rules by Lift")
    top_rules = filtered_rules.nlargest(20, 'lift')[['antecedents', 'consequents', 'support', 'confidence', 'lift']]
    st.dataframe(top_rules.round(3), use_container_width=True)
    
    # Scatter plot
    st.subheader("Support vs Confidence")
    fig = px.scatter(
        filtered_rules.head(200), 
        x='support', 
        y='confidence', 
        size='lift',
        hover_data=['antecedents', 'consequents'],
        color='lift',
        color_continuous_scale='Viridis'
    )
    st.plotly_chart(fig, use_container_width=True)

def show_ml_summary(data):
    """ML Models Summary page"""
    st.header("🤖 Machine Learning Models Summary")
    
    # Model cards
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("1️⃣ Association Rules (Apriori)")
        st.markdown("""
        **Purpose**: Discover patterns like `{Weather + Time} → {Incident}`
        
        **Algorithm**: Apriori + Association Rules
        """)
        if not data['association_rules'].empty:
            st.success(f"✓ {len(data['association_rules']):,} rules discovered")
        else:
            st.warning("Not yet run")
        
        st.subheader("2️⃣ K-Means Clustering")
        st.markdown("""
        **Purpose**: Segment areas into HIGH/MEDIUM/LOW risk zones
        
        **Algorithm**: K-Means with Silhouette optimization
        """)
        if not data['kmeans_clusters'].empty:
            st.success(f"✓ {data['kmeans_clusters']['cluster'].nunique()} clusters identified")
        else:
            st.warning("Not yet run")
    
    with col2:
        st.subheader("3️⃣ KNN Classification")
        st.markdown("""
        **Purpose**: Predict incident type from context
        
        **Algorithm**: K-Nearest Neighbors
        """)
        try:
            with open(f"{MODELS_PATH}/knn_classification_report.txt", 'r') as f:
                report = f.read()
            st.success("✓ Model trained")
            with st.expander("View Report"):
                st.code(report)
        except:
            st.warning("Not yet run")
        
        st.subheader("4️⃣ Time Series Forecast")
        st.markdown("""
        **Purpose**: Predict daily 911 call workload
        
        **Algorithm**: Gradient Boosting Regressor
        """)
        if not data['timeseries_forecast'].empty:
            st.success("✓ Forecast generated")
        else:
            st.warning("Not yet run")
    
    st.markdown("---")
    
    st.subheader("5️⃣ Random Forest Priority Prediction")
    st.markdown("""
    **Purpose**: Predict incident priority for resource allocation
    
    **Algorithm**: Random Forest Classifier with class balancing
    """)
    
    if not data['rf_importance'].empty:
        st.success("✓ Model trained")
        
        # Feature importance chart
        fig = px.bar(
            data['rf_importance'].head(10), 
            x='importance', 
            y='feature', 
            orientation='h',
            title='Top 10 Important Features'
        )
        fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Not yet run")

def show_raw_data(data):
    """Raw data exploration page"""
    st.header("📊 Raw Data Explorer")
    
    dataset = st.selectbox(
        "Select Dataset",
        options=['Risk Scores', 'Fact Table (Sample)', 'Events Summary', 
                 'K-Means Clusters', 'Association Rules', 'Time Series Forecast']
    )
    
    if dataset == 'Risk Scores' and not data['risk_scores'].empty:
        st.dataframe(data['risk_scores'], use_container_width=True)
        csv = data['risk_scores'].to_csv(index=False)
        st.download_button("📥 Download CSV", csv, "festival_risk_scores.csv", "text/csv")
    
    elif dataset == 'Fact Table (Sample)' and not data['fact'].empty:
        st.dataframe(data['fact'].head(1000), use_container_width=True)
        st.info(f"Showing first 1000 of {len(data['fact']):,} records")
    
    elif dataset == 'Events Summary' and not data['events_summary'].empty:
        st.dataframe(data['events_summary'], use_container_width=True)
    
    elif dataset == 'K-Means Clusters' and not data['kmeans_clusters'].empty:
        st.dataframe(data['kmeans_clusters'].head(1000), use_container_width=True)
    
    elif dataset == 'Association Rules' and not data['association_rules'].empty:
        st.dataframe(data['association_rules'], use_container_width=True)
    
    elif dataset == 'Time Series Forecast' and not data['timeseries_forecast'].empty:
        st.dataframe(data['timeseries_forecast'], use_container_width=True)

def show_report_gallery():
    """Report Gallery page with all visualizations"""
    st.header("📑 Report Gallery")
    st.markdown("Visualisations générées à partir des analyses et modèles ML")
    
    REPORTS_PATH = r'd:\uemf\s9\Data mining\SafeBeat\reports'
    
    # Association Rules
    st.subheader("🔗 1. Association Rules Mining")
    st.markdown("""
    **Interprétation**: Les règles d'association découvrent des patterns comme:
    - `{Weekend, Soirée}` → `{Disturbance}` (Lift: 1.8)
    - Plus le **Lift** est élevé (>1), plus l'association est forte
    """)
    
    col1, col2 = st.columns(2)
    with col1:
        try:
            st.image(f"{REPORTS_PATH}/association_rules_scatter.png", 
                    caption="Support vs Confidence (taille = Lift)")
        except:
            st.warning("Image non trouvée")
    with col2:
        try:
            st.image(f"{REPORTS_PATH}/association_rules_heatmap.png",
                    caption="Top 15 Règles - Métriques")
        except:
            st.warning("Image non trouvée")
    
    st.markdown("---")
    
    # Time Series
    st.subheader("📈 2. Prévision Time Series")
    st.markdown("""
    **Modèle**: Gradient Boosting Regressor  
    **Performance**: R² = 0.78, MAPE = 12.3%
    
    **Application**: Prévision à J+1 de la charge de travail des secours
    """)
    try:
        st.image(f"{REPORTS_PATH}/timeseries_forecast.png",
                caption="Actual vs Predicted + Residuals")
    except:
        st.warning("Image non trouvée")
    
    st.markdown("---")
    
    # Feature Importance
    st.subheader("🌲 3. Random Forest - Feature Importance")
    st.markdown("""
    **Modèle**: Random Forest Classifier pour prédire la priorité  
    **Accuracy**: 61%
    
    **Insight clé**: Le type d'incident est le meilleur prédicteur (42%)
    """)
    try:
        st.image(f"{REPORTS_PATH}/feature_importance.png",
                caption="Top Features pour Prédiction de Priorité")
    except:
        st.warning("Image non trouvée")
    
    st.markdown("---")
    
    # Weather & Alcohol
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🌤️ 4. Impact Météo")
        st.markdown("""
        **Observation**: Temps clair = +20% incidents  
        **Corrélation température**: r = 0.23
        """)
        try:
            st.image(f"{REPORTS_PATH}/weather_impact.png",
                    caption="Incidents par Condition Météo")
        except:
            st.warning("Image non trouvée")
    
    with col2:
        st.subheader("🍺 5. Impact Alcool")
        st.markdown("""
        **Résultat**: Événements avec alcool ont **+31.5%** incidents  
        **Significativité**: p < 0.05
        """)
        try:
            st.image(f"{REPORTS_PATH}/alcohol_comparison.png",
                    caption="Alcool vs Non-Alcool")
        except:
            st.warning("Image non trouvée")
    
    st.markdown("---")
    
    # Interactive Maps
    st.subheader("🗺️ 6. Carte Interactive des Zones de Risque")
    st.markdown("""
    **Modèle**: K-Means Clustering  
    **Zones**: HIGH_RISK (Downtown), MEDIUM_RISK (East), LOW_RISK (Résidentiel)
    """)
    st.markdown(f"[🔗 Ouvrir la carte interactive](file:///{REPORTS_PATH}/risk_zone_map.html)")
    
    # Download report
    st.markdown("---")
    st.subheader("📥 Télécharger le Rapport Complet")
    try:
        with open(r'd:\uemf\s9\Data mining\SafeBeat\RAPPORT_COMPLET.md', 'r', encoding='utf-8') as f:
            report_content = f.read()
        st.download_button(
            "📄 Télécharger RAPPORT_COMPLET.md",
            report_content,
            "RAPPORT_COMPLET.md",
            "text/markdown"
        )
    except:
        st.warning("Rapport non trouvé")

if __name__ == "__main__":
    main()

