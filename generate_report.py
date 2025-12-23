"""
SafeBeat - Generate Comprehensive Report
Creates an HTML report with all results, interpretations, and visualizations
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

# Paths
DATA_ANALYSIS = r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis'
DATA_ENRICHED = r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched'
MODELS_PATH = r'd:\uemf\s9\Data mining\SafeBeat\models'
OUTPUT_PATH = r'd:\uemf\s9\Data mining\SafeBeat\reports'

os.makedirs(OUTPUT_PATH, exist_ok=True)

def generate_report():
    print("=" * 60)
    print("GENERATING COMPREHENSIVE REPORT")
    print("=" * 60)
    
    # Load all data
    print("\n[1/8] Loading data...")
    
    risk_scores = pd.read_csv(f"{DATA_ANALYSIS}/festival_risk_scores.csv")
    fact = pd.read_parquet(f"{DATA_ENRICHED}/fact_festival_incidents.parquet")
    weather = pd.read_csv(f"{DATA_ANALYSIS}/incidents_by_weather.csv")
    alcohol = pd.read_csv(f"{DATA_ANALYSIS}/alcohol_impact_summary.csv")
    
    kmeans_clusters = pd.read_parquet(f"{MODELS_PATH}/kmeans_zone_clusters.parquet")
    kmeans_stats = pd.read_csv(f"{MODELS_PATH}/kmeans_cluster_stats.csv")
    association_rules = pd.read_csv(f"{MODELS_PATH}/association_rules_results.csv")
    ts_forecast = pd.read_parquet(f"{MODELS_PATH}/timeseries_forecast.parquet")
    rf_importance = pd.read_csv(f"{MODELS_PATH}/rf_feature_importance.csv")
    
    # Generate visualizations
    print("\n[2/8] Creating Association Rules Network...")
    create_association_rules_viz(association_rules)
    
    print("\n[3/8] Creating Risk Heatmap...")
    create_risk_heatmap(kmeans_clusters)
    
    print("\n[4/8] Creating Forecast Visualization...")
    create_forecast_viz(ts_forecast)
    
    print("\n[5/8] Creating Feature Importance Chart...")
    create_feature_importance_viz(rf_importance)
    
    print("\n[6/8] Creating Weather Impact Chart...")
    create_weather_viz(weather)
    
    print("\n[7/8] Creating Alcohol Comparison Chart...")
    create_alcohol_viz(alcohol)
    
    print("\n[8/8] Generating Report...")
    
    # Calculate key metrics
    total_incidents = len(fact)
    unique_events = fact['event_id'].nunique()
    high_risk_count = len(risk_scores[risk_scores['risk_category'] == 'HIGH'])
    
    # Top rules
    top_rules = association_rules.nlargest(10, 'lift')
    
    # Cluster summary
    cluster_summary = kmeans_stats
    
    # Report generation complete
    print("\n" + "=" * 60)
    print("REPORT GENERATION COMPLETE")
    print("=" * 60)
    print(f"\nVisualizations saved to: {OUTPUT_PATH}")
    
    return {
        'total_incidents': total_incidents,
        'unique_events': unique_events,
        'high_risk_count': high_risk_count,
        'top_rules': top_rules,
        'cluster_summary': cluster_summary
    }

def create_association_rules_viz(rules):
    """Create association rules visualization"""
    # Filter top rules
    top_rules = rules.nlargest(30, 'lift')
    
    # Sunburst chart for rules
    sunburst_data = []
    for _, row in top_rules.iterrows():
        sunburst_data.append({
            'antecedent': str(row['antecedents'])[:40],
            'consequent': str(row['consequents'])[:40],
            'lift': row['lift'],
            'confidence': row['confidence']
        })
    
    df_sun = pd.DataFrame(sunburst_data)
    
    # Scatter plot: Support vs Confidence colored by Lift
    fig = px.scatter(
        top_rules,
        x='support',
        y='confidence',
        size='lift',
        color='lift',
        hover_data=['antecedents', 'consequents'],
        title='Association Rules: Support vs Confidence (size=Lift)',
        color_continuous_scale='Viridis'
    )
    fig.update_layout(width=1000, height=600)
    fig.write_html(f"{OUTPUT_PATH}/association_rules_scatter.html")
    fig.write_image(f"{OUTPUT_PATH}/association_rules_scatter.png", scale=2)
    
    # Heatmap of top rules
    top_10 = rules.nlargest(15, 'lift')
    top_10['rule'] = top_10['antecedents'].str[:30] + ' → ' + top_10['consequents'].str[:20]
    
    fig2 = go.Figure(data=go.Heatmap(
        z=[top_10['confidence'].values, top_10['lift'].values, top_10['support'].values * 100],
        x=top_10['rule'].values,
        y=['Confidence', 'Lift', 'Support (%)'],
        colorscale='RdYlGn',
        text=[[f"{v:.2f}" for v in top_10['confidence'].values],
              [f"{v:.2f}" for v in top_10['lift'].values],
              [f"{v:.2f}" for v in (top_10['support'].values * 100)]],
        texttemplate="%{text}",
        textfont={"size": 10}
    ))
    fig2.update_layout(
        title='Top 15 Association Rules Metrics',
        xaxis_tickangle=-45,
        width=1200,
        height=400
    )
    fig2.write_html(f"{OUTPUT_PATH}/association_rules_heatmap.html")
    fig2.write_image(f"{OUTPUT_PATH}/association_rules_heatmap.png", scale=2)

def create_risk_heatmap(clusters):
    """Create geographic risk heatmap"""
    # Sample for performance
    if len(clusters) > 3000:
        sample = clusters.sample(3000, random_state=42)
    else:
        sample = clusters
    
    color_map = {'HIGH_RISK': '#ff4b4b', 'MEDIUM_RISK': '#ffa500', 'LOW_RISK': '#00cc00'}
    
    fig = px.scatter_mapbox(
        sample,
        lat='latitude',
        lon='longitude',
        color='risk_zone',
        color_discrete_map=color_map,
        size='incident_count',
        size_max=15,
        zoom=10,
        mapbox_style='carto-positron',
        title='Austin Risk Zones - K-Means Clustering Results'
    )
    fig.update_layout(width=1000, height=700)
    fig.write_html(f"{OUTPUT_PATH}/risk_zone_map.html")

def create_forecast_viz(forecast):
    """Create time series forecast visualization"""
    forecast['date'] = pd.to_datetime(forecast['date'])
    
    fig = make_subplots(rows=2, cols=1, 
                        subplot_titles=('Actual vs Predicted 911 Calls', 'Prediction Residuals'),
                        row_heights=[0.7, 0.3])
    
    # Actual vs Predicted
    fig.add_trace(
        go.Scatter(x=forecast['date'], y=forecast['actual'], name='Actual', 
                   line=dict(color='blue', width=2)),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=forecast['date'], y=forecast['predicted'], name='Predicted',
                   line=dict(color='red', width=2, dash='dash')),
        row=1, col=1
    )
    
    # Residuals
    residuals = forecast['actual'] - forecast['predicted']
    fig.add_trace(
        go.Bar(x=forecast['date'], y=residuals, name='Residual',
               marker_color=np.where(residuals > 0, 'green', 'red')),
        row=2, col=1
    )
    
    fig.update_layout(height=700, width=1000, title_text="Time Series Forecast Results")
    fig.write_html(f"{OUTPUT_PATH}/timeseries_forecast.html")
    fig.write_image(f"{OUTPUT_PATH}/timeseries_forecast.png", scale=2)

def create_feature_importance_viz(importance):
    """Create feature importance visualization"""
    fig = px.bar(
        importance.head(15),
        x='importance',
        y='feature',
        orientation='h',
        title='Random Forest - Feature Importance for Priority Prediction',
        color='importance',
        color_continuous_scale='Blues'
    )
    fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=500, width=800)
    fig.write_html(f"{OUTPUT_PATH}/feature_importance.html")
    fig.write_image(f"{OUTPUT_PATH}/feature_importance.png", scale=2)

def create_weather_viz(weather):
    """Create weather impact visualization"""
    fig = px.bar(
        weather,
        x='weather_category',
        y='incidents_per_hour',
        color='incidents_per_hour',
        title='Incidents per Hour by Weather Category',
        color_continuous_scale='RdYlGn_r',
        text='incidents_per_hour'
    )
    fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
    fig.update_layout(xaxis_tickangle=-45, height=500, width=800)
    fig.write_html(f"{OUTPUT_PATH}/weather_impact.html")
    fig.write_image(f"{OUTPUT_PATH}/weather_impact.png", scale=2)

def create_alcohol_viz(alcohol):
    """Create alcohol impact visualization"""
    fig = go.Figure()
    
    metrics = alcohol['metric'].tolist()
    with_alcohol = alcohol['with_alcohol'].tolist()
    without_alcohol = alcohol['without_alcohol'].tolist()
    
    fig.add_trace(go.Bar(name='With Alcohol', x=metrics, y=with_alcohol, marker_color='#ff6b6b'))
    fig.add_trace(go.Bar(name='Without Alcohol', x=metrics, y=without_alcohol, marker_color='#4ecdc4'))
    
    fig.update_layout(
        barmode='group',
        title='Alcohol vs Non-Alcohol Events Comparison',
        height=500,
        width=900
    )
    fig.write_html(f"{OUTPUT_PATH}/alcohol_comparison.html")
    fig.write_image(f"{OUTPUT_PATH}/alcohol_comparison.png", scale=2)

if __name__ == "__main__":
    results = generate_report()
    print("\n✓ All visualizations generated!")
