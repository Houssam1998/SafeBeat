"""
SafeBeat ML - Association Rules Mining
Identifies aggravating factors leading to specific incidents

Algorithm: Apriori + Association Rules
Use case: {Weather, Event_Type, Time} → {Incident_Type}

Output: models/association_rules_results.csv
"""
import sys
sys.path.insert(0, 'D:\\python_packages')

import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder

def prepare_transactions(df):
    """Convert incident data to transaction format for Apriori"""
    transactions = []
    
    for _, row in df.iterrows():
        transaction = []
        
        # Weather conditions
        if pd.notna(row.get('weather_category')):
            transaction.append(f"Weather_{row['weather_category']}")
        
        # Temperature category
        if pd.notna(row.get('temp_category')):
            transaction.append(f"Temp_{row['temp_category']}")
        
        # Time attributes
        if pd.notna(row.get('response_hour')):
            hour = row['response_hour']
            if hour < 6:
                transaction.append("Time_Night")
            elif hour < 12:
                transaction.append("Time_Morning")
            elif hour < 18:
                transaction.append("Time_Afternoon")
            else:
                transaction.append("Time_Evening")
        
        # Day of week
        if pd.notna(row.get('response_day_of_week')):
            dow = row['response_day_of_week']
            if dow in ['Sat', 'Sun']:
                transaction.append("Weekend")
            else:
                transaction.append("Weekday")
        
        # Priority level
        if pd.notna(row.get('priority_level')):
            transaction.append(f"Priority_{row['priority_level'].replace(' ', '')}")
        
        # Incident type (target)
        if pd.notna(row.get('final_problem_category')):
            # Simplify category
            category = str(row['final_problem_category']).split()[0][:20]
            transaction.append(f"Incident_{category}")
        
        # Festival context (if available)
        if row.get('is_festival_related', False):
            transaction.append("Festival_Related")
        
        # Alcohol (if from fact table)
        if row.get('event_has_alcohol', False):
            transaction.append("Alcohol_Event")
        
        if len(transaction) >= 2:
            transactions.append(transaction)
    
    return transactions

def mine_association_rules():
    print("=" * 60)
    print("ASSOCIATION RULES MINING")
    print("=" * 60)
    
    # 1. Load data
    print("\n[1/5] Loading data...")
    
    # Try to load weather-enriched 911 data
    try:
        df = pd.read_parquet(
            r'd:\uemf\s9\Data mining\SafeBeat\datasets\analysis\weather_incident_data.parquet'
        )
        print(f"   Loaded weather-incident data: {len(df):,} records")
    except:
        # Fallback to fact table
        df = pd.read_parquet(
            r'd:\uemf\s9\Data mining\SafeBeat\datasets\enriched\fact_festival_incidents.parquet'
        )
        print(f"   Loaded fact table: {len(df):,} records")
    
    # Sample if too large
    if len(df) > 50000:
        print(f"   Sampling to 50,000 for performance...")
        df = df.sample(n=50000, random_state=42)
    
    # 2. Prepare transactions
    print("\n[2/5] Preparing transactions...")
    transactions = prepare_transactions(df)
    print(f"   Generated {len(transactions):,} transactions")
    
    # Show sample transaction
    print(f"   Sample transaction: {transactions[0]}")
    
    # 3. One-hot encode
    print("\n[3/5] One-hot encoding transactions...")
    te = TransactionEncoder()
    te_ary = te.fit_transform(transactions)
    df_encoded = pd.DataFrame(te_ary, columns=te.columns_)
    print(f"   Encoded shape: {df_encoded.shape}")
    
    # 4. Find frequent itemsets
    print("\n[4/5] Mining frequent itemsets (Apriori)...")
    frequent_itemsets = apriori(df_encoded, min_support=0.01, use_colnames=True)
    print(f"   Found {len(frequent_itemsets):,} frequent itemsets")
    
    if len(frequent_itemsets) == 0:
        print("   ⚠ No frequent itemsets found. Trying lower support...")
        frequent_itemsets = apriori(df_encoded, min_support=0.005, use_colnames=True)
        print(f"   Found {len(frequent_itemsets):,} frequent itemsets")
    
    # 5. Generate association rules
    print("\n[5/5] Generating association rules...")
    
    if len(frequent_itemsets) > 0:
        rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=0.3)
        rules = rules.sort_values('lift', ascending=False)
        print(f"   Generated {len(rules):,} rules")
        
        # Filter to interesting rules (incident as consequent)
        incident_rules = rules[
            rules['consequents'].apply(lambda x: any('Incident_' in str(i) for i in x))
        ]
        print(f"   Rules with incident as consequence: {len(incident_rules):,}")
    else:
        print("   ⚠ Could not generate rules")
        rules = pd.DataFrame()
        incident_rules = pd.DataFrame()
    
    # Save results
    import os
    os.makedirs(r'd:\uemf\s9\Data mining\SafeBeat\models', exist_ok=True)
    
    if not rules.empty:
        # Convert frozensets to strings for saving
        rules_save = rules.copy()
        rules_save['antecedents'] = rules_save['antecedents'].apply(lambda x: ', '.join(list(x)))
        rules_save['consequents'] = rules_save['consequents'].apply(lambda x: ', '.join(list(x)))
        
        rules_save.to_csv(
            r'd:\uemf\s9\Data mining\SafeBeat\models\association_rules_results.csv',
            index=False
        )
    
    # =========================================
    # RESULTS
    # =========================================
    print("\n" + "=" * 60)
    print("ASSOCIATION RULES RESULTS")
    print("=" * 60)
    
    if not incident_rules.empty:
        print("\n🔍 TOP 15 RULES (Incident as Consequence):")
        print("-" * 60)
        
        for idx, row in incident_rules.head(15).iterrows():
            antecedents = ', '.join(list(row['antecedents']))
            consequents = ', '.join(list(row['consequents']))
            print(f"\n{antecedents}")
            print(f"  → {consequents}")
            print(f"  Support: {row['support']:.3f} | Confidence: {row['confidence']:.3f} | Lift: {row['lift']:.3f}")
    
    # Key insights
    print("\n" + "=" * 60)
    print("KEY PATTERNS DISCOVERED")
    print("=" * 60)
    
    if not rules.empty:
        # Weather patterns
        weather_rules = rules[rules['antecedents'].apply(lambda x: any('Weather_' in str(i) for i in x))]
        if not weather_rules.empty:
            top_weather = weather_rules.nlargest(3, 'lift')
            print("\n🌤️ Weather-related patterns:")
            for _, row in top_weather.iterrows():
                print(f"   {set(row['antecedents'])} → {set(row['consequents'])} (Lift: {row['lift']:.2f})")
        
        # Time patterns
        time_rules = rules[rules['antecedents'].apply(lambda x: any('Time_' in str(i) for i in x))]
        if not time_rules.empty:
            top_time = time_rules.nlargest(3, 'lift')
            print("\n⏰ Time-related patterns:")
            for _, row in top_time.iterrows():
                print(f"   {set(row['antecedents'])} → {set(row['consequents'])} (Lift: {row['lift']:.2f})")
    
    return rules

if __name__ == "__main__":
    rules = mine_association_rules()
    print("\n" + "=" * 60)
    print("ASSOCIATION RULES MINING COMPLETE!")
    print("=" * 60)
