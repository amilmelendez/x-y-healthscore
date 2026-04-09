import pandas as pd
import json
import os
import glob

def process():
    # 1. Find the latest files in the /data folder
    health_files = glob.glob('data/*Daily Node Health.csv')
    scoring_files = glob.glob('data/*Node_Scoring*.csv')
    
    if not health_files or not scoring_files:
        print("Missing CSV files in /data folder")
        return

    # Use the most recent files based on filename/date
    latest_health = max(health_files)
    latest_scoring = max(scoring_files)

    # 2. Load Data
    df_h = pd.read_csv(latest_health)
    df_s = pd.read_csv(latest_scoring)

    # 3. Process Nodes (Merging relevant columns)
    # We match by 'Node' to combine NRBY scores with technical KPIs
    nodes_list = []
    for _, row in df_s.iterrows():
        node_id = row['Node']
        # Find matching health data
        h_row = df_h[df_h['Node'] == node_id]
        
        nid_val = row['NID Score (x100)']
        qoe_val = row['QOE']
        
        # Determine buckets
        def get_bucket(val, is_qoe=False):
            if is_qoe:
                return 'red' if val < 60 else 'orange' if val < 70 else 'yellow' if val < 85 else 'green'
            return 'red' if val < 15 else 'orange' if val < 30 else 'yellow' if val < 50 else 'green'

        node_data = {
            "node": node_id,
            "region": row['Region'],
            "facility": row['Facility'],
            "nid": round(nid_val, 1),
            "qoe": round(qoe_val, 1),
            "impacted": int(h_row['Impacted'].values[0]) if not h_row.empty else 0,
            "total": int(h_row['Total'].values[0]) if not h_row.empty else 0,
            "tc_30d": int(row['TC 30d']),
            "snr": round(row['SNR Score'], 2),
            "ccr": round(row['CCR Score'], 2),
            "nid_bucket": get_bucket(nid_val),
            "qoe_bucket": get_bucket(qoe_val, True)
        }
        nodes_list.append(node_data)

    # 4. Prepare final JSON structure for the Dashboard
    # This mimics the ADV object your HTML expects
    output = {
        "last_update": pd.Timestamp.now().strftime('%Y-%m-%d'),
        "nodes": nodes_list,
        "div_nid_good": [n for n in nodes_list if n['nid_bucket'] == 'green' and n['qoe_bucket'] in ['red', 'orange']],
        "div_qoe_good": [n for n in nodes_list if n['qoe_bucket'] == 'green' and n['nid_bucket'] in ['red', 'orange']],
        # For chronic, we can eventually track history across multiple files
        "chronic": [n for n in nodes_list if n['nid_bucket'] == 'red' and n['tc_30d'] > 10] 
    }

    # 5. Save as data.json
    with open('data.json', 'w') as f:
        json.dump(output, f, indent=2)
    print("Successfully generated data.json")

if __name__ == "__main__":
    process()
