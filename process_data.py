import pandas as pd
import json
import os
import glob

def process():
    # 1. Load History (if it exists)
    history = {"trend_dates": [], "trend_nid_avg": [], "trend_qoe_avg": []}
    if os.path.exists('data.json'):
        with open('data.json', 'r') as f:
            old_data = json.load(f)
            history["trend_dates"] = old_data.get("trend_dates", [])
            history["trend_nid_avg"] = old_data.get("trend_nid_avg", [])
            history["trend_qoe_avg"] = old_data.get("trend_qoe_avg", [])

    # 2. Find latest files
    health_files = glob.glob('data/*Daily Node Health.csv')
    scoring_files = glob.glob('data/*Node_Scoring*.csv')
    
    if not health_files or not scoring_files: return

    latest_h = max(health_files)
    latest_s = max(scoring_files)
    df_h = pd.read_csv(latest_h)
    df_s = pd.read_csv(latest_s)

    # 3. Process Today's Snapshot
    nodes_list = []
    total_nid = 0
    total_qoe = 0
    count = 0

    for _, row in df_s.iterrows():
        node_id = row['Node']
        h_row = df_h[df_h['Node'] == node_id]
        nid_val = row['NID Score (x100)']
        qoe_val = row['QOE']
        
        # Keep track of averages for the chart
        total_nid += nid_val
        total_qoe += qoe_val
        count += 1

        nodes_list.append({
            "node": node_id,
            "region": row['Region'],
            "nid": round(nid_val, 1),
            "qoe": round(qoe_val, 1),
            "impacted": int(h_row['Impacted'].values[0]) if not h_row.empty else 0,
            "total": int(h_row['Total'].values[0]) if not h_row.empty else 0,
            "nid_bucket": 'red' if nid_val < 15 else 'orange' if nid_val < 30 else 'green'
        })

    # 4. Update the Trends (Don't duplicate the same day)
    today_str = pd.Timestamp.now().strftime('%Y-%m-%d')
    if today_str not in history["trend_dates"]:
        history["trend_dates"].append(today_str)
        history["trend_nid_avg"].append(round(total_nid / count, 1))
        history["trend_qoe_avg"].append(round(total_qoe / count, 1))

    # 5. Final Data Save
    output = {
        "last_update": today_str,
        "trend_dates": history["trend_dates"],
        "trend_nid_avg": history["trend_nid_avg"],
        "trend_qoe_avg": history["trend_qoe_avg"],
        "nodes": nodes_list
    }

    with open('data.json', 'w') as f:
        json.dump(output, f, indent=2)

if __name__ == "__main__":
    process()
