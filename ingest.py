#!/usr/bin/env python3
"""
Node Health Dashboard - Daily Data Ingestion Script
====================================================
Reads new NID (.xlsx) and QoE (.csv) files from the inbox/ folder,
appends today's data into data.js, and regenerates TECH + ADV.
 
File naming convention:
  NID:  Node_Scoring_YYYYMMDD.xlsx   (date parsed from filename)
  QoE:  YYYY-MM-DD_-_Daily_Node_Health.csv  (date parsed from filename)
"""
 
import re
import os
import sys
import json
import glob
import argparse
import pandas as pd
from datetime import datetime, date
 
 
# ── Bucket helpers ────────────────────────────────────────────────────────────
 
def nid_bucket(score):
    if score is None or pd.isna(score): return "green"
    if score <= 15:  return "red"
    if score <= 30:  return "orange"
    if score <= 49:  return "yellow"
    return "green"
 
def qoe_bucket(score):
    if score is None or pd.isna(score): return "green"
    if score <= 59:  return "red"
    if score <= 74:  return "orange"
    if score <= 84:  return "yellow"
    return "green"
 
 
# ── Date parsing from filename ─────────────────────────────────────────────────
 
def date_from_nid_filename(path):
    """Node_Scoring_20260409.xlsx → '2026-04-09'"""
    name = os.path.basename(path)
    m = re.search(r'(\d{8})', name)
    if not m:
        raise ValueError(f"Cannot parse date from NID filename: {name}")
    return datetime.strptime(m.group(1), "%Y%m%d").strftime("%Y-%m-%d")
 
def date_from_qoe_filename(path):
    """2026-04-09_-_Daily_Node_Health.csv → '2026-04-09'"""
    name = os.path.basename(path)
    m = re.search(r'(\d{4}-\d{2}-\d{2})', name)
    if not m:
        raise ValueError(f"Cannot parse date from QoE filename: {name}")
    return m.group(1)
 
 
# ── File discovery ─────────────────────────────────────────────────────────────
 
def find_inbox_files(inbox_dir="inbox"):
    nid_files = sorted(glob.glob(os.path.join(inbox_dir, "Node_Scoring_*.xlsx")))
    qoe_files = sorted(glob.glob(os.path.join(inbox_dir, "*.csv")))
    return nid_files, qoe_files
 
 
# ── Read source files ─────────────────────────────────────────────────────────
 
def read_nid(path):
    df = pd.read_excel(path, sheet_name="pivot")
    df["Date"] = df["Date"].astype(str).str[:10]  # normalize to YYYY-MM-DD
    # New file format stores NID Score as 0-1 decimal; use NID Score (x100) if available
    if "NID Score (x100)" in df.columns:
        df["NID Score"] = df["NID Score (x100)"]
    elif df["NID Score"].max() <= 1.0:
        df["NID Score"] = df["NID Score"] * 100
    return df
 
def read_qoe(path):
    df = pd.read_csv(path)
    date_str = date_from_qoe_filename(path)
    df["Date"] = date_str
    return df
 
 
# ── Build today's NID summary row ─────────────────────────────────────────────
 
def build_nid_summary_row(df_nid, date_str):
    scores = df_nid["NID Score"].dropna()
    total  = len(scores)
    red    = int((scores <= 15).sum())
    orange = int(((scores > 15) & (scores <= 30)).sum())
    yellow = int(((scores > 30) & (scores <= 49)).sum())
    green  = int((scores >= 50).sum())
    def pct(n): return round(n / total * 100, 2) if total else 0
    return {
        "date": date_str, "total": total,
        "red": red,    "red_pct":    pct(red),
        "orange": orange, "orange_pct": pct(orange),
        "yellow": yellow, "yellow_pct": pct(yellow),
        "green": green,  "green_pct":  pct(green),
    }
 
 
# ── Build today's QoE summary row ─────────────────────────────────────────────
 
def build_qoe_summary_row(df_qoe, date_str):
    scores = df_qoe["Score"].dropna()
    total  = len(scores)
    red    = int((scores <= 59).sum())
    orange = int(((scores > 59) & (scores <= 74)).sum())
    yellow = int(((scores > 74) & (scores <= 84)).sum())
    green  = int((scores >= 85).sum())
    def pct(n): return round(n / total * 100, 2) if total else 0
    return {
        "date": date_str, "total": total,
        "red": red,    "red_pct":    pct(red),
        "orange": orange, "orange_pct": pct(orange),
        "yellow": yellow, "yellow_pct": pct(yellow),
        "green": green,  "green_pct":  pct(green),
    }
 
 
# ── Build today's NID red nodes list ──────────────────────────────────────────
 
def build_nid_red_nodes(df_nid, df_qoe, date_str):
    red_df = df_nid[df_nid["NID Score"] <= 15].copy()
    # join QoE score if available
    qoe_map = dict(zip(df_qoe["Node"], df_qoe["Score"])) if df_qoe is not None else {}
    result = []
    for _, row in red_df.iterrows():
        node = row["Node"]
        result.append({
            "node":     node,
            "region":   row.get("Region", ""),
            "facility": row.get("Facility", ""),
            "nid":      round(float(row["NID Score"]), 1),
            "qoe":      float(qoe_map.get(node, None)) if qoe_map.get(node) is not None else None,
        })
    return result
 
 
# ── Build today's QoE red nodes list ──────────────────────────────────────────
 
def build_qoe_red_nodes(df_qoe, date_str):
    red_df = df_qoe[df_qoe["Score"] <= 59].copy()
    result = []
    for _, row in red_df.iterrows():
        result.append({
            "node": row["Node"],
            "qoe":  float(row["Score"]),
        })
    return result
 
 
# ── Update node_scores dicts (append new date column per node) ────────────────
 
def update_node_scores(existing_dict, df, node_col, score_col, date_str):
    """Appends date → score for every node in df into the existing dict."""
    updated = dict(existing_dict)  # shallow copy
    for _, row in df.iterrows():
        node  = str(row[node_col])
        score = row[score_col]
        if pd.isna(score):
            continue
        if node not in updated:
            updated[node] = {}
        updated[node][date_str] = round(float(score), 1)
    return updated
 
 
# ── Build TECH (latest snapshot per node) ─────────────────────────────────────
 
def build_tech(df_nid, df_qoe):
    qoe_map = {}
    if df_qoe is not None:
        for _, row in df_qoe.iterrows():
            qoe_map[str(row["Node"])] = {
                "score":    float(row["Score"]),
                "impacted": float(row.get("Impacted", 0) or 0),
                "stressed": float(row.get("Stressed", 0) or 0),
                "total":    float(row.get("Total", 0) or 0),
            }
 
    tech = []
    for _, row in df_nid.iterrows():
        node = str(row["Node"])
        nid  = row.get("NID Score")
        q    = qoe_map.get(node, {})
        qoe_score = q.get("score")
        total    = q.get("total", 0)
        impacted = q.get("impacted", 0)
        stressed = q.get("stressed", 0)
        offline  = max(0, total - impacted - stressed)
 
        def safe(col):
            v = row.get(col)
            return None if v is None or (isinstance(v, float) and pd.isna(v)) else v
 
        tech.append({
            "Node":    node,
            "Region":  safe("Region") or "",
            "Facility": safe("Facility") or "",
            "NID Score": round(float(nid), 2) if pd.notna(nid) else None,
            "NID Score Delta": safe("NID Score Delta"),
            "TC 30d":  safe("TC 30d"),
            "TC 7d":   safe("TC 7d"),
            "# CPE (Click for Node Detail)": safe("# CPE (Click for Node Detail)"),
            "# 3.1 CPE": safe("# 3.1 CPE"),
            "DS Score":  safe("DS Score"),
            "CER Score": safe("CER Score"),
            "T3 Score":  safe("T3 Score"),
            "T4 Score":  safe("T4 Score"),
            "CCR Score": safe("CCR Score"),
            "SNR Score": safe("SNR Score"),
            "PWR Lo-Hi Score": safe("PWR Lo-Hi Score"),
            "PWR VAR Score":   safe("PWR VAR Score"),
            "SUCKOUT": safe("SUCKOUT"),
            "WAVE":    safe("WAVE"),
            "dPWR Lo": safe("dPWR Lo"),
            "dPWR Hi": safe("dPWR Hi"),
            "dCCR": safe("dCCR"),
            "dCER": safe("dCER"),
            "dSNR": safe("dSNR"),
            "uPWR Lo": safe("uPWR Lo"),
            "uPWR Hi": safe("uPWR Hi"),
            "uCCR": safe("uCCR"),
            "uCER": safe("uCER"),
            "uSNR": safe("uSNR"),
            "oPWR VAR": safe("oPWR VAR"),
            "oCER": safe("oCER"),
            "oMER": safe("oMER"),
            "oaPWR VAR": safe("oaPWR VAR"),
            "oaCCR": safe("oaCCR"),
            "oaCER": safe("oaCER"),
            "oaT3": safe("oaT3"),
            "oaT4": safe("oaT4"),
            "QoE Score":  qoe_score,
            "Impacted":   impacted,
            "Stressed":   stressed,
            "Total":      total,
            "Offline":    offline,
            "nid_bucket": nid_bucket(nid),
            "qoe_bucket": qoe_bucket(qoe_score),
        })
    return tech
 
 
# ── Build ADV (chronic offenders, divergence, quadrant) ───────────────────────
 
def build_adv(raw, tech, latest_nid_date):
    # Build node → all NID scores by date for streak calculation
    nid_scores = raw["nid_node_scores"]   # {node: {date: score}}
    qoe_scores = raw["qoe_node_scores"]   # {node: {date: score}}
 
    sorted_nid_dates = sorted(raw["nid_summary"], key=lambda x: x["date"])
    date_list = [d["date"] for d in sorted_nid_dates]
 
    # ── Chronic offenders: consecutive red/orange NID days ─────────────────
    chronic = []
    for node, date_scores in nid_scores.items():
        streak = 0
        for d in reversed(date_list):
            score = date_scores.get(d)
            if score is None:
                break
            if score <= 30:  # red or orange
                streak += 1
            else:
                break
        if streak >= 3:
            latest_nid = date_scores.get(date_list[-1])
            # get latest QoE from TECH
            tech_node = next((t for t in tech if t["Node"] == node), None)
            latest_qoe = tech_node["QoE Score"] if tech_node else None
            chronic.append({
                "node": node,
                "streak": streak,
                "streak_end": date_list[-1],
                "latest_nid": round(float(latest_nid), 1) if latest_nid is not None else None,
                "latest_qoe": float(latest_qoe) if latest_qoe is not None else None,
                "nid_bucket": nid_bucket(latest_nid),
                "qoe_bucket": qoe_bucket(latest_qoe),
            })
    chronic.sort(key=lambda x: x["streak"], reverse=True)
 
    # ── Quadrant + divergence ──────────────────────────────────────────────
    quadrant   = []
    div_nid_good = []
    div_qoe_good = []
 
    for t in tech:
        node = t["Node"]
        nid  = t["NID Score"]
        qoe  = t["QoE Score"]
        if nid is None or qoe is None:
            continue
        nb = nid_bucket(nid)
        qb = qoe_bucket(qoe)
        quadrant.append({"node": node, "nid": nid, "qoe": qoe,
                         "nid_bucket": nb, "qoe_bucket": qb})
        # NID good (green/yellow) but QoE bad (red/orange)
        if nb in ("green", "yellow") and qb in ("red", "orange"):
            div_nid_good.append({"node": node, "nid": nid, "qoe": qoe,
                                  "nid_bucket": nb, "qoe_bucket": qb,
                                  "gap": round(abs(nid - qoe), 1)})
        # QoE good (green/yellow) but NID bad (red/orange)
        if qb in ("green", "yellow") and nb in ("red", "orange"):
            div_qoe_good.append({"node": node, "nid": nid, "qoe": qoe,
                                  "nid_bucket": nb, "qoe_bucket": qb,
                                  "gap": round(abs(qoe - nid), 1)})
 
    div_nid_good.sort(key=lambda x: x["gap"], reverse=True)
    div_qoe_good.sort(key=lambda x: x["gap"], reverse=True)
 
    return {
        "chronic":      chronic,
        "div_nid_good": div_nid_good,
        "div_qoe_good": div_qoe_good,
        "quadrant":     quadrant,
        "latest_nid_date": latest_nid_date,
    }
 
 
# ── Load existing data.js ─────────────────────────────────────────────────────
 
def load_datajs(path="data.js"):
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
 
    raw_match  = re.search(r'const RAW = (\{.*?\});',  content, re.DOTALL)
    tech_match = re.search(r'const TECH = (\[.*?\]);', content, re.DOTALL)
    adv_match  = re.search(r'const ADV = (\{.*?\});',  content, re.DOTALL)
 
    if not (raw_match and tech_match and adv_match):
        raise ValueError("Could not parse RAW, TECH, or ADV from data.js")
 
    raw  = json.loads(raw_match.group(1))
    tech = json.loads(tech_match.group(1))
    adv  = json.loads(adv_match.group(1))
    return content, raw, tech, adv
 
 
# ── Write updated data.js ─────────────────────────────────────────────────────
 
def write_datajs(content, raw, tech, adv, path="data.js"):
    raw_js  = json.dumps(raw,  separators=(',', ':'), ensure_ascii=False)
    tech_js = json.dumps(tech, separators=(',', ':'), ensure_ascii=False)
    adv_js  = json.dumps(adv,  separators=(',', ':'), ensure_ascii=False)
 
    # Replace each blob — use plain string concat to avoid f-string brace issues
    content = re.sub(r'const RAW = \{.*?\};',
                     'const RAW = ' + raw_js + ';',
                     content, flags=re.DOTALL)
    content = re.sub(r'const TECH = \[.*?\];',
                     'const TECH = ' + tech_js + ';',
                     content, flags=re.DOTALL)
    content = re.sub(r'const ADV = \{.*?\};',
                     'const ADV = ' + adv_js + ';',
                     content, flags=re.DOTALL)
 
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
 
 
# ── Assertions ────────────────────────────────────────────────────────────────
 
def assert_integrity(raw, adv, nid_date, qoe_date):
    assert any(d["date"] == nid_date for d in raw["nid_summary"]), \
        f"NID date {nid_date} not in nid_summary"
    assert any(d["date"] == qoe_date for d in raw["qoe_summary"]), \
        f"QoE date {qoe_date} not in qoe_summary"
    assert nid_date in raw["nid_red_nodes"], \
        f"NID date {nid_date} not in nid_red_nodes"
    assert qoe_date in raw["qoe_red_nodes"], \
        f"QoE date {qoe_date} not in qoe_red_nodes"
    assert adv["latest_nid_date"] == nid_date, \
        f"ADV latest_nid_date mismatch: {adv['latest_nid_date']} vs {nid_date}"
    print("✅ All integrity assertions passed.")
 
 
# ── Main ──────────────────────────────────────────────────────────────────────
 
def main():
    parser = argparse.ArgumentParser(description="Ingest daily node health files into data.js")
    parser.add_argument("--nid",    help="Path to NID .xlsx file")
    parser.add_argument("--qoe",    help="Path to QoE .csv file")
    parser.add_argument("--inbox",  default="inbox", help="Inbox folder to scan (default: inbox/)")
    parser.add_argument("--datajs", default="data.js", help="Path to data.js (default: data.js)")
    args = parser.parse_args()
 
    # ── Resolve input files ────────────────────────────────────────────────
    nid_path = args.nid
    qoe_path = args.qoe
 
    if not nid_path or not qoe_path:
        nid_files, qoe_files = find_inbox_files(args.inbox)
        if not nid_path:
            if not nid_files:
                sys.exit(f"❌ No NID file found in {args.inbox}/")
            nid_path = nid_files[-1]  # use latest
        if not qoe_path:
            if not qoe_files:
                sys.exit(f"❌ No QoE file found in {args.inbox}/")
            qoe_path = qoe_files[-1]
 
    nid_date = date_from_nid_filename(nid_path)
    qoe_date = date_from_qoe_filename(qoe_path)
 
    print(f"📂 NID file : {nid_path}  →  date: {nid_date}")
    print(f"📂 QoE file : {qoe_path}  →  date: {qoe_date}")
 
    # ── Load current data.js ───────────────────────────────────────────────
    print(f"📖 Loading {args.datajs} ...")
    content, raw, old_tech, old_adv = load_datajs(args.datajs)
 
    # ── Skip if date already ingested ──────────────────────────────────────
    existing_nid_dates = {d["date"] for d in raw["nid_summary"]}
    existing_qoe_dates = {d["date"] for d in raw["qoe_summary"]}
 
    if nid_date in existing_nid_dates:
        print(f"⚠️  NID date {nid_date} already exists in data.js — skipping NID.")
        nid_new = False
    else:
        nid_new = True
 
    if qoe_date in existing_qoe_dates:
        print(f"⚠️  QoE date {qoe_date} already exists in data.js — skipping QoE.")
        qoe_new = False
    else:
        qoe_new = True
 
    if not nid_new and not qoe_new:
        print("✅ Nothing to do — both dates already ingested.")
        return
 
    # ── Read source files ──────────────────────────────────────────────────
    print("📊 Reading source files ...")
    df_nid = read_nid(nid_path)
    df_qoe = read_qoe(qoe_path)
 
    # ── Append to RAW ──────────────────────────────────────────────────────
    if nid_new:
        print(f"➕ Appending NID data for {nid_date} ...")
        raw["nid_summary"].append(build_nid_summary_row(df_nid, nid_date))
        raw["nid_summary"].sort(key=lambda x: x["date"])
 
        raw["nid_red_nodes"][nid_date] = build_nid_red_nodes(df_nid, df_qoe, nid_date)
 
        raw["nid_node_scores"] = update_node_scores(
            raw["nid_node_scores"], df_nid, "Node", "NID Score", nid_date)
 
    if qoe_new:
        print(f"➕ Appending QoE data for {qoe_date} ...")
        raw["qoe_summary"].append(build_qoe_summary_row(df_qoe, qoe_date))
        raw["qoe_summary"].sort(key=lambda x: x["date"])
 
        raw["qoe_red_nodes"][qoe_date] = build_qoe_red_nodes(df_qoe, qoe_date)
 
        raw["qoe_node_scores"] = update_node_scores(
            raw["qoe_node_scores"], df_qoe, "Node", "Score", qoe_date)
 
    # ── Rebuild TECH (always latest snapshot) ──────────────────────────────
    print("🔧 Rebuilding TECH ...")
    tech = build_tech(df_nid, df_qoe)
 
    # ── Rebuild ADV ────────────────────────────────────────────────────────
    print("🔧 Rebuilding ADV ...")
    adv = build_adv(raw, tech, nid_date)
 
    # ── Write updated data.js ──────────────────────────────────────────────
    print(f"💾 Writing {args.datajs} ...")
    write_datajs(content, raw, tech, adv, args.datajs)
 
    # ── Verify ────────────────────────────────────────────────────────────
    assert_integrity(raw, adv, nid_date, qoe_date)
 
    print(f"\n🎉 Done! data.js updated with NID={nid_date}, QoE={qoe_date}")
    print(f"   NID history: {len(raw['nid_summary'])} days")
    print(f"   QoE history: {len(raw['qoe_summary'])} days")
    print(f"   Chronic offenders: {len(adv['chronic'])}")
 
 
if __name__ == "__main__":
    main()
