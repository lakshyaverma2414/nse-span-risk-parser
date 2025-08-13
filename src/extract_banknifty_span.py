#!/usr/bin/env python3
"""
extract_banknifty_span.py

Usage:
    - Put your nsccl.*.i02.zip OR the .spn file in the same folder.
    - Edit INPUT_PATH below or run: python extract_banknifty_span.py /path/to/nsccl.20250813.i2.zip
    - Output CSV: BANKNIFTY_span_extract.csv (same folder)

What it does:
    - Finds the .spn XML (inside zip or direct file)
    - Locates BANKNIFTY oopPf (options) and phyPf (underlying)
    - Extracts each option's strike, type, premium, delta, the 16-element risk array (ra/a)
    - Computes:
        * Worst_RA_per_unit = abs(min(ra_values))
        * SPAN_per_lot = Worst_RA_per_unit * lot_size
        * Notional = spot * lot_size (if spot found)
        * Exposure amounts for provided exposure rates (default 2.00% and 2.265%)
        * Total margins = SPAN_per_lot + exposure
    - Writes a CSV with everything useful for SHRI backtest / live run.
"""

import sys
import zipfile
import xml.etree.ElementTree as ET
import csv
import os

# ===== USER SETTINGS =====
# Change this to your file path, or call script with the file path as first arg.
INPUT_PATH = sys.argv[1] if len(sys.argv) > 1 else "nsccl.20250808.s.zip"
OUTPUT_CSV = "span_began.csv"
SYMBOL = "BANKNIFTY"

# Exposure rates to compute columns for (common values). You can change/add.
EXPOSURE_RATES = [0.02, 0.02265]   # 2.00% and 2.265% (example)

# Fallback lot size if not present in file (BankNifty historical/common): adjust as needed.
FALLBACK_LOT = 35

# ========== Helper functions ==========
def read_spn_from_path(path):
    """Return XML content string from either .zip (containing .spn) or .spn file directly."""
    if path.lower().endswith(".zip"):
        if not os.path.exists(path):
            raise FileNotFoundError(f"ZIP not found: {path}")
        with zipfile.ZipFile(path, "r") as z:
            # find first .spn file
            spn_name = next((n for n in z.namelist() if n.lower().endswith(".spn")), None)
            if spn_name is None:
                raise FileNotFoundError("No .spn file found inside ZIP")
            raw = z.read(spn_name)
            return raw.decode("latin-1", errors="ignore")
    elif path.lower().endswith(".spn"):
        with open(path, "rb") as f:
            raw = f.read()
            return raw.decode("latin-1", errors="ignore")
    else:
        # try both attempts
        if os.path.exists(path + ".zip"):
            return read_spn_from_path(path + ".zip")
        if os.path.exists(path + ".spn"):
            return read_spn_from_path(path + ".spn")
        raise FileNotFoundError(f"Input file not found or not .zip/.spn: {path}")

def get_parent(root_elem, child_elem):
    """Bruteforce parent lookup for xml.etree (no parent pointer)."""
    for p in root_elem.iter():
        for c in p:
            if c is child_elem:
                return p
    return None

def safe_float(x):
    try:
        return float(x)
    except:
        return None

# ========== Main parsing ==========
xml_text = read_spn_from_path(INPUT_PATH)
root = ET.fromstring(xml_text)

# locate pointInTime -> clearingOrg
point = root.find("pointInTime")
if point is None:
    raise ValueError("pointInTime node not found in SPN XML")
clearing = point.find("clearingOrg")
if clearing is None:
    raise ValueError("clearingOrg node not found in SPN XML")

# find all pfCode nodes equal to SYMBOL and collect their parents
bank_pf_parents = []
for pf in clearing.findall(".//pfCode"):
    if (pf.text or "").strip().upper() == SYMBOL:
        parent = get_parent(clearing, pf)
        if parent is not None:
            bank_pf_parents.append(parent)

# find oopPf (options portfolio) and phyPf (underlying) among parents
oop_pf = next((n for n in bank_pf_parents if n.tag.lower() == "ooppf" or n.tag.lower()=="oopPf".lower()), None)
phy_pf = next((n for n in bank_pf_parents if n.tag.lower() == "phyPf".lower()), None)

# fallback: try searching by tag directly too
if oop_pf is None:
    oop_pf = clearing.find(".//oopPf")
if phy_pf is None:
    phy_pf = clearing.find(".//phyPf")

if oop_pf is None or phy_pf is None:
    # sometimes tags vary; try scanning for pf blocks with pfCode text = SYMBOL
    # build mapping parent->pfCode
    found_op = None
    for p in clearing:
        pfcode = p.findtext("pfCode") or ""
        if pfcode.strip().upper() == SYMBOL:
            # guess which is options (series inside) vs phy
            if p.find("series") is not None and found_op is None:
                oop_pf = p
            elif p.find("phy") is not None:
                phy_pf = p

if oop_pf is None:
    raise ValueError(f"{SYMBOL} options (oopPf) block not found in SPN")
if phy_pf is None:
    # not fatal: we can still parse options; spot/notional will be None
    print("Warning: underlying phyPf not found; spot/notional columns will be empty.")

# UNDERLYING SPOT and LOT parse
spot = None
lot_size = None
phy_node = phy_pf.find("phy") if phy_pf is not None else None
if phy_node is not None:
    # common tags that might contain lot or multiplier
    spot = safe_float(phy_node.findtext("p") or "")
    # try common lot tags
    lot_candidates = [phy_node.findtext(t) for t in ("m","mult","mktLot","lotSize","lot","sc","l") if phy_node.findtext(t) is not None]
    # filter valid floats/ints
    for cand in lot_candidates:
        if cand:
            try:
                val = int(float(cand))
                if val > 0:
                    lot_size = 35 #val
                    break
            except:
                pass

# fallback lot if not found
if lot_size is None:
    lot_size = FALLBACK_LOT

# Collect all series (expiries) under oop_pf
series_nodes = oop_pf.findall("series")
if not series_nodes:
    raise ValueError("No <series> nodes found under BANKNIFTY oopPf")

# Build records
records = []
for s in series_nodes:
    expiry_raw = (s.findtext("pe") or "").strip()
    # normalize expiry to YYYY-MM-DD if it's YYYYMMDD or 20250828 etc.
    expiry = expiry_raw
    if expiry_raw.isdigit() and len(expiry_raw) == 8:
        expiry = f"{expiry_raw[0:4]}-{expiry_raw[4:6]}-{expiry_raw[6:8]}"
    # iterate opt nodes
    for opt in s.findall("opt"):
        try:
            typ = (opt.findtext("o") or "").strip()   # C or P
            strike_txt = (opt.findtext("k") or "").strip()
            strike = safe_float(strike_txt)
            premium = safe_float(opt.findtext("p") or "")
            delta = safe_float(opt.findtext("d") or "")
            # risk array RA -> list of <a> inside <ra>
            ra_node = opt.find("ra")
            if ra_node is None:
                # skip if no RA present
                continue
            raw_a_vals = [a.text for a in ra_node.findall("a")]
            a_vals = []
            for v in raw_a_vals:
                if v is None:
                    continue
                vclean = v.strip().replace(",", "")  # remove thousand separators if any
                try:
                    a_vals.append(float(vclean))
                except:
                    # try to strip non-numeric
                    filtered = "".join(ch for ch in vclean if ch in "0123456789-+.eE")
                    try:
                        a_vals.append(float(filtered))
                    except:
                        pass
            if not a_vals:
                continue
            worst = min(a_vals)          # most negative -> worst loss per unit (signed)
            worst_per_unit = abs(worst)  # positive rupee/point per unit
            span_per_lot = worst_per_unit * lot_size
            notional = (spot * lot_size) if spot is not None else None

            # exposures for each rate
            exposures = {}
            totals = {}
            for r in EXPOSURE_RATES:
                exp_amt = (notional * r) if notional is not None else None
                tot = span_per_lot + exp_amt if exp_amt is not None else None
                exposures[f"exposure_{int(r*10000)/100:.4f}_pct"] = exp_amt
                totals[f"total_{int(r*10000)/100:.4f}_pct"] = tot

            rec = {
                "expiry_raw": expiry_raw,
                "expiry": expiry,
                "strike": strike,
                "type": typ,
                "premium_in_rpf": premium,
                "delta_in_rpf": delta,
                "worst_RA_per_unit": worst_per_unit,
                "span_per_lot": span_per_lot,
                "spot_from_rpf": spot,
                "lot_size": lot_size,
                "notional": notional
            }
            rec.update(exposures)
            rec.update(totals)
            records.append(rec)
        except Exception as e:
            # keep parsing robust; skip problematic option but report
            print(f"Warning: failed to parse an option node: {e}")
            continue

# Write CSV
fieldnames = [
    "expiry_raw", "expiry", "strike", "type", "premium_in_rpf", "delta_in_rpf",
    "worst_RA_per_unit", "span_per_lot", "spot_from_rpf", "lot_size", "notional"
]
# extend with exposures/totals based on EXPOSURE_RATES (consistent order)
for r in EXPOSURE_RATES:
    tag_exp = f"exposure_{int(r*10000)/100:.4f}_pct"
    tag_tot = f"total_{int(r*10000)/100:.4f}_pct"
    fieldnames.append(tag_exp)
    fieldnames.append(tag_tot)

with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csvf:
    writer = csv.DictWriter(csvf, fieldnames=fieldnames)
    writer.writeheader()
    for rec in records:
        writer.writerow(rec)

print(f"✅ Wrote {len(records)} option rows to {OUTPUT_CSV}")
print(f"Underlying spot (from file): {spot}; lot_size used: {lot_size}")
print("Example rows (first 6):")
import itertools, pprint
pprint.pprint(records[:6])
