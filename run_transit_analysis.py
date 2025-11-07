# run_transit_analysis.py
import json, math, os
from datetime import datetime, timezone
import pandas as pd
from dateutil import parser as dtparser
import pytz

INPUT_FILE = "Downloads/Swift Assignment 4 - Dataset (2).json"
DETAIL_CSV = "transitperformancedetailed.csv"
SUMMARY_OVERALL_CSV = "transitperformancesummary_overall.csv"
SUMMARY_BY_SERVICE_CSV = "transitperformancesummary_by_service.csv"
SUMMARY_LONGFORM_CSV = "transitperformancesummary.csv"

IST = pytz.timezone("Asia/Kolkata")

def parse_ts(value):
    """Parse timestamps from dict(numberLong), epoch ms/sec, or ISO strings to IST timezone."""
    if value is None:
        return pd.NaT
    if isinstance(value, float) and math.isnan(value):
        return pd.NaT
    try:
        if isinstance(value, dict) and "numberLong" in value:
            ms = int(value["numberLong"])
            dt = datetime.fromtimestamp(ms/1000, tz=timezone.utc)
            return dt.astimezone(IST)
        if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
            ms = int(value)
            # Heuristic: > 1e12 looks like ms; > 1e9 looks like sec UNIX epoch
            if ms > 10**12:
                dt = datetime.fromtimestamp(ms/1000, tz=timezone.utc)
            elif ms > 10**9:
                dt = datetime.fromtimestamp(ms, tz=timezone.utc)
            else:
                dt = datetime.fromtimestamp(ms, tz=timezone.utc)
            return dt.astimezone(IST)
        dt = dtparser.parse(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(IST)
    except Exception:
        return pd.NaT

def safe_get(d, path, default=None):
    cur = d
    for p in path:
        if cur is None:
            return default
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur

def is_facility(arrival_location):
    if not arrival_location:
        return False
    s = str(arrival_location).upper()
    # Matches ORIGINFEDEXFACILITY, DESTINATIONFEDEXFACILITY, FEDEXFACILITY, etc.
    return "FACILITY" in s

def classify_service(service_type):
    """Classify service type into express/standard using simple keyword mapping."""
    if not service_type:
        return "standard"
    st = str(service_type).upper()
    express_keys = [
        "PRIORITY",
        "STANDARDOVERNIGHT",
        "INTERNATIONALPRIORITY",
        "FEDEXPRIORITYOVERNIGHT"
    ]
    economy_keys = [
        "SAVER",
        "ECONOMY",
        "FEDEXEXPRESSSAVER"
    ]
    if any(k in st for k in express_keys):
        return "express"
    if any(k in st for k in economy_keys):
        return "standard"
    return "standard"

def is_pickup_event(et, ed):
    e = str(et or "").upper()
    d = str(ed or "").upper()
    return e == "PU" or "PICKED UP" in d

def is_delivery_event(et, ed):
    e = str(et or "").upper()
    d = str(ed or "").upper()
    return e == "DL" or "DELIVERED" in d

def is_ofd_event(et, ed):
    e = str(et or "").upper()
    d = str(ed or "").upper()
    return e == "OD" or "OUT FOR DELIVERY" in d or "ON FEDEX VEHICLE FOR DELIVERY" in d

def load_shipments(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    shipments = []

    def add_track_details(obj):
        td = safe_get(obj, ["trackDetails"])
        if isinstance(td, list):
            shipments.extend(td)
        elif isinstance(td, dict):
            shipments.append(td)
        else:
            if isinstance(obj, list):
                for x in obj:
                    add_track_details(x)

    if isinstance(data, list):
        for obj in data:
            add_track_details(obj)
    elif isinstance(data, dict):
        add_track_details(data)

    # Fallback: some datasets might be a raw list of shipments
    if not shipments and isinstance(data, list):
        shipments = data

    return shipments

def build_detail_df(shipments):
    rows = []
    for s in shipments:
        tracking = safe_get(s, ["trackingNumber"])
        service_type = safe_get(s, ["service","type"])
        service_desc = safe_get(s, ["service","description"])
        carrier = safe_get(s, ["carrierCode"])
        pkg_w_val = safe_get(s, ["packageWeight","value"])
        pkg_w_units = safe_get(s, ["packageWeight","units"])
        packaging = (
            safe_get(s, ["packaging","type"])
            or safe_get(s, ["packaging","description"])
            or safe_get(s, ["packaging","shortDescription"])
            or safe_get(s, ["packaging","type"])
        )

        origin_city = safe_get(s, ["shipperAddress","city"])
        origin_state = safe_get(s, ["shipperAddress","stateOrProvinceCode"])
        origin_pin = safe_get(s, ["shipperAddress","postalCode"])
        dest_city = safe_get(s, ["destinationAddress","city"]) or safe_get(s, ["lastUpdatedDestinationAddress","city"])
        dest_state = safe_get(s, ["destinationAddress","stateOrProvinceCode"]) or safe_get(s, ["lastUpdatedDestinationAddress","stateOrProvinceCode"])
        dest_pin = safe_get(s, ["destinationAddress","postalCode"]) or safe_get(s, ["lastUpdatedDestinationAddress","postalCode"])

        delivery_loc_type = safe_get(s, ["deliveryLocationType"])

        # datesOrTimes for pickup/delivery
        dates_times = safe_get(s, ["datesOrTimes"], []) or []
        pickup_dt = pd.NaT
        delivery_dt = pd.NaT
        if isinstance(dates_times, list):
            for dt_item in dates_times:
                t = (safe_get(dt_item, ["type"]) or "").upper()
                ts = safe_get(dt_item, ["dateOrTimestamp"])
                if t == "ACTUALPICKUP" and pd.isna(pickup_dt):
                    pickup_dt = parse_ts(ts)
                if t == "ACTUALDELIVERY" and pd.isna(delivery_dt):
                    delivery_dt = parse_ts(ts)

        # Events
        evs = safe_get(s, ["events"], []) or []
        event_rows = []
        for e in evs:
            et = safe_get(e, ["eventType"])
            ed = safe_get(e, ["eventDescription"])
            ts = parse_ts(safe_get(e, ["timestamp"]) or safe_get(e, ["dateOrTimestamp"]))
            city = safe_get(e, ["address","city"])
            state = safe_get(e, ["address","stateOrProvinceCode"])
            pincode = safe_get(e, ["address","postalCode"])
            arrloc = safe_get(e, ["arrivalLocation"])
            event_rows.append({
                "eventType": et,
                "eventDescription": ed,
                "timestamp": ts,
                "city": city,
                "state": state,
                "postalCode": pincode,
                "arrivalLocation": arrloc,
                "isFacility": is_facility(arrloc),
                "isPickup": is_pickup_event(et, ed),
                "isDelivery": is_delivery_event(et, ed),
                "isOFD": is_ofd_event(et, ed),
            })

        ev_df = pd.DataFrame(event_rows)
        if not ev_df.empty:
            ev_df = ev_df.sort_values("timestamp")

        # Derive pickup/delivery from events if missing
        if pd.isna(pickup_dt) and not ev_df.empty:
            p = ev_df.loc[ev_df["isPickup"] & ev_df["timestamp"].notna()]
            if not p.empty:
                pickup_dt = p["timestamp"].min()
        if pd.isna(delivery_dt) and not ev_df.empty:
            d = ev_df.loc[ev_df["isDelivery"] & ev_df["timestamp"].notna()]
            if not d.empty:
                delivery_dt = d["timestamp"].max()

        # Transit time hours
        total_transit_hours = None
        if not pd.isna(pickup_dt) and not pd.isna(delivery_dt):
            total_transit_hours = (delivery_dt - pickup_dt).total_seconds()/3600.0

        # Facility touchpoints
        num_facilities_flag = int(ev_df["isFacility"].sum()) if not ev_df.empty else 0
        unique_facilities = 0
        if not ev_df.empty:
            unique_facilities = ev_df.loc[ev_df["isFacility"], "arrivalLocation"].astype(str).str.upper().nunique()

        # Count in-transit events
        num_in_transit_events = int((ev_df["eventType"].astype(str).str.upper() == "IT").sum()) if not ev_df.empty else 0

        # Inter-facility transit hours: sum of gaps between consecutive facility events
        time_in_interfacility_hours = None
        if not ev_df.empty:
            fac = ev_df.loc[ev_df["isFacility"] & ev_df["timestamp"].notna(), ["timestamp"]].copy()
            if len(fac) >= 2:
                gaps = fac["timestamp"].diff().dropna().dt.total_seconds()/3600.0
                time_in_interfacility_hours = float(gaps.sum())
            else:
                time_in_interfacility_hours = 0.0

        # Average hours per facility visit 
        avg_hours_per_facility = None
        denom = num_facilities_flag if num_facilities_flag else unique_facilities
        if total_transit_hours is not None and denom:
            avg_hours_per_facility = total_transit_hours/denom

        # Service express flag
        service_category = classify_service(service_type)
        is_express = True if service_category == "express" else False

        # OFD attempts and first-attempt delivery
        num_ofd = int(ev_df["isOFD"].sum()) if not ev_df.empty else 0
        first_attempt_delivery = None
        if not ev_df.empty:
            delivered = ev_df["isDelivery"].any()
            # heuristic: one OFD event preceding delivered status
            first_attempt_delivery = bool(delivered and num_ofd == 1)

        rows.append({
            "trackingnumber": tracking,
            "servicetype": service_type,
            "carriercode": carrier,
            "packageweightkg": pkg_w_val if (pkg_w_units and str(pkg_w_units).upper() == "KG") else pkg_w_val,
            "packagingtype": packaging,
            "origincity": origin_city,
            "originstate": origin_state,
            "originpincode": origin_pin,
            "destinationcity": dest_city,
            "destinationstate": dest_state,
            "destinationpincode": dest_pin,
            "pickupdatetimeist": pickup_dt,
            "deliverydatetimeist": delivery_dt,
            "totaltransithours": total_transit_hours,
            "numfacilitiesvisited": unique_facilities,
            "numintransitevents": num_in_transit_events,
            "timeininterfacilitytransithours": time_in_interfacility_hours,
            "avghoursperfacility": avg_hours_per_facility,
            "isexpressservice": is_express,
            "deliverylocationtype": delivery_loc_type,
            "numoutfordeliveryattempts": num_ofd,
            "firstattemptdelivery": first_attempt_delivery,
            "totaleventscount": int(len(ev_df)) if not ev_df.empty else 0
        })
    detail_df = pd.DataFrame(rows)
    return detail_df

def write_detail_csv(df):
    # Ensure column order
    cols = [
        "trackingnumber","servicetype","carriercode","packageweightkg","packagingtype",
        "origincity","originstate","originpincode",
        "destinationcity","destinationstate","destinationpincode",
        "pickupdatetimeist","deliverydatetimeist","totaltransithours",
        "numfacilitiesvisited","numintransitevents","timeininterfacilitytransithours",
        "avghoursperfacility","isexpressservice","deliverylocationtype",
        "numoutfordeliveryattempts","firstattemptdelivery","totaleventscount"
    ]
    # Add any missing columns as NaN for safety
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    df[cols].to_csv(DETAIL_CSV, index=False, date_format="%Y-%m-%d %H:%M:%S%z")

def write_summaries(detail_df):
    summary = {}
    summary["totalshipmentsanalyzed"] = int(len(detail_df))

    def safe_series_numeric(s):
        return pd.to_numeric(s, errors="coerce")

    transit = safe_series_numeric(detail_df["totaltransithours"])
    facilities = safe_series_numeric(detail_df["numfacilitiesvisited"])
    hours_per_fac = safe_series_numeric(detail_df["avghoursperfacility"])
    ofd = safe_series_numeric(detail_df["numoutfordeliveryattempts"])
    first_attempt = detail_df["firstattemptdelivery"].fillna(False).astype(bool)

    summary["avgtransithours"] = float(transit.mean(skipna=True)) if len(transit) else None
    summary["mediantransithours"] = float(transit.median(skipna=True)) if len(transit) else None
    summary["stddevtransithours"] = float(transit.std(skipna=True)) if len(transit) else None
    summary["mintransithours"] = float(transit.min(skipna=True)) if len(transit) else None
    summary["maxtransithours"] = float(transit.max(skipna=True)) if len(transit) else None

    summary["avgfacilitiespershipment"] = float(facilities.mean(skipna=True)) if len(facilities) else None
    summary["medianfacilitiespershipment"] = float(facilities.median(skipna=True)) if len(facilities) else None
    mode_vals = facilities.mode(dropna=True)
    summary["modefacilitiespershipment"] = float(mode_vals.iloc[0]) if not mode_vals.empty else None

    summary["avghoursperfacility"] = float(hours_per_fac.mean(skipna=True)) if len(hours_per_fac) else None
    summary["medianhoursperfacility"] = float(hours_per_fac.median(skipna=True)) if len(hours_per_fac) else None

    # Delivery performance
    summary["pctfirstattemptdelivery"] = float(first_attempt.mean()*100.0) if len(first_attempt) else None
    summary["avgoutfordeliveryattempts"] = float(ofd.mean(skipna=True)) if len(ofd) else None

    # Per-service breakdown
    svc_summary = (
        detail_df
        .groupby("servicetype", dropna=False)
        .agg(
            avgtransithoursbyservicetype=("totaltransithours","mean"),
            avgfacilitiesbyservicetype=("numfacilitiesvisited","mean"),
            countshipmentsbyservicetype=("trackingnumber","count")
        )
        .reset_index()
    )
    # Write CSVs
    pd.DataFrame([summary]).to_csv(SUMMARY_OVERALL_CSV, index=False)
    svc_summary.to_csv(SUMMARY_BY_SERVICE_CSV, index=False)

    # Long-form combined summary
    long_rows = []
    for k,v in summary.items():
        long_rows.append({"metric": k, "value": v})
    long_rows.append({"metric": "SECTION", "value": "SERVICE_TYPE_BREAKDOWN"})
    for _, r in svc_summary.iterrows():
        svc = r["servicetype"]
        long_rows.append({"metric": f"service::{svc or 'NULL'}::avgtransithours", "value": r["avgtransithoursbyservicetype"]})
        long_rows.append({"metric": f"service::{svc or 'NULL'}::avgfacilities", "value": r["avgfacilitiesbyservicetype"]})
        long_rows.append({"metric": f"service::{svc or 'NULL'}::count", "value": r["countshipmentsbyservicetype"]})
    pd.DataFrame(long_rows).to_csv(SUMMARY_LONGFORM_CSV, index=False)

def main():
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")
    shipments = load_shipments(INPUT_FILE)
    detail_df = build_detail_df(shipments)
    write_detail_csv(detail_df)
    write_summaries(detail_df)
    print("Done. Files written:")
    print(" -", DETAIL_CSV)
    print(" -", SUMMARY_OVERALL_CSV)
    print(" -", SUMMARY_BY_SERVICE_CSV)
    print(" -", SUMMARY_LONGFORM_CSV)
    print("hello")

if __name__ == "__main__":
    main()
