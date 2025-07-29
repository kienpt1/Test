import os
import gc
import re
import json
import time
import pymysql
import pandas as pd
import requests as rqs
import streamlit as st
from datetime import datetime
from streamlit_autorefresh import st_autorefresh

# Webhook URL
url = "http://localhost:3020/prtg-webhook-electric-me"

# Database connection config
db_config = {
    "host": "101.99.5.34",
    "port": 3308,
    "user": "root",
    "password": "qYBwu]cSZ[*NmS.0",
    "database": "bts"
}

# Alarm mapping
ALARM_LABELS = {
    "AMABD1": "Cảnh báo đột nhập",
    "AMADR1": "Cảnh báo mở cửa",
    "AMAFL1": "Cảnh báo: ngập nước trong trạm",
    "AMAFR1": "Cảnh báo có cháy khói trong trạm",
    "AMATI1": "Cảnh báo nhiệt độ cao",
    "AMIAC1": "Cảnh báo: mất điện AC",
    "AMIAL1": "Cảnh báo điện AC thấp dưới ngưỡng",
    "AMIAP1": "Cảnh báo lệch tần số điện AC",
    "AMIAR1": "Cảnh báo: có sự cố điều hòa",
    "AMIGN1": "Cảnh báo đang chạy máy phát điện",
    "AMIHU1": "Cảnh báo độ ẩm trạm cao",
    "AMIPS1": "Cảnh báo chập nguồn sensor"
}

@st.cache_data
def load_json(path):
    with open(path, 'r', encoding='utf-8') as file:
        return json.load(file)

def read_json(data, compare):
    return data.get(str(compare), None)

def fetch_new_messages(cursor):
    cursor.execute("SELECT * FROM messages ORDER BY id DESC LIMIT 100;")
    return cursor.fetchall()

def extract_msg_time(details):
    match = re.search(r";(\d{2}:\d{2}:\d{2}-\d{2}/\d{2}/\d{2});", details)
    if match:
        try:
            return datetime.strptime(match.group(1), "%H:%M:%S-%d/%m/%y")
        except:
            return None
    return None

def process_data(raw_data, json_data):
    structured_data = []

    alm_keys = [
        "AMATI", "AMADR", "AMAFL", "AMAFR", "AMIPS", "AMIHU",
        "AMIAC", "AMIGN", "AMIAR", "AMIAL", "AMIAP",
        "AMIDC", "AMIDE", "AMIX1", "AMIX2", "AMIX3",
        "BTI0", "BHU", "BAV", "BAP", "BDV", "BDE"
    ]
    seq_keys = [
        "BTI0", "BTO0", "BHU0", "BAV", "BAP", "BAC0", "BAF", "BSE0",
        "BFA", "BFD", "BPW", "BDV", "BDC", "BDE", "BDR", "BFR",
        "BFL", "BPS", "BX1", "BX2", "BX3"
    ]

    for data in raw_data:
        id_value, status, _, value, start_time, _, end_time, details = data

        extracted_data = {
            "Start_Time": start_time,
            "End_Time": end_time,
        }

        eqid_match = re.search(r"EQID=(\d+)", details)
        msg_type_match = re.search(r"EQID=\d+;(ALM|SEQ);", details)

        extracted_data["EQID"] = eqid_match.group(1) if eqid_match else None
        extracted_data["Type"] = msg_type_match.group(1) if msg_type_match else "UNKNOWN"
        extracted_data["Location"] = read_json(json_data, extracted_data["EQID"])
        
        keys = alm_keys if extracted_data["Type"] == "ALM" else seq_keys
        
        for key in keys:
            match = re.search(rf"{key}[-:]?([0-9\.]+)", details)
            extracted_data[key] = match.group(1) if match else None

        alert_descriptions = []
        if extracted_data["Type"] == "ALM":
            for key, desc in ALARM_LABELS.items():
                if extracted_data.get(key[:-1]) == key[-1]:
                    alert_descriptions.append(desc)

        extracted_data["Alert_Description"] = ", ".join(alert_descriptions) if alert_descriptions else None
        extracted_data["Message_Time"] = extract_msg_time(details)
        structured_data.append(extracted_data)

    df = pd.DataFrame(structured_data)
    df = df.dropna(subset=["EQID", "Message_Time"])
    df = df.sort_values("Message_Time").groupby("EQID", as_index=False).tail(1)

    print(f"GC Collected: {gc.collect()}")
    return df

def main():
    st.title("📡 CVCS-TEC Message Monitor Version 0.1 Low Tech")
    st.caption("Live feed of messages from MySQL and EQID mapped locations.")

    json_path = "locations.json"
    json_data_cp = load_json(json_path)
    json_data = {eqid: info["location_name"] for eqid, info in json_data_cp.items()}

    refresh_interval = st.slider("Refresh Interval (seconds)", 1, 10, 3)
    st_autorefresh(interval=refresh_interval * 1000, key="data_refresh")

    st.subheader("Loại bỏ toàn bộ cảnh báo của trạm")
    EXCLUDE_FILE = "excluded_eqids.json"
    if "excluded_eqids" not in st.session_state:
        if os.path.exists(EXCLUDE_FILE):
            with open(EXCLUDE_FILE, "r", encoding="utf-8") as f:
                st.session_state.excluded_eqids = set(json.load(f))
        else:
            st.session_state.excluded_eqids = set()

    with st.form("exclude_station_form"):

        exclude_eqid_input = st.text_input("Nhập EQID trạm cần loại", "")
        submitted = st.form_submit_button("Add")
        returned = st.form_submit_button("Delete")

        if submitted and exclude_eqid_input.strip():
            st.session_state.excluded_eqids.add(exclude_eqid_input.strip())
            with open(EXCLUDE_FILE, "w", encoding="utf-8") as f:
                json.dump(list(st.session_state.excluded_eqids), f, indent=2)
            st.success(f"✅ Đã loại trạm có EQID = {exclude_eqid_input.strip()}")


        if returned:
            with open("excluded_eqids.json", "w", encoding="utf-8") as f:
                 json.dump([], f, indent=2)
            st.success("Đã xóa nội dung trạm không bắn cảnh báo")

    if st.session_state.excluded_eqids:
        st.info("🚫 Các trạm đã bị loại: " + ", ".join(st.session_state.excluded_eqids))

    st.subheader("🚫 Loại bỏ một số cảnh báo cụ thể theo trạm")
    EXCLUDED_ALERTS_FILE = "excluded_alerts_by_eqid.json"
    if "excluded_alerts_by_eqid" not in st.session_state:
        if os.path.exists(EXCLUDED_ALERTS_FILE):
            try:
                with open(EXCLUDED_ALERTS_FILE, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    st.session_state.excluded_alerts_by_eqid = json.loads(content) if content else {}
            except json.JSONDecodeError:
                st.warning("⚠️ Lỗi định dạng trong excluded_alerts_by_eqid.json. Khởi tạo lại.")
                st.session_state.excluded_alerts_by_eqid = {}
        else:
            st.session_state.excluded_alerts_by_eqid = {}

    with st.form("exclude_specific_alerts_form"):
        eqid_input = st.text_input("EQID của trạm", "")
        alarm_options = list(ALARM_LABELS.values())
        selected_alarms = st.multiselect("Chọn loại cảnh báo cần loại", options=alarm_options)
        submitted_alarms = st.form_submit_button("Add")
        return_alarms = st.form_submit_button("Delete")

        if submitted_alarms and eqid_input.strip() and selected_alarms:
            eqid = eqid_input.strip()
            excluded = st.session_state.excluded_alerts_by_eqid.get(eqid, [])
            for alarm in selected_alarms:
                if alarm not in excluded:
                    excluded.append(alarm)
            st.session_state.excluded_alerts_by_eqid[eqid] = excluded
            with open(EXCLUDED_ALERTS_FILE, "w", encoding="utf-8") as f:
                json.dump(st.session_state.excluded_alerts_by_eqid, f, indent=2, ensure_ascii=False)
            st.success(f"✅ Đã loại {len(selected_alarms)} cảnh báo cho trạm EQID = {eqid}")
        
        if return_alarms:
            with open("excluded_alerts_by_eqid.json", "w", encoding="utf-8") as f:
                 json.dump([], f, indent=2)
            st.success("Đã xóa nội dung cảnh bảo trạm ")


    if st.session_state.excluded_alerts_by_eqid:
        st.info("📌 Cảnh báo bị loại theo trạm:")
        for eqid, alerts in st.session_state.excluded_alerts_by_eqid.items():
            st.write(f"• **{eqid}**: {', '.join(alerts)}")

    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        st.write("Fetching new messages...")
        raw_messages = fetch_new_messages(cursor)
        df = process_data(raw_messages, json_data)

        if 'sent_alerts' not in st.session_state:
            st.session_state.sent_alerts = {}

        now = datetime.now()

        for _, row in df.iterrows():
            eqid = row["EQID"]
            if eqid in st.session_state.excluded_eqids:
                continue
            if row["Type"] != "ALM":
                continue

            desc = row["Alert_Description"]
            loc = row["Location"]
            time_key = f"{eqid}_{desc}"

            # ❌ Bỏ cảnh báo cụ thể theo trạm
            excluded_alerts = st.session_state.get("excluded_alerts_by_eqid", {})
            if eqid in excluded_alerts and desc in excluded_alerts[eqid]:
                print(f"⏹️ Bỏ qua cảnh báo '{desc}' tại EQID {eqid}")
                continue

            last_sent = st.session_state.sent_alerts.get(time_key)
            if last_sent is None or (now - last_sent).total_seconds() > 300:
                json_payload = {
                    "status": "ok",
                    "Location": loc or "",
                    "EQID": eqid,
                    "Alert_Description": desc or "",
                    "datetime": row["Message_Time"].strftime("%Y-%m-%d %H:%M:%S")
                }
                print(json_payload)
                try:
                    response = rqs.post(url, json=json_payload, timeout=5)
                    if response.status_code == 200:
                        st.session_state.sent_alerts[time_key] = now
                        print(f"Gửi cảnh báo: {json_payload}")
                    else:
                        print(f"Lỗi gửi cảnh báo: {response.status_code}")
                except Exception as e:
                    print(f"Lỗi kết nối webhook: {e}")

        df["Status"] = df["Type"].apply(lambda x: "🔴 ALARM" if x == "ALM" else "✅ OK")
        st.dataframe(df, use_container_width=True)

    except Exception as e:
        st.error(f"❌ Lỗi: {e}")
    finally:
        try:
            cursor.close()
            connection.close()
        except:
            pass

if __name__ == "__main__":
    main()
