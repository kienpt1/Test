import os
import re
import json
import time
import logging 
import pymysql
import pandas as pd
import requests as rqs
import streamlit as st
from datetime import datetime
import pickle
from streamlit_autorefresh import st_autorefresh
logging.getLogger("streamlit.runtime.scriptrunner.script_runner").setLevel(logging.ERROR)
# Webhook URL
url = "http://localhost:4587/prtg-webhook-electric-me"

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
    "AMIAR1": "Cảnh báo: có sự cố điều hòa",
    "AMIHU1": "Cảnh báo độ ẩm trạm cao",
    "AMIPS1": "Cảnh báo chập nguồn sensor"
}

@st.cache_data
def load_json(path):
    with open(path, 'r', encoding='utf-8') as file:
        return json.load(file)

def read_json(data, compare):
    return data.get(str(compare), None)

STATE_FILE = "alert_state.json"

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_state(data):
    serializable_data = {
        key: value if isinstance(value, str) else value.strftime("%Y-%m-%d %H:%M:%S")
        for key, value in data.items()
    }
    with open(STATE_FILE, "w") as f:
        json.dump(serializable_data, f, indent=2)

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


def process_data(raw_data, json_data, json_path):

    ####################
    json_data, pool_bitch = load_location_data(json_path)
    ####################
    
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
    seq_descriptions = [ 
        "Nhiệt độ trong trạm", "Nhiệt độ ngoài", "Độ ẩm môi trường (%)", "Điện áp AC (V)", 
        "Tần số (Hz)", "Dòng điện AC (A)", "cos phi", "Điện đang sử dụng (Điện lưới/Máy phát)",
        "Quạt AC (Bật/Tắt)", "Quạt DC (Bật/Tắt)", "Công suất tiêu thụ AC (Wh)", 
        "Điện áp tổ acquy", "Dòng điện DC (A)", "Điện áp lệch giữa 2 tổ acquy", 
        "Mở cửa (Có/Không)", "Cháy/Khói (Có/Không)", "Ngập nước (Có/Không)",
        "CB nguồn cấp cho sensor (BT/Chập nguồn)", "", "", ""
    ]
    results = [] 
    Last_love = []
    for data in raw_data:
        id_value, status, _, value, start_time, _, end_time, details = data
        #print(details)
        extracted_data = {
            "Start_Time": start_time,
            "End_Time": end_time,
        }

        eqid_match = re.search(r"EQID=(\d+)", details)
        msg_type_match = re.search(r"EQID=\d+;(ALM|SEQ);", details)
        temperature = re.search(r"BTI0(\d+\.\d+)", details)
        extracted_data["EQID"] = eqid_match.group(1) if eqid_match else None
        extracted_data["Type"] = msg_type_match.group(1) if msg_type_match else "UNKNOWN"
        extracted_data["Location"] = read_json(json_data, extracted_data["EQID"])
        extracted_data["Value_Temp"] = float(temperature.group(1)) if temperature else None
        #results.append(f"{extracted_data['Location']}:{extracted_data['Value_Temp']}")
        results.append({
                    "Location": extracted_data["Location"],
                    "Value_Temp": extracted_data["Value_Temp"]
                })
        keys = alm_keys if extracted_data["Type"] == "ALM" else seq_keys
        descriptions = None
        if extracted_data["Type"] == "SEQ":
            descriptions = seq_descriptions

        # Extract key values
        for i, key in enumerate(keys):
            match = re.search(rf"{key}[-:]?([0-9\.]+)", details)
            value = match.group(1) if match else None
            if extracted_data["Type"] == "SEQ" and descriptions and i < len(descriptions):
                desc = descriptions[i]
                extracted_data[desc] = value
            else:
                extracted_data[key] = value

        # Process alarms
        alert_descriptions = []
        if extracted_data["Type"] == "ALM":
            ac_status = extracted_data.get("AMIAC")
            gen_status = extracted_data.get("AMIGN")

            if ac_status == "1":
                if gen_status == "1":
                    alert_descriptions.append("Mất điện AC - Máy phát đang chạy")
                elif gen_status == "0":
                    alert_descriptions.append("Mất điện AC - Máy phát KHÔNG chạy!")
                else:
                    alert_descriptions.append("Mất điện AC")

            elif ac_status == "0":
                if gen_status == "1":
                    alert_descriptions.append("Có điện AC nguồn điện của phát")
                elif gen_status == "0":
                    alert_descriptions.append("Có điện AC, không chạy máy phát")
                else:
                    alert_descriptions.append("Có điện AC")

            for key, desc in ALARM_LABELS.items():
                if key.startswith("AMIAC") or key.startswith("AMIGN"):
                    continue
                if extracted_data.get(key[:-1]) == key[-1]:
                    alert_descriptions.append(desc)

        extracted_data["Alert_Description"] = ", ".join(alert_descriptions) if alert_descriptions else None
        extracted_data["Message_Time"] = extract_msg_time(details)
        structured_data.append(extracted_data)

    for r in results:
        if r['Location'] not in results:
             if len(Last_love)==42:
                 #print("checkmate")
                 break
             else:  
                Last_love.append(r) 


    for j in pool_bitch: 
        for i in Last_love:
            if str(j) == str(i['Location']):
                clone = {j:i['Value_Temp']}
        print(clone)

    df = pd.DataFrame(structured_data)
    df = df.dropna(subset=["EQID", "Message_Time"])
    df = df.sort_values("Message_Time").groupby("EQID", as_index=False).tail(1)
    return alert_descriptions, df

def load_location_data(json_path):
    pool_bitch=[]
    json_data_cp = load_json(json_path)
    for eqid, info in json_data_cp.items(): 
        check_mate = info["location_name"]
        pool_bitch.append(check_mate)
    
    # # location_name= info
    return {eqid: info["location_name"] for eqid, info in json_data_cp.items()}, pool_bitch

def handle_exclude_whole_stations():
    st.subheader("Loại bỏ toàn bộ cảnh báo của trạm")
    EXCLUDE_FILE = "excluded_eqids.json"

    if "excluded_eqids" not in st.session_state:

        if os.path.exists(EXCLUDE_FILE):
            try:
                with open(EXCLUDE_FILE, "r", encoding="utf-8") as f:
                    st.session_state.excluded_eqids = set(json.load(f))
            except json.JSONDecodeError:
                st.warning("Lỗi định dạng trong excluded_eqids.json. Khởi tạo lại.")
                st.session_state.excluded_eqids = set()
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
            st.success(f"Đã loại trạm EQID = {exclude_eqid_input.strip()}")

        if returned:
            st.session_state.excluded_eqids = set()
            with open(EXCLUDE_FILE, "w", encoding="utf-8") as f:
                json.dump([], f, indent=2)
            st.success("Đã xóa danh sách trạm bị loại.")

    if st.session_state.excluded_eqids:
        st.info("Các trạm đã bị loại: " + ", ".join(st.session_state.excluded_eqids))


def handle_exclude_specific_alerts():
    st.subheader("Loại bỏ một số cảnh báo cụ thể theo trạm")
    EXCLUDED_ALERTS_FILE = "excluded_alerts_by_eqid.json"

    if "excluded_alerts_by_eqid" not in st.session_state:
        if os.path.exists(EXCLUDED_ALERTS_FILE):
            try:
                with open(EXCLUDED_ALERTS_FILE, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    st.session_state.excluded_alerts_by_eqid = json.loads(content) if content else {}
            except json.JSONDecodeError:
                st.warning("Lỗi định dạng JSON. Khởi tạo lại.")
                st.session_state.excluded_alerts_by_eqid = {}
        else:
            st.session_state.excluded_alerts_by_eqid = {}

    if not isinstance(st.session_state.excluded_alerts_by_eqid, dict):
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
            st.success(f"Đã loại {len(selected_alarms)} cảnh báo cho trạm EQID = {eqid}")

        if return_alarms:
            st.session_state.excluded_alerts_by_eqid = {}
            with open(EXCLUDED_ALERTS_FILE, "w", encoding="utf-8") as f:
                json.dump({}, f, indent=2, ensure_ascii=False)
            st.success("Đã xóa toàn bộ cảnh báo bị loại.")

    if st.session_state.excluded_alerts_by_eqid:
        st.info("Cảnh báo bị loại theo trạm:")
        for eqid, alerts in st.session_state.excluded_alerts_by_eqid.items():
            st.write(f"• **{eqid}**: {', '.join(alerts)}")


def handle_alert_processing(json_data,json_path):
    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        st.write("Fetching new messages...")
        raw_messages = fetch_new_messages(cursor)
        # without path for enough args 
        alert_descriptions, df = process_data(raw_messages, json_data, json_path)
        
        if 'sent_alerts' not in st.session_state:
            st.session_state.sent_alerts = {
                k: datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
                for k, v in load_state().items()
            }
        if 'active_alerts' not in st.session_state:
            st.session_state.active_alerts = {}
        if 'alarm_seen_at' not in st.session_state:
            st.session_state.alarm_seen_at = {}

        now = datetime.now()
        current_keys = set(f"{row['EQID']}_{row['Alert_Description']}" for _, row in df.iterrows())

        # Clean up old alerts
        to_remove = [key for key in st.session_state.active_alerts if key not in current_keys]
        for key in to_remove:
            st.session_state.active_alerts.pop(key, None)
            st.session_state.sent_alerts.pop(key, None)
            st.session_state.alarm_seen_at.pop(key, None)
            print(f"🧹 Xoá cảnh báo: {key}")

        for _, row in df.iterrows():
            eqid = row["EQID"]
            desc = row["Alert_Description"]
            loc = row["Location"]
            msg_time = row["Message_Time"]
            key = f"{eqid}_{desc}"

            if row["Type"] != "ALM" or eqid in st.session_state.excluded_eqids:
                continue

            excluded = st.session_state.excluded_alerts_by_eqid.get(eqid, [])
            if desc in excluded:
                print(f"Bỏ qua cảnh báo '{desc}' tại EQID {eqid}")
                continue

            if key not in st.session_state.alarm_seen_at:
                st.session_state.alarm_seen_at[key] = now
                print(f"Lần đầu thấy cảnh báo {key}")
                continue

            last_sent = st.session_state.sent_alerts.get(key)
            wait_seconds = 600 if "điện ac" in desc.lower() else 120
            print("tinnnnnnnnnnnnnnnnnnn",desc.lower())
            print("tinnnnnnnnnnnnnnnnnnnn",wait_seconds)
            if last_sent and (now - last_sent).total_seconds() < wait_seconds:
                st.session_state.active_alerts[key] = True
                continue
            print(f"thời gian gửi : {last_sent}")
            payload = {
                "status": "ok",
                "Location": loc or "",
                "EQID": eqid,
                "Alert_Description": desc or "",
                "datetime": msg_time.strftime("%Y-%m-%d %H:%M:%S")
            }

            try:
                response = rqs.post(url, json=payload, timeout=5)
                if response.status_code == 200:
                    st.session_state.sent_alerts[key] = now
                    st.session_state.active_alerts[key] = True
                    save_state(st.session_state.sent_alerts)
                    print(f"Đã gửi cảnh báo: {payload}")
                else:
                    print(f"Lỗi gửi cảnh báo: {response.status_code}")
            except Exception as e:
                print(f"Lỗi kết nối webhook: {e}")

            time.sleep(0.5)

        st.dataframe(df, use_container_width=True)

    except Exception as e:
        st.error(f"Lỗi: {e}")
    finally:
        try:
            cursor.close()
            connection.close()
        except:
            pass
# test
ALARM_CODES = {
    "AMABD1": "Cảnh báo đột nhập",
    "AMADR1": "Cảnh báo mở cửa",
    "AMAFL1": "Cảnh báo: ngập nước trong trạm",
    "AMAFR1": "Cảnh báo có cháy khói trong trạm",
    "AMATI1": "Cảnh báo nhiệt độ cao",
    "AMIAR1": "Cảnh báo: có sự cố điều hòa",
    "AMIHU1": "Cảnh báo độ ẩm trạm cao",
    "AMIPS1": "Cảnh báo chập nguồn sensor"
}

def main():
    st.title("CVCS-TEC Message Monitor Version 0.1 Low Tech")
    st.caption("Live feed of messages from MySQL and EQID mapped locations.")

    if 'alarm_seen_at' not in st.session_state:
        st.session_state.alarm_seen_at = {}

    json_path = "locations.json"
    json_data, pool_bitch = load_location_data(json_path)
    #print(pool_bitch)
    # for i in range(len(json_data)):
    #     print("check mate -----------",i[0])

    st_autorefresh(interval=5 * 1000, key="data_refresh")

    handle_exclude_whole_stations()
    handle_exclude_specific_alerts()
    handle_alert_processing(json_data,json_path)

if __name__ == "__main__":
    main()