import pymysql
import time
import pandas as pd
import re
import json
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from datetime import datetime  

db_config = {
    "host": "101.99.5.34",
    "port": 3308,
    "user": "root",
    "password": "qYBwu]cSZ[*NmS.0",
    "database": "bts"
}
influxdb_config = {
    "url": "http://localhost:8086",  # Corrected syntax
    "token": "3zRD8dz7l_Cl4ApMSt-Xea_Sl_gS0y7IJemVaLmftGTUX36xvzan_-kdUHPKy60oVma_4UW7uu27TdfI6lvIOw==",
    "org": "cmc",
    "bucket": "Test"
}
# Load JSON only once
@st.cache_data
def load_json(path):
    with open(path, 'r', encoding='utf-8') as file:
        return json.load(file)

def read_json(data, compare):
    try:
        return data.get(str(compare), None)
    except:
        return None

def highlight_alm(s):
    color = 'red' if s.Type == "ALM" else 'green'
    return [f'background-color: {color}'] * len(s)

def fetch_new_messages(cursor):
    cursor.execute("SELECT * FROM messages ORDER BY id DESC LIMIT 100;")
    return cursor.fetchall()

def extract_msg_time(details):
    # Extract the time pattern like 09:26:28-04/04/25
    match = re.search(r";(\d{2}:\d{2}:\d{2}-\d{2}/\d{2}/\d{2});", details)
    if match:
        try:
            return datetime.strptime(match.group(1), "%H:%M:%S-%d/%m/%y")
        except:
            return None
    return None

def process_data(raw_data, json_data):
    structured_data = []

    # Define keys for ALM and SEQ message types
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

    # Bản đồ mô tả cảnh báo
    ALARM_LABELS = {
        "AMABD": "Cảnh báo đột nhập",
        "AMADR": "Cảnh báo mở cửa",
        "AMAFL": "Ngập nước trong trạm",
        "AMAFR": "Cháy khói trong trạm",
        "AMATI": "Nhiệt độ cao",
        "AMIAC": "Mất điện AC",
        "AMIAL": "Điện AC thấp",
        "AMIAP": "Lệch tần số điện AC",
        "AMIAR": "Sự cố điều hòa",
        "AMIGN": "Đang chạy máy phát",
        "AMIHU": "Độ ẩm cao",
        "AMIPS": "Chập nguồn sensor",
        "AMIGN": "Không chạy máy phát"
    }
    
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
                if extracted_data.get(key) == "1":
                    alert_descriptions.append(desc)
            if extracted_data.get("AMIGN") == "0":
                alert_descriptions.append("Không chạy máy phát")
                
        extracted_data["Alert_Description"] = ", ".join(alert_descriptions) if alert_descriptions else None

        extracted_data["Message_Time"] = extract_msg_time(details)

        structured_data.append(extracted_data)

    df = pd.DataFrame(structured_data)

   
    df = df.dropna(subset=["EQID", "Message_Time"])
    df = df.drop_duplicates(subset=["EQID", "Message_Time"])
    df["Message_Time"] = pd.to_datetime(df["Message_Time"], errors='coerce')
    df = df.dropna(subset=["Message_Time"])

    
    df = df.sort_values("Message_Time").groupby("EQID", as_index=False).tail(1)
    return df


def write_data_to_influxdb(data, config):
    """
    Write processed data to InfluxDB.
    """
    with InfluxDBClient(
        url=config["url"],
        token=config["token"],
        org=config["org"]
    ) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        for record in data:
            try:
                point = (
                    Point("bts_messages")
                    .tag("EQID", record["EQID"])
                    .field("EQID_pi", record["EQID"])
                    .tag("Type", record["Type"])
                    .field("Type_pi", record["Type"])
                    .tag("Location", record["Location"] if record["Location"] else "Unknown")
                    .field("Location_pi", record["Location"] if record["Location"] else "Unknown")
                    .tag("Alert_Description", record["Alert_Description"] if record["Alert_Description"] else "None")
                    .field("Alert_Description_pi", record["Alert_Description"] if record["Alert_Description"] else "None")
                    .tag("Status", record["Status"])
                    .field("Status_pi", record["Status"])
                    .tag("Start_Time", str(record["Start_Time"]))
                    .field("Start_Time_pi", str(record["Start_Time"]))
                    .tag("End_Time", str(record["End_Time"]))
                    .field("End_Time_pi", str(record["End_Time"]))
                    .tag("Message_Time", str(record["Message_Time"]))
                    .field("Message_Time_pi", str(record["Message_Time"]))
                    .tag("latitude", str(record["latitude"]))
                    .tag("longitude", str(record["longitude"]))
                )

                # Add dynamic numeric fields
                for col in ["BTI0", "BTO0", "BHU0", "BAV", "BAP", "BAC0", "BAF", "BSE0",
                            "BFA", "BFD", "BPW", "BDV", "BDC", "BDE", "BDR", "BFR",
                            "BFL", "BPS", "BX1", "BX2", "BX3"]:
                    val = record.get(col)
                    if pd.notna(val):
                        point.field(col, float(val))

                # Write the point
                write_api.write(bucket=config["bucket"], record=point)

            except Exception as e:
                print(f"Failed to write record: {record}\nError: {e}")

        write_api.flush()

def main():
    st.title("📡 BTS Message Monitor")
    st.caption("Live feed of messages from MySQL and EQID mapped locations.")

    # Load the EQID JSON mapping file
    json_path = "locations.json"
    json_data_cp = load_json(json_path)
    json_data = {eqid: info["location_name"] for eqid, info in json_data_cp.items()}
    latitude = {eqid: info["latitude"] for eqid, info in json_data_cp.items()}
    longitude = {eqid: info["longitude"] for eqid, info in json_data_cp.items()}
    print(json_data)


    # Define the refresh interval slider
    refresh_interval = st.slider("Refresh Interval (seconds)", 1, 10, 3)

    # Auto-refresh every X seconds
    count = st_autorefresh(interval=refresh_interval * 1000, key="data_refresh")

    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()
        # add latitude and longitude to the df
        # df["latitude"] = df["EQID"].map(latitude)
        # df["longitude"] = df["EQID"].map(longitude)
        # df["Location"] = df["EQID"].map(json_data)
        st.write("Fetching new messages...")
        raw_messages = fetch_new_messages(cursor)
        df = process_data(raw_messages, json_data)
        df["latitude"] = df["EQID"].map(latitude)
        df["longitude"] = df["EQID"].map(longitude)
        df = df[[
            "Alert_Description", "Type", "EQID", "Location", "Message_Time", 
            "Start_Time", "End_Time","latitude", "longitude", "BTI0", "BTO0", "BHU0", "BAV", "BAP", 
            "BAC0", "BAF", "BSE0", "BFA", "BFD", "BPW", "BDV", "BDC", "BDE", 
            "BDR", "BFR", "BFL", "BPS", "BX1", "BX2", "BX3"
        ]]
        
        df["Status"] = df["Type"].apply(lambda x: "🔴 ALARM" if x == "ALM" else "✅ OK")
        
        st.dataframe(df, use_container_width=True)
        data_to_influx = df.to_dict(orient="records")
        # print(data_to_influx)
        write_data_to_influxdb(data_to_influx, influxdb_config)

    except Exception as e:
        st.error(f"Error: {e}")
    finally:
        try:
            cursor.close()
            connection.close()
        except:
            pass

if __name__ == "__main__":
    main()
