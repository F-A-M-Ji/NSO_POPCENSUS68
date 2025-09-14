import pandas as pd
from sqlalchemy import create_engine
import urllib
import datetime
import os

# --- 1. การตั้งค่า ---
PROVINCE_CODES_TO_RUN = [
    '33', '34', '35', '36', '37', 
    '38', '39', '40', '42', '43', 
    '44', '45', '46', '47', '48', '49'
    ]  # << ระบุจังหวัดที่ต้องการประมวลผล
SOURCE_TABLE = 'r_online_survey_chk_dup'  # ตารางตั้งต้น (ผลลัพธ์จากส่วนที่ 1)
REFERENCE_TABLE = 'r_additional'           # ตารางอ้างอิง (ข้อมูล FI)
TARGET_TABLE = 'r_online_survey_dup_add'   # ตารางสำหรับเก็บผลลัพธ์สุดท้าย

# --- การตั้งค่า Report ---
REPORT_FILENAME = 'C:/Users/NSO/Desktop/pop_run_data/duplicate_check/report_final_check.csv'
REPORT_COLUMNS = [
    'ProvCode', 'time_begin', 'time_end', 'time_difference',
    'rows_from_source', 'rows_in_reference', 'rows_found_as_new'
]

# --- การตั้งค่าฐานข้อมูล ---
DB_SERVER = '192.168.0.203'
DB_NAME = 'pop6768'
DB_UID = 'pdan'
DB_PWD = 'P@ssw0rd12#$'

# คอลัมน์ที่ใช้เป็น Key ในการตรวจสอบความซ้ำซ้อน
key_columns = [
    'NsoBuilding_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'VilCode',
    'EA_Code_15', 'BuildingType', 'HouseNumber', 'RoomNumber', 'FirstName', 'LastName'
]

# --- 2. เริ่มกระบวนการ ---
sql_conn = None
try:
    # สร้าง Engine สำหรับการเชื่อมต่อที่มีประสิทธิภาพ
    quoted_password = urllib.parse.quote_plus(DB_PWD)
    engine_url = f"mssql+pyodbc://{DB_UID}:{quoted_password}@{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(engine_url)
    sql_conn = engine.connect()
    print("✅ เชื่อมต่อฐานข้อมูลสำเร็จ")

    for prov_code in PROVINCE_CODES_TO_RUN:
        time_begin = datetime.datetime.now()
        print(f"\n--- เริ่มกระบวนการสำหรับจังหวัด: {prov_code} | เวลา: {time_begin:%H:%M:%S} ---")

        # ========================= ส่วนที่ 2: กรองข้อมูลซ้ำกับ r_additional =========================
        
        print(f"[{prov_code}] กำลังดึงข้อมูลจาก '{SOURCE_TABLE}'...")
        query_source = f"SELECT * FROM {SOURCE_TABLE} WHERE ProvCode = '{prov_code}'"
        df_source = pd.read_sql(query_source, sql_conn)

        print(f"[{prov_code}] กำลังดึงข้อมูลจาก '{REFERENCE_TABLE}'...")
        query_ref = f"SELECT {', '.join(key_columns)} FROM {REFERENCE_TABLE} WHERE ProvCode = '{prov_code}'"
        df_reference = pd.read_sql(query_ref, sql_conn)
        
        rows_source_count = len(df_source)
        rows_reference_count = len(df_reference)

        print(f"[{prov_code}] ข้อมูลต้นฉบับ: {rows_source_count:,} แถว | ข้อมูลอ้างอิง: {rows_reference_count:,} แถว")

        if df_source.empty:
            print(f"[{prov_code}] 🟡 ไม่พบข้อมูลใน '{SOURCE_TABLE}' เพื่อประมวลผล")
            df_new_records = pd.DataFrame()
        elif df_reference.empty:
            print(f"[{prov_code}] 🟡 ไม่พบข้อมูลอ้างอิงใน '{REFERENCE_TABLE}', ข้อมูลทั้งหมดจึงถือเป็นข้อมูลใหม่")
            df_new_records = df_source.copy()
        else:
            print(f"[{prov_code}] อยู่ระหว่างดำเนินการเปรียบเทียบข้อมูล...")
            df_source['composite_key'] = df_source[key_columns].fillna('').astype(str).sum(axis=1)
            df_reference['composite_key'] = df_reference[key_columns].fillna('').astype(str).sum(axis=1)
            
            existing_keys = set(df_reference['composite_key'])
            df_new_records = df_source[~df_source['composite_key'].isin(existing_keys)].copy()
        
        rows_new_records_count = len(df_new_records)
        if 'composite_key' in df_new_records.columns:
            df_new_records.drop(columns=['composite_key'], inplace=True)
            
        print(f"[{prov_code}] 🟢 พบข้อมูลใหม่ที่ไม่ซ้ำจำนวน {rows_new_records_count:,} แถว")
        
        # ========================= ส่วนที่ 3: บันทึกผลลัพธ์ลงตารางใหม่ =========================
        
        if not df_new_records.empty:
            print(f"[{prov_code}] กำลังบันทึกข้อมูลใหม่ลงในตาราง '{TARGET_TABLE}'...")
            df_new_records.to_sql(
                name=TARGET_TABLE,
                con=engine,
                if_exists='append',  # นำข้อมูลไปต่อท้ายตารางเดิม
                index=False,
                chunksize=1000
            )
            print(f"[{prov_code}] ✅ บันทึกข้อมูลสำเร็จ!")
        else:
            print(f"[{prov_code}] 🟡 ไม่มีข้อมูลใหม่ให้บันทึก")

        # ========================= ส่วนของการสร้าง Report =========================
        time_end = datetime.datetime.now()
        time_difference = time_end - time_begin

        report_data = {
            'ProvCode': prov_code,
            'time_begin': time_begin.strftime('%Y-%m-%d %H:%M:%S'),
            'time_end': time_end.strftime('%Y-%m-%d %H:%M:%S'),
            'time_difference': str(time_difference),
            'rows_from_source': rows_source_count,
            'rows_in_reference': rows_reference_count,
            'rows_found_as_new': rows_new_records_count
        }
        
        report_df = pd.DataFrame([report_data], columns=REPORT_COLUMNS)
        
        report_df.to_csv(
            REPORT_FILENAME,
            mode='a',
            header=not os.path.exists(REPORT_FILENAME),
            index=False,
            encoding='utf-8-sig'
        )
        print(f"[{prov_code}] ✅ บันทึก Report ลงไฟล์ '{REPORT_FILENAME}' เรียบร้อย | ใช้เวลา: {time_difference}")

except Exception as e:
    print(f"❌ เกิดข้อผิดพลาดร้ายแรง: {e}")
finally:
    if sql_conn:
        sql_conn.close()
        print("\nปิดการเชื่อมต่อฐานข้อมูลเรียบร้อยแล้ว")