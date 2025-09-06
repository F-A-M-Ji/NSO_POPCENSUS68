import pyodbc
import hashlib
import base64
import pandas as pd
import time
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

# --- 1. ตั้งค่าการเชื่อมต่อและข้อมูลที่ต้องการ ---

# ตั้งค่าการเชื่อมต่อฐานข้อมูล
DB_CONFIG = {
    'driver': '{SQL Server}',
    'server': '192.168.0.204',
    'database': 'pop6768',
    'uid': 'pdan',
    'pwd': 'P@ssw0rd12#$'
}

# รายชื่อรหัสจังหวัดที่ต้องการประมวลผล
PROVINCE_CODES_TO_RUN = ["10", "11", "12", "90", "91", "92"] # <--- แก้ไขรหัสจังหวัดตรงนี้

# ตั้งค่า Key และ IV ให้ตรงกับ PHP
SECRET_KEY = b'Pp9xeukV5j3pp89w'
SECRET_IV = b'T5eUeG3MsJkhr6sc'

# ขนาดของข้อมูลที่จะส่งไปอัปเดตในแต่ละครั้ง (ปรับได้ตามความเหมาะสม)
BATCH_SIZE = 1000

# --- 2. ฟังก์ชันถอดรหัส (จำลองจาก PHP) ---

def decrypt_data(encrypted_string):
    """
    ถอดรหัสข้อมูลที่เข้ารหัสด้วย AES-256-CBC และ Base64
    คืนค่าเป็นข้อความที่ถอดรหัสแล้ว หรือ None หากเกิดข้อผิดพลาด
    """
    if not encrypted_string:
        return None

    try:
        # 1. สร้าง Key และ IV ให้เหมือน PHP
        key = hashlib.sha256(SECRET_KEY).digest()
        iv = hashlib.sha256(SECRET_IV).digest()[:16]

        # 2. ถอดรหัส Base64
        decoded_data = base64.b64decode(encrypted_string)

        # 3. ถอดรหัส AES-256-CBC
        cipher = AES.new(key, AES.MODE_CBC, iv)
        decrypted_padded = cipher.decrypt(decoded_data)

        # 4. ลบ Padding ออก (สำคัญมาก! แก้ปัญหา Padding Error)
        decrypted_text = unpad(decrypted_padded, AES.block_size).decode('utf-8')

        return decrypted_text

    except (ValueError, KeyError, TypeError, base64.binascii.Error) as e:
        # หากเกิดปัญหาในการถอดรหัส (เช่น padding, base64 ผิด) ให้คืนค่า None
        # print(f"  [Warning] Cannot decrypt '{encrypted_string[:20]}...': {e}")
        return None

# --- 3. ส่วนประมวลผลหลัก ---

def main():
    """
    ฟังก์ชันหลักในการเชื่อมต่อ, ดึงข้อมูล, ถอดรหัส, และอัปเดต
    """
    cnxn = None
    try:
        # เชื่อมต่อฐานข้อมูล
        print("Connecting to SQL Server...")
        cnxn_str = (
            f"DRIVER={DB_CONFIG['driver']};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['uid']};"
            f"PWD={DB_CONFIG['pwd']};"
        )
        cnxn = pyodbc.connect(cnxn_str)
        cursor = cnxn.cursor()
        print("Connection successful.\n")

        # วนลูปทำงานทีละจังหวัด
        for prov_code in PROVINCE_CODES_TO_RUN:
            start_time = time.time()
            print(f"============================================================")
            print(f"Processing Province Code: {prov_code}")
            
            # 1. ดึงข้อมูลทั้งจังหวัดมาเก็บใน DataFrame (Pandas)
            sql_select = """
                SELECT IDEN, FirstName, LastName 
                FROM r_alldata 
                WHERE ProvCode = ? AND (FirstName_D IS NULL OR LastName_D IS NULL)
            """
            print("Fetching data...")
            df = pd.read_sql_query(sql_select, cnxn, params=[prov_code])

            if df.empty:
                print("No records to process for this province. Skipping.\n")
                continue

            print(f"Found {len(df)} records to process.")
            
            # 2. ถอดรหัสข้อมูลใน DataFrame
            updates_to_execute = []
            failed_count = 0
            
            print("Decrypting records...")
            for row in df.itertuples():
                decrypted_first = decrypt_data(row.FirstName)
                decrypted_last = decrypt_data(row.LastName)

                # เพิ่มข้อมูลเข้า list สำหรับอัปเดตเฉพาะที่ถอดรหัสได้สำเร็จทั้ง 2 ฟิลด์
                if decrypted_first and decrypted_last:
                    updates_to_execute.append((decrypted_first, decrypted_last, row.IDEN))
                else:
                    failed_count += 1
            
            # 3. อัปเดตข้อมูลกลับไปยังฐานข้อมูลเป็นชุด (Batch Update)
            if not updates_to_execute:
                print("No records were successfully decrypted. Nothing to update.")
            else:
                print(f"Starting batch update for {len(updates_to_execute)} records...")
                sql_update = "UPDATE r_alldata SET FirstName_D = ?, LastName_D = ? WHERE IDEN = ?"
                
                # เปิดใช้งาน Fast ExecuteMany เพื่อความเร็วสูงสุด
                cursor.fast_executemany = True
                cursor.executemany(sql_update, updates_to_execute)
                cnxn.commit()
                print("Batch update completed.")

            end_time = time.time()
            # สรุปผล
            print("\n--- Summary for Province Code: {prov_code} ---")
            print(f"Total records found:      {len(df)}")
            print(f"Successfully decrypted:   {len(updates_to_execute)}")
            print(f"Failed/Skipped records:   {failed_count}")
            print(f"Time taken:               {end_time - start_time:.2f} seconds")
            print(f"============================================================\n")

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Database Error Occurred: {sqlstate}")
        print(ex)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if cnxn:
            cnxn.close()
            print("Database connection closed.")

# สั่งให้ฟังก์ชัน main() ทำงานเมื่อรันสคริปต์
if __name__ == "__main__":
    main()

    # pip install pycryptodomex pyodbc pandas