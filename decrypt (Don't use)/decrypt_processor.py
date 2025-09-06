# decrypt_processor.py

import pyodbc
from config import DB_CONFIG, decrypt_string

# **------ ส่วนที่ต้องแก้ไข ------**
# กำหนดรหัสจังหวัดทั้งหมดที่ต้องการให้สคริปต์ทำงาน
PROVINCE_CODES = [
    "49"
]
# **------ สิ้นสุดส่วนที่ต้องแก้ไข ------**

def process_decryption():
    """
    ฟังก์ชันหลักสำหรับดำเนินการถอดรหัสข้อมูลในฐานข้อมูล
    """
    conn = None
    try:
        # 1. สร้าง Connection String และเชื่อมต่อฐานข้อมูล
        conn_str = (
            f"DRIVER={DB_CONFIG['driver']};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['uid']};"
            f"PWD={DB_CONFIG['pwd']};"
        )
        print("กำลังเชื่อมต่อฐานข้อมูล...")
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("เชื่อมต่อฐานข้อมูลสำเร็จ!\n")

        # 2. เริ่มต้นกระบวนการวนลูปตามรหัสจังหวัด
        for prov_code in PROVINCE_CODES:
            print("=" * 50)
            print(f">>> เริ่มประมวลผลจังหวัด: {prov_code}")
            
            updated_count = 0

            # 3. เตรียมคำสั่ง SQL สำหรับดึงข้อมูลที่ยังไม่ได้ถอดรหัส
            # เพิ่มเงื่อนไข (FirstName IS NOT NULL OR LastName IS NOT NULL) เพื่อป้องกัน error ตอนส่งค่า None ไปถอดรหัส
            sql_select = """
                SELECT IDEN, FirstName, LastName 
                FROM r_alldata 
                WHERE ProvCode = ? 
                  AND (FirstName_D IS NULL OR LastName_D IS NULL)
                  AND (FirstName IS NOT NULL OR LastName IS NOT NULL)
            """

            # 4. ดึงข้อมูลทีละแถว
            cursor.execute(sql_select, prov_code)
            rows_to_process = cursor.fetchall()
            
            if not rows_to_process:
                print(f"ไม่พบข้อมูลที่ต้องถอดรหัสสำหรับจังหวัด {prov_code}")
                continue

            print(f"พบข้อมูลที่ต้องประมวลผล {len(rows_to_process)} รายการ...")

            # 5. วนลูปเพื่อถอดรหัสและอัปเดต
            for row in rows_to_process:
                iden, first_name_enc, last_name_enc = row.IDEN, row.FirstName, row.LastName

                decrypted_firstname = decrypt_string(first_name_enc)
                decrypted_lastname = decrypt_string(last_name_enc)
                
                # เตรียมคำสั่ง SQL สำหรับอัปเดตโดยใช้ IDEN
                sql_update = """
                    UPDATE r_alldata 
                    SET FirstName_D = ?, LastName_D = ? 
                    WHERE IDEN = ?
                """
                
                try:
                    cursor.execute(sql_update, decrypted_firstname, decrypted_lastname, iden)
                    updated_count += 1
                except pyodbc.Error as ex:
                    print(f"!!! เกิดข้อผิดพลาดในการอัปเดต IDEN: {iden} - {ex}")
                    conn.rollback() # หากเกิด error ให้ยกเลิกการ commit ของแถวนั้น
            
            # Commit การเปลี่ยนแปลงทั้งหมดของจังหวัดนี้ลงฐานข้อมูล
            conn.commit()
            print(f"อัปเดตข้อมูลสำหรับจังหวัด {prov_code} สำเร็จ: {updated_count} แถว")
            print("=" * 50 + "\n")


    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"เกิดข้อผิดพลาดเกี่ยวกับฐานข้อมูล: {sqlstate}")
        print(ex)
    except Exception as e:
        print(f"เกิดข้อผิดพลาดที่ไม่คาดคิด: {e}")
    finally:
        if conn:
            conn.close()
            print("ปิดการเชื่อมต่อฐานข้อมูลเรียบร้อยแล้ว")

# --- จุดเริ่มต้นการทำงานของสคริปต์ ---
if __name__ == "__main__":
    process_decryption()