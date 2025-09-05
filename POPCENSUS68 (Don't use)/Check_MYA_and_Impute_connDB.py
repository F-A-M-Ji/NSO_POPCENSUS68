import pandas as pd
from datetime import datetime
import pyodbc
from sqlalchemy import create_engine
import numpy as np
from urllib.parse import quote_plus

time_begin = datetime.now()
print("เวลาเริ่มต้น:", time_begin)

# การกำหนดค่าการเชื่อมต่อฐานข้อมูล
sql_conn_str = 'DRIVER={SQL Server};SERVER=172.19.3.71;DATABASE=pop6768;UID=danny;PWD=P@ssw0rd12#$'
#sql_conn = pyodbc.connect('DRIVER={SQL Server};SERVER=172.19.3.71;DATABASE=pop6768;UID=danny;PWD=P@ssw0rd12#$')
ProvCode = 32


def handle_general_imputation(df, condition_to_process):
    """
    ฟังก์ชันรวมสำหรับจัดการการคำนวณและแก้ไขข้อมูลทั่วไป
    โดยจะทำงานเฉพาะแถวที่ไม่ได้ถูกประมวลผลในเงื่อนไขพิเศษ
    """
    current_year_th = 2568

    # คำนวณ YearOfBirth จาก Age ที่มี YearOfBirth_Old ไม่สมบูรณ์
    condition_yob_short = (df['YearOfBirth'].astype(str).str.len().isin([1, 3]))
    condition_age_valid = (df['Age_01'] < 115) & df['Age_01'].notna()
    condition = condition_yob_short & condition_age_valid & condition_to_process
    calculated_yob = current_year_th - df.loc[condition, 'Age_01'].round()
    after_april_condition = df.loc[condition, 'MonthOfBirth'] >= 4
    calculated_yob.loc[after_april_condition] -= 1
    df.loc[condition, 'YearOfBirth_new'] = calculated_yob.astype('Int64')

    # คำนวณ Age จาก YearOfBirth ที่เป็นทศนิยม
    condition_main = df['Age_01'].notna()
    condition_decimal = condition_main & (df['Age_01'] % 1 != 0)
    condition_has_yob = condition_decimal & df['YearOfBirth'].notna() & condition_to_process
    df.loc[condition_has_yob, 'Age_01_new'] = (
            current_year_th - df.loc[condition_has_yob, 'YearOfBirth'].round()
    ).astype('Int64')

    # จัดการ Age ที่มีจำนวนหลักไม่ถูกต้อง
    condition_main = df['Age_01'].notna() & (df['Age_01'].astype(str).str.len() < 3)
    condition_has_yob = condition_main & df['YearOfBirth'].notna() & condition_to_process
    calculated_age_from_yob = current_year_th - df.loc[condition_has_yob, 'YearOfBirth'].round()
    df.loc[condition_has_yob, 'Age_01_new'] = calculated_age_from_yob.astype('Int64')

    # จัดการ YearOfBirth ที่ไม่สมบูรณ์ (2 หลัก)
    condition_main = df['Age_01_new'].isna() & df['YearOfBirth'].notna() & condition_to_process
    condition_2_digit_le_68 = condition_main & (df['YearOfBirth'] <= 68) & (df['YearOfBirth'] >= 0)
    new_yob_2500 = df.loc[condition_2_digit_le_68, 'YearOfBirth'] + 2500
    df.loc[condition_2_digit_le_68, 'YearOfBirth_new'] = new_yob_2500.astype('Int64')
    df.loc[condition_2_digit_le_68, 'Age_01_new'] = (current_year_th - new_yob_2500).astype('Int64')

    # จัดการ YearOfBirth ที่ไม่สมบูรณ์ (พ.ศ. 25xx)
    condition_main = df['Age_01_new'].isna() & df['MonthOfBirth'].isna() & df[
        'YearOfBirth'].notna() & condition_to_process
    condition_thai_era = (df['YearOfBirth'] >= 2453) & (df['YearOfBirth'] <= 2568)
    df.loc[condition_main & condition_thai_era, 'MonthOfBirth_new'] = 6
    df.loc[condition_main & condition_thai_era, 'Age_01_new'] = (
            current_year_th - df.loc[condition_main & condition_thai_era, 'YearOfBirth'].round()
    ).astype('Int64')

    return df


def handle_special_yob_cases(df):
    """
    จัดการกรณีพิเศษที่ต้องมีการกำหนดค่าก่อนการคำนวณอื่น ๆ ทั้งหมด
    """
    # YearOfBirth = 9999 และมีค่า Age_01
    condition_9999_with_age = (df['YearOfBirth'] == 9999) & df['Age_01'].notna()
    # **แก้ไข:** ปัดเศษค่าทศนิยมก่อนแปลงเป็น 'Int64' เพื่อป้องกัน TypeError
    df.loc[condition_9999_with_age, 'Age_01_new'] = df.loc[condition_9999_with_age, 'Age_01'].round(0).astype('Int64')

    # YearOfBirth = 9999 และไม่มีค่า Age_01
    condition_9999_and_no_age = (df['YearOfBirth'] == 9999) & df['Age_01'].isna()
    df.loc[condition_9999_and_no_age, 'Age_01_new'] = 999

    # YearOfBirth = 2568 และ MonthOfBirth >= 4
    condition_yob_2568_and_mob_ge_04 = (df['YearOfBirth'] == 2568) & (df['MonthOfBirth'] >= 4)
    df.loc[condition_yob_2568_and_mob_ge_04, 'Age_01_new'] = 0

    return df


def calculate_age_and_impute():
    engine = None
    conn = None

    # 1. เชื่อมต่อและดึงข้อมูลจาก SQL Server
    try:
        encoded_sql_conn_str = quote_plus(sql_conn_str)
        engine = create_engine(f'mssql+pyodbc:///?odbc_connect={encoded_sql_conn_str}')
        conn = engine.connect()
        query = "SELECT * FROM r_additional WHERE ProvCode = ?"
        df = pd.read_sql_query(query, conn, params=(ProvCode,))

        # ตรวจสอบว่า DataFrame ว่างเปล่าหรือไม่
        if df.empty:
            print(f"ไม่พบข้อมูลสำหรับ ProvCode = {ProvCode}")
            return

        print("จำนวนข้อมูลที่นำเข้า:", df.shape)

    except Exception as ex:
        print(f"เกิดข้อผิดพลาดในการเชื่อมต่อหรือดึงข้อมูล: {ex}")
        return
    finally:
        if conn:
            conn.close()

    # 2. สำรองข้อมูลเดิมและแปลงประเภทข้อมูลให้ปลอดภัย
    df['MonthOfBirth_Old'] = pd.to_numeric(df['MonthOfBirth'], errors='coerce')
    df['YearOfBirth_Old'] = pd.to_numeric(df['YearOfBirth'], errors='coerce')
    df['Age_01_Old'] = pd.to_numeric(df['Age_01'], errors='coerce')
    df['IDEN_Old'] = df['IDEN']

    # 3. จัดการค่าที่ไม่ใช่ตัวเลขให้เป็น NaN และปัดเศษก่อนแปลงเป็น Int64
    df['NationalityNumeric'] = pd.to_numeric(df['NationalityNumeric'], errors='coerce').astype('Int64')
    df['MonthOfBirth'] = pd.to_numeric(df['MonthOfBirth'], errors='coerce').astype('Int64')
    df['YearOfBirth'] = pd.to_numeric(df['YearOfBirth'], errors='coerce').astype('Int64')
    df['Age_01'] = pd.to_numeric(df['Age_01'], errors='coerce').round(0).astype('Int64')

    # 4. จัดการกรณีพิเศษ
    df = handle_special_yob_cases(df)

    # 5. กำหนดเงื่อนไขแถวที่ไม่ใช่กรณีพิเศษ
    special_cases_condition = (df['YearOfBirth_Old'] == 9999) | (
            (df['YearOfBirth_Old'] == 2568) & (df['MonthOfBirth_Old'] >= 4))

    # 6. ประมวลผลทั่วไป
    df = handle_general_imputation(df, ~special_cases_condition)

    # 7. จัดรูปแบบผลลัพธ์สุดท้าย
    df['Age_01'] = df['Age_01'].astype(str).str.zfill(3).replace('<NA>', '')
    #df['MonthOfBirth'] = df['MonthOfBirth'].astype(str).str.zfill(2).replace('<NA>', '')
    df['MonthOfBirth'] = df['MonthOfBirth'].astype(str).str.zfill(2).replace('<NA>', '').replace('00', '')
    df['YearOfBirth'] = df['YearOfBirth'].astype(str).replace('<NA>', '')
    df['NationalityNumeric'] = df['NationalityNumeric'].astype(str).replace('<NA>', '')

    # พิมพ์ผลลัพธ์สำหรับตรวจสอบ
    print(df[['MonthOfBirth', 'YearOfBirth', 'Age_01',
              'NationalityNumeric']])

    # 8. อัปเดตข้อมูลกลับไปยังฐานข้อมูล
    print("กำลังอัปเดตข้อมูลกลับไปยังฐานข้อมูล...")
    conn = None
    try:
        conn = pyodbc.connect(sql_conn_str)
        cursor = conn.cursor()
        sql = """
            UPDATE r_additional
            SET MonthOfBirth = ?, YearOfBirth = ?, Age_01 = ?
            WHERE IDEN = ? AND ProvCode = ?
        """
        for index, row in df.iterrows():
            cursor.execute(sql, (
                row['MonthOfBirth'],
                row['YearOfBirth'],
                row['Age_01'],
                row['IDEN'],
                ProvCode
            ))
        conn.commit()
        print("การอัปเดตข้อมูลเสร็จสมบูรณ์")
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการอัปเดตข้อมูล: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
        if engine:
            engine.dispose()




if __name__ == '__main__':
    calculate_age_and_impute()

time_end = datetime.now()
print("เวลาสิ้นสุด:", time_end)
print("ความต่างของเวลา:", time_end - time_begin)