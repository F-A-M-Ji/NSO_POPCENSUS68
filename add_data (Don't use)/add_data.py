# import pandas as pd
# import numpy as np
# import pyodbc

# server = '10.0.72.110,1433'
# database = 'pop_edit_FAM'
# username = 'sa'
# password = 'Ict_2568'

# # สร้าง Connection String สำหรับ pyodbc
# connection_string = (
#     f"DRIVER={{ODBC Driver 17 for SQL Server}};"
#     f"SERVER={server};"
#     f"DATABASE={database};"
#     f"UID={username};"
#     f"PWD={password};"
# )

# # คอลัมน์ที่ต้องการให้เป็นชนิดข้อมูล string เพื่อป้องกันการตัด 0 นำหน้า
# cols_as_str = ['RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'VilCode', 'HouseNumber']

# sql_conn = None
# try:
#     # --- ส่วนที่ 2: เชื่อมต่อและดึงข้อมูลจากฐานข้อมูล ---
#     print("กำลังเชื่อมต่อกับฐานข้อมูล...")
#     sql_conn = pyodbc.connect(connection_string)
#     print("เชื่อมต่อสำเร็จ!")

#     # ดึงข้อมูลจากตาราง r_alldata_test
#     print("กำลังดึงข้อมูลจากตาราง r_alldata_test...")
#     query_data = "SELECT * FROM r_alldata_test"
#     data = pd.read_sql(query_data, sql_conn)
#     print(f"ดึงข้อมูล r_alldata_test สำเร็จ: {data.shape[0]} แถว")

#     # ดึงข้อมูลจากตาราง R_alldata_cpp_test
#     print("กำลังดึงข้อมูลจากตาราง R_alldata_cpp_test...")
#     query_data_cdd = "SELECT * FROM R_alldata_cpp_test"
#     data_cdd = pd.read_sql(query_data_cdd, sql_conn)
#     print(f"ดึงข้อมูล R_alldata_cpp_test สำเร็จ: {data_cdd.shape[0]} แถว")

#     # --- ส่วนที่ 3: จัดการประเภทข้อมูลให้เหมือนตอนอ่านจาก CSV ---
#     # แปลงคอลัมน์ที่ระบุให้เป็น string เพื่อให้ตรรกะเดิมทำงานได้ถูกต้อง
#     for col in cols_as_str:
#         if col in data.columns:
#             data[col] = data[col].fillna('').astype(str)
#         if col in data_cdd.columns:
#             data_cdd[col] = data_cdd[col].fillna('').astype(str)

#     cols_to_zfill = {
#         'RegCode': 1,
#         'ProvCode': 2,
#         'DistCode': 2,
#         'SubDistCode': 2,
#         'VilCode': 2
#     }

#     # จัดการทุกคอลัมน์ตามที่ระบุ
#     for col, width in cols_to_zfill.items():
#         if col in data.columns:
#             data[col] = data[col].apply(
#                 lambda x: '' if pd.isna(x) or str(x).strip() == '' else str(int(float(str(x).strip()))).zfill(width)
#             )

#     for col, width in cols_to_zfill.items():
#         if col in data.columns:
#             data_cdd[col] = data_cdd[col].apply(
#                 lambda x: '' if pd.isna(x) or str(x).strip() == '' else str(int(float(str(x).strip()))).zfill(width)
#             )

#     # จัดการ HouseNumber ให้เป็น string อย่างเดียว
#     if 'HouseNumber' in data.columns:
#         data['HouseNumber'] = data['HouseNumber'].fillna('').astype(str).str.strip()

#     if 'HouseNumber' in data.columns:
#         data_cdd['HouseNumber'] = data_cdd['HouseNumber'].fillna('').astype(str).str.strip()


#     data['Household_No_new'] = data['Household_No']
#     mask_blank = data['Household_No'].isna() | (data['Household_No'].astype(str).str.strip() == '')

#     # ตรวจสอบว่ามีแถวที่ต้องเติมค่าหรือไม่
#     if mask_blank.sum() > 0:
#         data.loc[mask_blank, 'Household_No_new'] = list(range(1, mask_blank.sum() + 1))

#     data = data.sort_values(by=['NsoBuilding_No','Building_No','RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode','MunTaoCode','VilCode',
#                                 'EA_Code_15', 'Household_No','Household_No_new', 'HouseNumber','RoomNumber','HouseholdMemberNumber'], ascending=True).reset_index(drop=True)

#     data_cdd = data_cdd.sort_values(by=['RegCode','ProvCode','DistCode', 'SubDistCode', 'VilCode', 'HouseNumber', 'Household_No','Population_No'], ascending=True).reset_index(drop=True)
    
#     df = data.copy().reset_index(drop=True)
#     df_cdd = data_cdd.copy().reset_index(drop=True)

#     key_columns_df = ['RegCode','ProvCode','DistCode','SubDistCode','VilCode','HouseNumber']
#     df['key_HH_df'] = df[key_columns_df].astype(str).agg('|'.join, axis=1)

#     key_columns_df_cdd = ['RegCode','ProvCode','DistCode','SubDistCode','VilCode','HouseNumber']
#     df_cdd['key_HH_df_cdd'] = df_cdd[key_columns_df_cdd].astype(str).agg('|'.join, axis=1)

#     df_HH = df.groupby('Household_No_new')[['key_HH_df']].first().reset_index()
#     df_cdd_HH = df_cdd.groupby('Household_No')[['key_HH_df_cdd']].first().reset_index()

#     df_cdd_Add = df_cdd_HH[~df_cdd_HH['key_HH_df_cdd'].isin(df_HH['key_HH_df'])]


#     merged_df = pd.merge(df_cdd, df_cdd_Add[['Household_No']], on='Household_No', how='left', indicator=True)
#     df_cdd['Merge_Flag'] = (merged_df['_merge'] == 'both').astype(int).reset_index(drop=True)
#     df_cdd = df_cdd[df_cdd['Merge_Flag'] == 1]
#     df_cdd_final = df_cdd.copy().reset_index(drop=True)

#     df_cdd_final.drop(columns=['key_HH_df_cdd','Merge_Flag'], inplace=True)
#     df.drop(columns=['Household_No_new','key_HH_df'], inplace=True)


#     df_cdd_final['DataSources'] = 4


#     df_final_2 = pd.concat([df, df_cdd_final], axis=0, ignore_index=True)
#     df_final_2['HouseNumber'] = df_final_2['HouseNumber'].apply(lambda x: f'="{x}"')

#     # ชื่อตารางใหม่ที่คุณต้องการสร้างในฐานข้อมูล
#     new_table_name = 'r_alldata_final_result' 

#     print(f"กำลังบันทึกผลลัพธ์ลงตารางใหม่ '{new_table_name}' ในฐานข้อมูล...")

#     # ใช้ .to_sql เพื่อส่งข้อมูลจาก DataFrame ไปยัง SQL Server
#     df_final_2.to_sql(
#         name=new_table_name, 
#         con=sql_conn, 
#         if_exists='replace', # 'replace' คือลบตารางเก่า (ถ้ามี) แล้วสร้างใหม่, 'append' คือต่อท้าย, 'fail' คือ error ถ้ามีตารางอยู่แล้ว
#         index=False, # ไม่ต้องเอา index ของ DataFrame ไปสร้างเป็นคอลัมน์ในตาราง
#         chunksize=1000 # ช่วยจัดการข้อมูลขนาดใหญ่โดยการส่งข้อมูลทีละ 1000 แถว
#     )

#     print("บันทึกข้อมูลลงตารางในฐานข้อมูลสำเร็จ!")

# except pyodbc.Error as ex:
#     sqlstate = ex.args[0]
#     print(f"เกิดข้อผิดพลาดในการเชื่อมต่อฐานข้อมูล: {sqlstate}")
#     print(ex)
# except Exception as e:
#     print(f"เกิดข้อผิดพลาดที่ไม่คาดคิด: {e}")
# finally:
#     if sql_conn:
#         sql_conn.close()
#         print("ปิดการเชื่อมต่อฐานข้อมูลแล้ว")















import pandas as pd
import numpy as np
# ไม่จำเป็นต้อง import pyodbc โดยตรงแล้ว SQLAlchemy จะจัดการให้
from sqlalchemy import create_engine
import urllib

# --- 1. ตั้งค่าการเชื่อมต่อ (เหมือนเดิม) ---
server = '10.0.72.110,1433'
database = 'pop_edit_FAM'
username = 'sa'
password = 'Ict_2568'

# --- 2. สร้าง Connection Engine ด้วย SQLAlchemy (แก้ไข) ---
# จัดการกับอักขระพิเศษในรหัสผ่าน (ถ้ามี)
quoted_password = urllib.parse.quote_plus(password)

# สร้าง Connection URL สำหรับ SQLAlchemy
connection_url = (
    f"mssql+pyodbc://{username}:{quoted_password}@{server}/{database}"
    f"?driver=ODBC+Driver+17+for+SQL+Server"
)

# สร้าง Engine ซึ่งเป็นตัวกลางในการเชื่อมต่อ
engine = create_engine(connection_url)

print("สร้าง Engine สำหรับเชื่อมต่อฐานข้อมูลสำเร็จ!")

try:
    # --- 3. ดึงข้อมูลโดยใช้ engine ---
    print("กำลังดึงข้อมูลจากตาราง r_alldata_test...")
    # แนะนำ: เพื่อความเร็วสูงสุด ควรเลือกเฉพาะคอลัมน์ที่จำเป็นแทน SELECT *
    query_data = "SELECT * FROM r_alldata_test"
    data = pd.read_sql(query_data, engine)
    print(f"ดึงข้อมูล r_alldata_test สำเร็จ: {data.shape[0]} แถว")

    print("กำลังดึงข้อมูลจากตาราง R_alldata_cpp_test...")
    query_data_cdd = "SELECT * FROM R_alldata_cpp_test"
    data_cdd = pd.read_sql(query_data_cdd, engine)
    print(f"ดึงข้อมูล R_alldata_cpp_test สำเร็จ: {data_cdd.shape[0]} แถว")

    # --- 4. จัดการประเภทข้อมูล ---
    cols_as_str = ['RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'VilCode', 'HouseNumber']
    for col in cols_as_str:
        if col in data.columns:
            data[col] = data[col].fillna('').astype(str)
        if col in data_cdd.columns:
            data_cdd[col] = data_cdd[col].fillna('').astype(str)

    cols_to_zfill = {
        'RegCode': 1, 'ProvCode': 2, 'DistCode': 2, 'SubDistCode': 2, 'VilCode': 2
    }

    # --- 5. ปรับปรุงความเร็ว: zfill แบบ Vectorized (แก้ไข) ---
    for col, width in cols_to_zfill.items():
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(width)
        # --- แก้ไข Bug: เปลี่ยนเป็น data_cdd.columns ---
        if col in data_cdd.columns:
            data_cdd[col] = pd.to_numeric(data_cdd[col], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(width)

    # จัดการ HouseNumber (ส่วนนี้ยังคงเดิม)
    if 'HouseNumber' in data.columns:
        data['HouseNumber'] = data['HouseNumber'].fillna('').astype(str).str.strip()
    if 'HouseNumber' in data_cdd.columns:
        data_cdd['HouseNumber'] = data_cdd['HouseNumber'].fillna('').astype(str).str.strip()

    # --- ส่วนของการประมวลผลข้อมูล (Logic) เหมือนเดิม ---
    data['Household_No_new'] = data['Household_No']
    mask_blank = data['Household_No'].isna() | (data['Household_No'].astype(str).str.strip() == '')
    if mask_blank.sum() > 0:
        data.loc[mask_blank, 'Household_No_new'] = list(range(1, mask_blank.sum() + 1))

    data = data.sort_values(by=['NsoBuilding_No','Building_No','RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode','MunTaoCode','VilCode','EA_Code_15', 'Household_No','Household_No_new', 'HouseNumber','RoomNumber','HouseholdMemberNumber'], ascending=True).reset_index(drop=True)
    data_cdd = data_cdd.sort_values(by=['RegCode','ProvCode','DistCode', 'SubDistCode', 'VilCode', 'HouseNumber', 'Household_No','Population_No'], ascending=True).reset_index(drop=True)

    df = data.copy().reset_index(drop=True)
    df_cdd = data_cdd.copy().reset_index(drop=True)

    key_columns = ['RegCode','ProvCode','DistCode','SubDistCode','VilCode','HouseNumber']
    
    # ปรับปรุงความเร็ว: สร้าง key แบบ Vectorized
    df['key_HH_df'] = df[key_columns].astype(str).agg('|'.join, axis=1)
    df_cdd['key_HH_df_cdd'] = df_cdd[key_columns].astype(str).agg('|'.join, axis=1)

    df_HH = df.groupby('Household_No_new')[['key_HH_df']].first().reset_index()
    df_cdd_HH = df_cdd.groupby('Household_No')[['key_HH_df_cdd']].first().reset_index()
    df_cdd_Add = df_cdd_HH[~df_cdd_HH['key_HH_df_cdd'].isin(df_HH['key_HH_df'])]

    merged_df = pd.merge(df_cdd, df_cdd_Add[['Household_No']], on='Household_No', how='left', indicator=True)
    df_cdd['Merge_Flag'] = (merged_df['_merge'] == 'both').astype(int).reset_index(drop=True)
    df_cdd = df_cdd[df_cdd['Merge_Flag'] == 1]
    df_cdd_final = df_cdd.copy().reset_index(drop=True)

    df_cdd_final.drop(columns=['key_HH_df_cdd','Merge_Flag'], inplace=True)
    df.drop(columns=['Household_No_new','key_HH_df'], inplace=True)
    df_cdd_final['DataSources'] = 4
    
    df_final_2 = pd.concat([df, df_cdd_final], axis=0, ignore_index=True)
    
    # --- 6. ลบโค้ดที่ไม่จำเป็น (แก้ไข) ---
    # บรรทัดนี้ไม่จำเป็นสำหรับการบันทึกลง DB
    # df_final_2['HouseNumber'] = df_final_2['HouseNumber'].apply(lambda x: f'="{x}"')

    # --- 7. บันทึกข้อมูลลง DB โดยใช้ engine ---
    new_table_name = 'r_alldata_final_result'
    print(f"กำลังบันทึกผลลัพธ์ลงตารางใหม่ '{new_table_name}' ในฐานข้อมูล...")
    df_final_2.to_sql(
        name=new_table_name,
        con=engine,
        if_exists='replace',
        index=False,
        chunksize=10000 # เพิ่ม chunksize เพื่อจัดการข้อมูลขนาดใหญ่
    )
    print("บันทึกข้อมูลลงตารางในฐานข้อมูลสำเร็จ!")

except Exception as e:
    print(f"เกิดข้อผิดพลาดที่ไม่คาดคิด: {e}")

print("โปรแกรมทำงานเสร็จสิ้น")