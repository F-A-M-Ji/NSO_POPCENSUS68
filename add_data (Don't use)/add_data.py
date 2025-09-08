import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import urllib
import time
from datetime import datetime

server = '10.0.72.110,1433'
database = 'pop_edit_FAM'
username = 'sa'
password = 'Ict_2568'

provinces_to_process = [
    '18'
]

target_table = 'r_alldata_test2'
report_filename = 'report.csv'
report_log = []

try:
    print(f"\n{'='*70}")
    print(f"\n{'='*10} เริ่มการทำงาน {'='*10}")
    print(f"\n{'='*70}")
    print("กำลังสร้างการเชื่อมต่อกับฐานข้อมูล...")
    quoted_password = urllib.parse.quote_plus(password)
    connection_url = (
        f"mssql+pyodbc://{username}:{quoted_password}@{server}/{database}"
        f"?driver=ODBC+Driver+17+for+SQL+Server"
    )
    engine = create_engine(connection_url, fast_executemany=True)
    print("สร้าง Engine สำหรับเชื่อมต่อฐานข้อมูลสำเร็จ (โหมด fast_executemany' ถูกเปิดใช้งาน)!")

    print(f"กำลังดึง IDEN ที่มีอยู่ทั้งหมดจากตาราง '{target_table}' เพื่อตรวจสอบความซ้ำซ้อน...")
    all_existing_idens_query = f"SELECT IDEN FROM {target_table} WHERE IDEN IS NOT NULL"
    idens_df = pd.read_sql(all_existing_idens_query, engine)
    existing_idens = set(idens_df['IDEN'])
    print(f"พบ IDEN ที่มีอยู่แล้ว {len(existing_idens)} รายการ")
    
    total_new_rows_all_provinces = 0

    for prov_code in provinces_to_process:
        start_time = datetime.now() 
        print(f"\n{'='*30} เริ่มกระบวนการสำหรับจังหวัด: {prov_code} {'='*30}")
        print(f"\nเวลาเริ่มต้น: '{start_time}'")

        print(f"[{prov_code}] กำลังดึงข้อมูลจากตาราง {target_table}...")
        query_data = f"SELECT * FROM {target_table} WHERE ProvCode = '{prov_code}'"
        data = pd.read_sql(query_data, engine)
        original_row_count = len(data)
        
        print(f"[{prov_code}] กำลังดึงข้อมูลจากตาราง R_alldata_cpp_test...")
        query_data_cdd = f"SELECT * FROM R_alldata_cpp_test WHERE ProvCode = '{prov_code}'"
        data_cdd = pd.read_sql(query_data_cdd, engine)

        if data_cdd.empty:
            print(f"[{prov_code}] ไม่พบข้อมูลใน R_alldata_cpp_test สำหรับจังหวัดนี้ ข้ามไปจังหวัดถัดไป...")
            continue
        if data.empty:
            print(f"[{prov_code}] ไม่พบข้อมูลตั้งต้นใน {target_table} สำหรับจังหวัดนี้ จะทำการเพิ่มข้อมูลใหม่ทั้งหมด")

        cols_as_str = ['RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'VilCode', 'HouseNumber']
        for col in cols_as_str:
            if col in data.columns: data[col] = data[col].fillna('').astype(str)
            if col in data_cdd.columns: data_cdd[col] = data_cdd[col].fillna('').astype(str)
        
        cols_to_zfill = {'RegCode': 1, 'ProvCode': 2, 'DistCode': 2, 'SubDistCode': 2, 'VilCode': 2}
        for col, width in cols_to_zfill.items():
            if col in data.columns: data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(width)
            if col in data_cdd.columns: data_cdd[col] = pd.to_numeric(data_cdd[col], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(width)

        if 'HouseNumber' in data.columns: data['HouseNumber'] = data['HouseNumber'].fillna('').astype(str).str.strip()
        if 'HouseNumber' in data_cdd.columns: data_cdd['HouseNumber'] = data_cdd['HouseNumber'].fillna('').astype(str).str.strip()

        if not data.empty:
            data['Household_No_new'] = data['Household_No']
            mask_blank = data['Household_No'].isna() | (data['Household_No'].astype(str).str.strip() == '')
            if mask_blank.sum() > 0:
                data.loc[mask_blank, 'Household_No_new'] = list(range(1, mask_blank.sum() + 1))
        
        if not data.empty:
            data = data.sort_values(by=['NsoBuilding_No','Building_No','RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode','MunTaoCode','VilCode','EA_Code_15', 'Household_No','Household_No_new', 'HouseNumber','RoomNumber','HouseholdMemberNumber'], ascending=True).reset_index(drop=True)
        data_cdd = data_cdd.sort_values(by=['RegCode','ProvCode','DistCode', 'SubDistCode', 'VilCode', 'HouseNumber', 'Household_No','Population_No'], ascending=True).reset_index(drop=True)

        print(f"[{prov_code}] กำลังเปรียบเทียบเพื่อหาข้อมูลที่ต้องเพิ่มใหม่...")
        df = data.copy()
        df_cdd = data_cdd.copy()

        key_columns = ['RegCode','ProvCode','DistCode','SubDistCode','VilCode','HouseNumber']
        df_cdd['key_HH_df_cdd'] = df_cdd[key_columns].astype(str).agg('|'.join, axis=1)

        if not df.empty:
            df['key_HH_df'] = df[key_columns].astype(str).agg('|'.join, axis=1)
            df_HH = df.groupby('Household_No_new')[['key_HH_df']].first().reset_index()
            df_cdd_HH = df_cdd.groupby('Household_No')[['key_HH_df_cdd']].first().reset_index()
            df_cdd_Add = df_cdd_HH[~df_cdd_HH['key_HH_df_cdd'].isin(df_HH['key_HH_df'])]
            merged_df = pd.merge(df_cdd, df_cdd_Add[['Household_No']], on='Household_No', how='left', indicator=True)
            df_cdd['Merge_Flag'] = (merged_df['_merge'] == 'both')
        else:
            df_cdd['Merge_Flag'] = True

        df_cdd_final = df_cdd[df_cdd['Merge_Flag']].copy()
        df_cdd_final.drop(columns=['key_HH_df_cdd','Merge_Flag'], inplace=True)
        df_cdd_final.reset_index(drop=True, inplace=True)
        df_cdd_final['DataSources'] = 4
        new_row_count = len(df_cdd_final)

        if not df_cdd_final.empty:
            print(f"[{prov_code}] พบข้อมูลใหม่ {len(df_cdd_final)} แถวที่ต้องเพิ่มและสร้าง IDEN")
            total_new_rows_all_provinces += len(df_cdd_final)

            def generate_random_part(): return ''.join(np.random.choice(list('0123456789'), 16))

            new_iden_list = []
            generated_idens_in_this_batch = set()

            for index, row in df_cdd_final.iterrows():
                prefix = str(row['RegCode']) + str(row['ProvCode']) + str(row['DistCode']) + str(row['SubDistCode'])
                while True:
                    random_part = generate_random_part()
                    new_iden = prefix + random_part
                    if new_iden not in existing_idens and new_iden not in generated_idens_in_this_batch:
                        break
                new_iden_list.append(new_iden)
                generated_idens_in_this_batch.add(new_iden)

            df_cdd_final['IDEN'] = new_iden_list
            existing_idens.update(generated_idens_in_this_batch)
            
            print(f"[{prov_code}] สร้าง IDEN สำหรับข้อมูลใหม่ทั้งหมดสำเร็จ!")
            print(f"[{prov_code}] กำลังบันทึกข้อมูลใหม่ลงในตาราง '{target_table}'...")
            df_cdd_final.to_sql(
                name=target_table,
                con=engine,
                if_exists='append',
                index=False,
                chunksize=10000
            )
            print(f"[{prov_code}] เพิ่มข้อมูลใหม่ {len(df_cdd_final)} แถวลงในตาราง '{target_table}' สำเร็จ!")
        else:
            print(f"[{prov_code}] ไม่พบข้อมูลใหม่ที่ต้องเพิ่มสำหรับจังหวัดนี้")

        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        report_log.append({
            'ProvCode': prov_code,
            'จำนวนข้อมูลเดิม': original_row_count,
            'จำนวนข้อมูลใหม่': new_row_count,
            'จำนวนรวมทั้งหมด': original_row_count + new_row_count,
            'เวลาเริ่มต้น': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'เวลาสิ้นสุด': end_time.strftime('%Y-%m-%d %H:%M:%S'),
            'เวลาที่ใช้ (วินาที)': round(processing_time, 2)
        })

        print(f"\nเวลาสิ้นสุด: '{end_time}'")
        print(f"[{prov_code}] จังหวัดนี้ใช้เวลาประมวลผล: {processing_time:.2f} วินาที")

        if report_log:
            report_df = pd.DataFrame(report_log)
          
            # สร้างแถวสรุปรวมจากข้อมูลทั้งหมดที่เคยประมวลผลมา
            summary_row = {
                'ProvCode': 'Total',
                'จำนวนข้อมูลเดิม': report_df['จำนวนข้อมูลเดิม'].sum(),
                'จำนวนข้อมูลใหม่': report_df['จำนวนข้อมูลใหม่'].sum(),
                'จำนวนรวมทั้งหมด': report_df['จำนวนรวมทั้งหมด'].sum(),
                'เวลาเริ่มต้น': '-',
                'เวลาสิ้นสุด': '-',
                'เวลาที่ใช้ (วินาที)': round(report_df['เวลาที่ใช้ (วินาที)'].sum(), 2)
            }
            report_df_with_total = pd.concat([report_df, pd.DataFrame([summary_row])], ignore_index=True)
            
            report_df_with_total.to_csv(report_filename, index=False, encoding='utf-8-sig')
        
        print(f"\nรายงานสรุปผลถูกบันทึกเรียบร้อยแล้วในไฟล์: '{report_filename}'")

    print(f"\nประมวลผลเสร็จสิ้นทุกจังหวัดที่กำหนด")

except Exception as e:
    print(f"เกิดข้อผิดพลาดร้ายแรงที่ไม่คาดคิด: {e}")
finally:
    if 'engine' in locals():
        engine.dispose()
    print("โปรแกรมทำงานเสร็จสิ้น และปิดการเชื่อมต่อแล้ว")
