import warnings
import pandas as pd
import datetime
import pyodbc
import numpy as np
import os
import itertools

warnings.filterwarnings('ignore')
current_datetime_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

# ANSI escape codes สำหรับสีต่างๆ
GREEN = '\033[92m'
RED = '\033[91m'
BLUE = '\033[94m'
YELLOW = '\033[93m'
RESET = '\033[0m'
ORANGE = '\033[38;2;255;165;0m'
PINK = '\033[38;2;255;105;180m'

# --------------------------- แก้ array เป็นรหัสจังหวัด ---------------------------
PROVINCE_CODES_TO_RUN = ["85"]

report_data = []
report_columns = [
    'ProvCode', 'time_begin', 'time_end_check_dup', 'time_difference_check_dup', 'time_end', 'time_difference',
    'Shape of df_bmn', 'Shape of df_dopa', 'df_dopa_cut', 'df_bmn_key', 'df_dopa_key', 'merged_df', 'rows_from_dopa',
    'rows_to_expand', 'rows_to_keep', 'expanded_df', 'Shape of final_merged_df', 'Shape of df_filtered',
    'Shape of df_to_insert'
]

# --------------------------- แก้ที่อยู่ของ report.csv --------------------------
report_filename = 'report.csv'

sql_conn = None
cursor = None
try:
    # ------------------------------------ ที่ ip server ----------------------------------------
    sql_conn = pyodbc.connect('DRIVER={SQL Server};SERVER=192.168.0.204;DATABASE=pop6768;UID=pdan;PWD=P@ssw0rd12#$')
    print("Connection successful")
    cursor = sql_conn.cursor()

    for ProvCode in PROVINCE_CODES_TO_RUN:
        print(f"\n{BLUE}====================================================={RESET}")
        print(f"{BLUE}>>> เริ่มกระบวนการสำหรับ ProvCode: {ProvCode}{RESET}")
        print(f"{BLUE}====================================================={RESET}")

        time_begin = datetime.datetime.now()
        print("เวลาเริ่มต้น:", time_begin)

        # ดึงข้อมูลจาก r_additional
        query_bmn = f'''SELECT * FROM r_additional WHERE (ProvCode = '{ProvCode}') '''
        df_bmn = pd.read_sql(query_bmn, sql_conn)
        print("Shape of df_bmn:", df_bmn.shape)

        # ดึงข้อมูลจาก M_MOI_PROV โดยปรับชื่อตารางตามรหัสจังหวัด
        query_dopa = f'''SELECT * FROM m_moi_prov{ProvCode}'''
        df_dopa = pd.read_sql(query_dopa, sql_conn)
        print("Shape of df_dopa:", df_dopa.shape)


        # แปลงรหัสจังหวัด (cc_code) ให้เป็นรหัสภาค
        def get_region_code(cc_code):
            if pd.isna(cc_code):
                return np.nan
            cc_code = int(cc_code)
            if cc_code == 10:
                return 0
            elif (11 <= cc_code <= 19) or (70 <= cc_code <= 77):
                return 1
            elif (80 <= cc_code <= 86) or (90 <= cc_code <= 93):
                return 2
            elif (94 <= cc_code <= 96):
                return 3
            elif (20 <= cc_code <= 27):
                return 4
            elif (30 <= cc_code <= 49):
                return 5
            elif (50 <= cc_code <= 58) or (60 <= cc_code <= 67):
                return 6
            else:
                return np.nan


        # แปลง รหัสภาค ให้เป็น ชื่อภาค
        def get_region_name(reg_code):
            if pd.isna(reg_code):
                return np.nan
            reg_code = int(reg_code)
            if reg_code == 0:
                return 'กรุงเทพมหานคร'
            elif reg_code == 1:
                return 'ภาคกลาง'
            elif reg_code == 2:
                return 'ภาคใต้'
            elif reg_code == 3:
                return 'ภาคใต้ชายแดน'
            elif reg_code == 4:
                return 'ภาคตะวันออก'
            elif reg_code == 5:
                return 'ภาคตะวันออกเฉียงเหนือ'
            elif reg_code == 6:
                return 'ภาคเหนือ'
            else:
                return np.nan


        print(f"กำลังประมวลผลจังหวัด: {ProvCode}")

        # ประเภทสิ่งปลูกสร้างของกรมการปกครอง
        house_types_to_select = ['บ้าน', 'หอพัก', 'บ้านพักข้าราชการ', 'แพ', 'ทาวน์เฮ้าส์', 'ห้องแถว', 'อาคารชุด',
                                 'ตึก', 'ตึกแถว',
                                 'เรือ', 'บ้านแฝด', 'บ้านแถว', 'อาคารอเนกประสงค์', 'แฟลต', 'ค่ายทหาร',
                                 'คลังสินค้า-อยู่อาศัย']
        df_dopa = df_dopa[
            (df_dopa['HTYPE_NAME'].isin(house_types_to_select)) |
            (df_dopa['HTYPE_NAME'].isna())].copy()

        # เงื่อนไขสำหรับ "ท้องถิ่นเทศบาล"
        condition_area_1 = df_dopa['RCODE_NAME'].str.contains('ท้องถิ่นเทศบาล', na=False)
        df_dopa.loc[condition_area_1, 'AREA_CODE'] = 1
        df_dopa.loc[condition_area_1, 'AREA_NAME'] = 'ในเขตเทศบาล'

        # เงื่อนไขสำหรับ "อำเภอ"
        condition_area_2 = df_dopa['RCODE_NAME'].str.contains('อำเภอ', na=False)
        df_dopa.loc[condition_area_2, 'AREA_CODE'] = 2
        df_dopa.loc[condition_area_2, 'AREA_NAME'] = 'นอกเขตเทศบาล'

        # แปลง HTYPE_CODE เป็น building
        building_mapping = {1: 1, 28: 1, 46: 18, 39: 2, 45: 2, 18: 3, 22: 3, 21: 4, 16: 5, 17: 5, 49: 5, 14: 6,
                            3: 7, 4: 7, 6: 15, 40: 18, 41: 18}
        building_name = {1: 'บ้านเดี่ยว', 28: 'บ้านเดี่ยว', 46: 'บ้านเดี่ยว', 39: 'ทาวน์เฮาส์/บ้านแฝด/ทาวน์โฮม',
                         45: 'ทาวน์เฮาส์/บ้านแฝด/ทาวน์โฮม',
                         18: 'คอนโดมิเนียม/แมนชั่น', 22: 'คอนโดมิเนียม/แมนชั่น', 21: 'แฟลต/อพาร์ทเมนท์',
                         16: 'ตึกแถว/ห้องแถว/เรือนแถว',
                         17: 'ตึกแถว/ห้องแถว/เรือนแถว', 49: 'ตึกแถว/ห้องแถว/เรือนแถว', 14: 'หอพักทั่วไป',
                         3: 'เรือ/แพ/รถ', 4: 'เรือ/แพ/รถ',
                         6: 'กรมกองทหารหรือตำรวจ', 40: 'อื่น ๆ (ระบุ….)', 41: 'อื่น ๆ (ระบุ….)'}
        df_dopa['building'] = df_dopa['HTYPE_CODE'].map(building_mapping)
        df_dopa['buildingname'] = df_dopa['HTYPE_CODE'].map(building_name)

        # เงื่อนไขสำหรับ TOTAL_MEMBERS > 0
        condition_members_gt_0 = df_dopa['TOTAL_MEMBERS'] > 0
        df_dopa.loc[condition_members_gt_0, 'Household'] = 11

        # เงื่อนไขสำหรับ TOTAL_MEMBERS มีค่าว่างหรือ 0
        condition_members_na_or_0 = (df_dopa['TOTAL_MEMBERS'].isna()) | (df_dopa['TOTAL_MEMBERS'] == 0)
        df_dopa.loc[condition_members_na_or_0, 'Household'] = 13

        # แปลง dtype ของคอลัมน์ EA_Code_15 ให้เป็น object เพื่อป้องกัน Warning
        df_dopa['EA_Code_15'] = df_dopa['EA_Code_15'].astype(object)
        mask = df_dopa['EA_Code_15'].isna()
        region_codes = df_dopa.loc[mask, 'CC_CODE'].apply(get_region_code).astype(str).replace('.0', '', regex=False)
        tt_codes = df_dopa.loc[mask, 'TT_CODE'].fillna(0).astype(int).astype(str)
        area_codes = df_dopa.loc[mask, 'AREA_CODE'].fillna(0).astype(int).astype(str)
        mm_codes = df_dopa.loc[mask, 'MM'].fillna(0).astype(int).astype(str).str.slice(-2)
        df_dopa.loc[mask, 'EA_Code_15'] = region_codes + tt_codes + area_codes + '0' + mm_codes

        # สร้าง key สำหรับการ merge
        df_bmn['key'] = df_bmn['ProvCode'].astype(str).str.zfill(2) + df_bmn['DistCode'].astype(str).str.zfill(2) + \
                        df_bmn['SubDistCode'].astype(str).str.zfill(2) + df_bmn['VilCode'].astype(str).str.zfill(2) + \
                        df_bmn['HouseNumber'].astype(str)
        df_dopa['key'] = df_dopa['CC_CODE'].astype(str) + df_dopa['AA_CODE'].astype(str) + df_dopa['TT_CODE'].astype(
            str) + df_dopa['MM'].astype(str) + df_dopa['HNO'].astype(str)

        print(f"df_dopa_cut {df_dopa.shape}")
        print(f"df_bmn_key {df_bmn.shape}")
        print(f"df_dopa_key {df_dopa.shape}")

        # ใช้ how='outer' เพื่อให้ได้ข้อมูลทั้งหมดจากทั้งสองตาราง
        merged_df = pd.merge(df_bmn, df_dopa, on='key', how='outer', suffixes=('_bmn', '_dopa'), indicator=True)

        print(f"merged_df {merged_df.shape}")

        # --- ส่วนที่ปรับปรุง: การแยกข้อมูลที่ต้อง INSERT และข้อมูลที่ต้อง UPDATE ---
        # ข้อมูลที่ต้อง INSERT คือข้อมูลที่มาจาก df_dopa เท่านั้น (มีใน _merge = 'right_only')
        df_to_insert = merged_df[merged_df['_merge'] == 'right_only'].copy()
        print(f"Shape of df_to_insert before expansion: {df_to_insert.shape}")

        # ขยายแถวสำหรับข้อมูลที่มีคนในครัวเรือน > 0
        rows_to_expand = df_to_insert[df_to_insert['TOTAL_MEMBERS'] > 0]
        expanded_rows = []
        for _, row in rows_to_expand.iterrows():
            total_members = int(row['TOTAL_MEMBERS'])
            sex_list = []
            if 'TOTAL_THAI_M' in row and pd.notna(row['TOTAL_THAI_M']): sex_list.extend([1] * int(row['TOTAL_THAI_M']))
            if 'TOTAL_THAI_FM' in row and pd.notna(row['TOTAL_THAI_FM']): sex_list.extend(
                [2] * int(row['TOTAL_THAI_FM']))
            if 'TOTAL_NONTHAI_M' in row and pd.notna(row['TOTAL_NONTHAI_M']): sex_list.extend(
                [3] * int(row['TOTAL_NONTHAI_M']))
            if 'TOTAL_NONTHAI_FM' in row and pd.notna(row['TOTAL_NONTHAI_FM']): sex_list.extend(
                [4] * int(row['TOTAL_NONTHAI_FM']))
            while len(sex_list) < total_members: sex_list.append(np.nan)
            for sex_val in sex_list:
                new_row = row.copy()
                new_row['Residing'] = 1
                new_row['sex'] = sex_val
                expanded_rows.append(new_row)
        expanded_df = pd.DataFrame(expanded_rows)

        # ข้อมูลที่ไม่มีคนในครัวเรือน (TOTAL_MEMBERS == 0)
        rows_to_keep = df_to_insert[df_to_insert['TOTAL_MEMBERS'] == 0]

        # รวมข้อมูลที่ขยายและข้อมูลที่ไม่มีสมาชิกเข้าด้วยกัน
        df_to_insert = pd.concat([expanded_df, rows_to_keep], ignore_index=True)
        print(f"Shape of df_to_insert after expansion: {df_to_insert.shape}")

        # --- สร้าง IDEN และปรับปรุงค่าสำหรับ df_to_insert โดยเฉพาะ ---
        print(f"กำลังสร้าง IDEN")
        existing_idens_query = f"SELECT IDEN FROM r_additional WHERE ProvCode = '{ProvCode}'"
        cursor.execute(existing_idens_query)
        existing_idens = {row[0] for row in cursor.fetchall()}

        if not df_to_insert.empty:
            counter = 1
            for index, row in df_to_insert.iterrows():
                iden_base = str(row['EA_Code_15_dopa']) if pd.notna(row['EA_Code_15_dopa']) else ''
                new_iden = f"{iden_base}{str(counter).zfill(8)}"
                while new_iden in existing_idens:
                    counter += 1
                    new_iden = f"{iden_base}{str(counter).zfill(8)}"
                df_to_insert.loc[index, 'IDEN'] = new_iden
                existing_idens.add(new_iden)
                counter += 1

        # กำหนดค่า DataSources เป็น 5 สำหรับข้อมูลที่จะ INSERT
        df_to_insert['DataSources'] = 5

        # แปลงข้อมูลใน df_to_insert ให้ตรงกับ schema ของ r_additional
        df_to_insert['EA_Code_15'] = df_to_insert['EA_Code_15_dopa']
        df_to_insert['NsoBuilding_No'] = df_to_insert['NsoBuilding_No_dopa']
        df_to_insert['ProvCode'] = df_to_insert['CC_CODE']
        df_to_insert['ProvName'] = df_to_insert['CC_NAME']
        df_to_insert['DistCode'] = df_to_insert['AA_CODE']
        df_to_insert['SubDistCode'] = df_to_insert['TT_CODE']
        df_to_insert['VilCode'] = df_to_insert['MM']
        df_to_insert['DistName'] = df_to_insert['AA_NAME']
        df_to_insert['SubDistName'] = df_to_insert['TT_NAME']
        df_to_insert['RoadName'] = df_to_insert['THANON']
        df_to_insert['AlleyWayName'] = df_to_insert['TROK']
        df_to_insert['AlleyName'] = df_to_insert['SOI']
        df_to_insert['HouseNumber'] = df_to_insert['HNO'].astype(str)
        df_to_insert['VilName'] = df_to_insert['MM_NAME']
        df_to_insert['RegCode'] = df_to_insert['CC_CODE'].apply(get_region_code)
        df_to_insert['RegName'] = df_to_insert['RegCode'].apply(get_region_name)
        df_to_insert['AreaCode'] = df_to_insert['AREA_CODE']
        df_to_insert['AreaName'] = df_to_insert['AREA_NAME']
        df_to_insert['BuildingType'] = df_to_insert['building']
        df_to_insert['BuildingName'] = df_to_insert['buildingname']
        df_to_insert['HouseholdEnumeration'] = df_to_insert['Household']
        df_to_insert['NationalityNumeric'] = np.nan
        condition_thai = df_to_insert['sex'].isin([1, 2])
        df_to_insert.loc[condition_thai, 'NationalityNumeric'] = 764
        condition_thai = df_to_insert['sex'].isin([3, 4])
        df_to_insert.loc[condition_thai, 'NationalityNumeric'] = 996
        df_to_insert['sex'] = df_to_insert['sex'].replace({3: 1, 4: 2})
        df_to_insert['Sex'] = df_to_insert['sex']

        # เลือกคอลัมน์ที่ต้องการสำหรับ df_to_insert
        selected_columns = [
            'IDEN', 'EA_Code_15', 'Building_No', 'BuildingCode', 'NsoBuilding_No', 'RegCode', 'RegName', 'ProvCode',
            'ProvName', 'DistCode', 'DistName', 'SubDistCode', 'SubDistName', 'AreaCode', 'AreaName', 'MuniCode',
            'MuniName', 'SubAdminCode', 'SubAdminName', 'MunTaoCode', 'MunTaoName', 'CommuCode', 'CommuName',
            'EA_No', 'VilCode', 'VilName', 'HouseNumber', 'RoomNumber', 'RoadName', 'AlleyWayName', 'AlleyName',
            'BuildingName', 'BuildingNumber', 'BuildingType', 'BuildingTypeOther', 'Residing', 'HouseholdEnumeration',
            'HouseholdEnumerationOther', 'HouseholdEnumeration_1', 'HouseholdEnumeration_2', 'HouseholdEnumeration_3',
            'HouseholdType', 'NumberOfHousehold', 'TotalRoom', 'RoomVacant', 'RoomResidence', 'NewBuilding',
            'BuildingStatus', 'IsBuilding', 'Building_IsActive', 'Building_Note', 'Building_CrtdByCode',
            'Building_CrtdByName', 'TotalPopulation', 'TotalMale', 'TotalFemale', 'ApproveStatusSupCode', 'IsMapping',
            'Building_CrtdDateStart', 'Building_CrtdDateEnd', 'Building_UpdateDateStart', 'Building_UpdateDateEnd',
            'Household_No', 'Building_No_HH', 'HouseholdNumber', 'HouseholdEnumeration_HH', 'ConstructionMaterial',
            'ConstructionMaterialOther', 'TenureResidence', 'TenureResidenceOther', 'TenureLand', 'TenureLandOther',
            'NumberOfHousueholdMember', 'HouseholdStatus', 'Household_IsActive', 'Household_Note',
            'Household_CrtdByCode',
            'Household_CrtdByName', 'Household_CrtdDateStart', 'Household_CrtdDateEnd', 'Household_UpdateDateStart',
            'Household_UpdateDateEnd', 'Population_No', 'Building_No_POP', 'Household_No_POP', 'HouseholdMemberNumber',
            'Title', 'TitleOther', 'FirstName', 'LastName', 'Relationship', 'Sex', 'MonthOfBirth', 'YearOfBirth',
            'Age_01',
            'NationalityNumeric', 'EducationalAttainment', 'EmploymentStatus', 'NameInHouseholdRegister',
            'NameInHouseholdRegisterOther', 'DurationOfResidence', 'DurationOfResidence_Text',
            'MigrationCharecteristics',
            'MovedFromProvince', 'MovedFromAbroad', 'MigrationReason', 'MigrationReasonOther', 'Gender',
            'PopulationStatus',
            'Population_IsActive', 'Population_Note', 'Longitude_POP', 'Population_CrtdByCode', 'Population_CrtdByName',
            'Population_CrtdDateStart',
            'Population_CrtdDateEnd', 'Population_UpdateDateStart', 'Population_UpdateDateEnd', 'ApproveStatusSup',
            'ApproveDateSup',
            'ApproveStatusProv', 'ApproveDateProv', 'm_control_ea_CrtdByCode', 'ADV_EA', 'InternetAr', 'DataSources'
        ]

        df_to_insert = df_to_insert.reindex(columns=selected_columns)
        print("Shape of df_to_insert for insertion:", df_to_insert.shape)

        # --- โค้ดที่ปรับปรุง: การจัดการค่าว่างและจำกัดความยาวของข้อมูลก่อนการ INSERT ---
        print("กำลังตรวจสอบและแก้ไขค่าวันที่/เวลา (NaT) และจำกัดความยาวข้อมูล...")
        max_lengths = {
            'IDEN': 23, 'EA_Code_15': 15, 'Building_No': 50, 'BuildingCode': 50, 'NsoBuilding_No': 50,
            'RegCode': 1, 'RegName': 255, 'ProvCode': 2, 'ProvName': 255, 'DistCode': 2, 'DistName': 255,
            'SubDistCode': 2, 'SubDistName': 255, 'AreaCode': 1, 'AreaName': 100, 'MuniCode': 3,
            'MuniName': 200, 'SubAdminCode': 3, 'SubAdminName': 200, 'MunTaoCode': 3, 'MunTaoName': 200,
            'CommuCode': 20, 'CommuName': 255, 'EA_No': 4, 'VilCode': 2, 'VilName': 255, 'HouseNumber': 50,
            'RoomNumber': 50, 'RoadName': 50, 'AlleyWayName': 50, 'AlleyName': 50, 'BuildingName': 255,
            'BuildingNumber': 4, 'BuildingType': 2, 'BuildingTypeOther': 50, 'Residing': 1,
            'HouseholdEnumeration': 2, 'HouseholdEnumerationOther': 255, 'HouseholdEnumeration_1': 2,
            'HouseholdEnumeration_2': 2, 'HouseholdEnumeration_3': 2, 'HouseholdType': 1, 'TotalRoom': 4,
            'RoomVacant': 4, 'RoomResidence': 4, 'Building_IsActive': 1, 'Building_Note': 255,
            'Building_CrtdByCode': 8, 'Building_CrtdByName': 100, 'TotalPopulation': 4, 'TotalMale': 4,
            'TotalFemale': 4, 'ApproveStatusSupCode': 1, 'Household_No': 50, 'Building_No_HH': 50,
            'HouseholdNumber': 4, 'ConstructionMaterial': 1, 'ConstructionMaterialOther': 50,
            'TenureResidence': 1, 'TenureResidenceOther': 30, 'TenureLand': 1, 'TenureLandOther': 255,
            'Household_IsActive': 1, 'Household_Note': 255, 'Household_CrtdByCode': 8,
            'Household_CrtdByName': 100, 'Population_No': 50, 'Building_No_POP': 50, 'Household_No_POP': 50,
            'HouseholdMemberNumber': 5, 'Title': 2, 'TitleOther': 50, 'FirstName': 255, 'LastName': 255,
            'Relationship': 2, 'Sex': 1, 'MonthOfBirth': 2, 'YearOfBirth': 4, 'Age_01': 3,
            'NationalityNumeric': 3, 'EducationalAttainment': 2, 'EmploymentStatus': 1,
            'NameInHouseholdRegister': 1, 'NameInHouseholdRegisterOther': 2, 'DurationOfResidence': 1,
            'DurationOfResidence_Text': 255, 'MigrationCharecteristics': 1, 'MovedFromProvince': 2,
            'MovedFromAbroad': 3, 'MigrationReason': 1, 'MigrationReasonOther': 255, 'Gender': 1,
            'PopulationStatus': 1, 'Population_IsActive': 1, 'Population_Note': 255, 'Longitude_POP': 1,
            'Population_CrtdByCode': 8, 'Population_CrtdByName': 100, 'ApproveStatusSup': 1,
            'ApproveStatusProv': 1, 'm_control_ea_CrtdByCode': 255, 'ADV_EA': 1, 'InternetAr': 1,
            'DataSources': 1, 'MonthOfBirth_new': 2, 'YearOfBirth_new': 4, 'Age_01_new': 3
        }

        # การจัดการค่าว่างตามชนิดข้อมูล
        for col in df_to_insert.columns:
            # สำหรับคอลัมน์ที่เป็นข้อความ ให้แทนที่ NaN ด้วยสตริงว่าง ('')
            if df_to_insert[col].dtype == 'object':
                df_to_insert[col] = df_to_insert[col].replace({np.nan: '', pd.NA: ''}).astype(str).str.slice(0, max_lengths.get(col, 255))
            # สำหรับคอลัมน์ที่เป็นตัวเลข ให้แทนที่ NaN ด้วย None (ซึ่งจะกลายเป็น NULL ใน SQL)
            elif pd.api.types.is_numeric_dtype(df_to_insert[col]):
                df_to_insert[col] = df_to_insert[col].replace({np.nan: None, pd.NA: None})
            # สำหรับคอลัมน์ที่เป็นวันที่ ให้แทนที่ NaT ด้วย None (ซึ่งจะกลายเป็น NULL ใน SQL)
            elif pd.api.types.is_datetime64_any_dtype(df_to_insert[col]):
                df_to_insert[col] = df_to_insert[col].replace({pd.NaT: None})

        print(f"{GREEN}การเตรียมข้อมูลเสร็จสมบูรณ์{RESET}")
        # --- สิ้นสุดการจัดการข้อมูล ---

        # --- ซิงค์ข้อมูลกับฐานข้อมูล r_additional (เฉพาะการเพิ่มข้อมูล) ---
        try:
            placeholders = ', '.join(['?'] * len(selected_columns))
            insert_query_r_additional = f"INSERT INTO r_additional ({', '.join(selected_columns)}) VALUES ({placeholders})"

            total_inserted_count = 0
            print(f"\n{BLUE}กำลังตรวจสอบว่ามีข้อมูล m_moi_prov ที่ต้อง Insert หรือไม่...{RESET}")

            if not df_to_insert.empty:
                print(
                    f"{YELLOW}พบข้อมูลจาก m_moi_prov จำนวน {len(df_to_insert)} รายการ จะทำการ INSERT ลงใน r_additional{RESET}")
                inserted_r_additional_count = 0
                for index, row in df_to_insert.iterrows():
                    try:
                        cursor.execute(insert_query_r_additional, [row[col] for col in selected_columns])
                        inserted_r_additional_count += 1
                    except pyodbc.Error as ex:
                        sqlstate = ex.args[0]
                        print(f"{RED}เกิดข้อผิดพลาดในการ INSERT ข้อมูลลง r_additional: {sqlstate} - {ex}{RESET}")
                        print(f"Values that caused the error: {list(row)}")
                        sql_conn.rollback()
                        raise
                print(
                    f"{PINK}Inserted {inserted_r_additional_count} new records for ProvCode = '{ProvCode}' from m_moi_prov into r_additional.{RESET}")
                total_inserted_count += inserted_r_additional_count
            else:
                print(f"{YELLOW}ไม่พบข้อมูลใหม่จาก m_moi_prov ที่ต้องทำการ INSERT{RESET}")

            sql_conn.commit()
            print(
                f"{GREEN}การดำเนินการ INSERT ข้อมูลสำหรับ ProvCode = '{ProvCode}' ใน r_additional เสร็จสมบูรณ์ (รวม {total_inserted_count} รายการใหม่){RESET}")

        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"{RED}เกิดข้อผิดพลาดในการซิงค์ข้อมูล: {sqlstate} - {ex}{RESET}")
            sql_conn.rollback()
            raise
        except Exception as e:
            print(f"{RED}เกิดข้อผิดพลาดที่ไม่คาดคิดในการซิงค์ข้อมูล: {e}{RESET}")
            if sql_conn:
                sql_conn.rollback()
            raise
        # --- สิ้นสุดการซิงค์ข้อมูล ---

        time_end = datetime.datetime.now()
        print("เวลาสิ้นสุด:", time_end)
        time_difference = time_end - time_begin
        print("ความต่างของเวลา:", time_difference)

        rows_to_insert_count = len(df_to_insert)

        single_report_df = pd.DataFrame([{
            'ProvCode': ProvCode,
            'time_begin': time_begin,
            'time_end_check_dup': datetime.datetime.now(),  # ปรับปรุงให้ถูกต้อง
            'time_difference_check_dup': time_difference,
            'time_end': time_end,
            'time_difference': str(time_difference),
            'Shape of df_bmn': str(df_bmn.shape),
            'Shape of df_dopa': str(df_dopa.shape),
            'df_dopa_cut': str(df_dopa.shape),
            'df_bmn_key': str(df_bmn.shape),
            'df_dopa_key': str(df_dopa.shape),
            'merged_df': str(merged_df.shape),
            'rows_from_dopa': str(df_to_insert.shape),  # ปรับให้เป็น df_to_insert
            'rows_to_expand': str(rows_to_expand.shape),
            'rows_to_keep': str(rows_to_keep.shape),
            'expanded_df': str(expanded_df.shape),
            'Shape of final_merged_df': str(df_to_insert.shape),  # ปรับให้เป็น df_to_insert
            'Shape of df_filtered': str(df_to_insert.shape),  # ปรับให้เป็น df_to_insert
            'Shape of df_to_insert': str(rows_to_insert_count)
        }], columns=report_columns)

        single_report_df.to_csv(
            report_filename,
            mode='a',
            header=not os.path.exists(report_filename),
            index=False,
            encoding='utf-8-sig'
        )
        print(f"\n{GREEN}บันทึกข้อมูล ProvCode {ProvCode} ลงใน '{report_filename}' เสร็จสมบูรณ์{RESET}")

except pyodbc.Error as ex:
    sqlstate = ex.args[0]
    print(f"Database error occurred: {sqlstate}")
    print(ex)
    if sql_conn:
        sql_conn.rollback()
        print("Transaction rolled back.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    if cursor:
        cursor.close()
    if sql_conn:
        sql_conn.close()
        print("Database connection closed.")