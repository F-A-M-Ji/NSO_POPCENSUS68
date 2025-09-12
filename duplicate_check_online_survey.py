import warnings
import pandas as pd
import datetime
import pyodbc
from functools import reduce
import os

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
PROVINCE_CODES_TO_RUN = ["41"]

# RegCode = '5'
# ProvCode = '41'

report_data = []
report_columns = [
    'ProvCode', 'time_begin', 'time_end', 'time_difference', 'Shape of df', 'Shape of df_final_combined', 'Shape of df_filtered'
]

# --------------------------- แก้ที่อยู่ของ report_online_survey.csv ---------------------------
report_filename = 'C:/Users/NSO/Desktop/pop_run_data/duplicate_check/report_online_survey.csv'

sql_conn = None
cursor = None
try:

    # ------------------------------------ ที่ ip server ----------------------------------------
    sql_conn = pyodbc.connect('DRIVER={SQL Server};SERVER=192.168.0.203;DATABASE=pop6768;UID=pdan;PWD=P@ssw0rd12#$')
    print("Connection successful")
    cursor = sql_conn.cursor()

    for ProvCode in PROVINCE_CODES_TO_RUN:
        print(f"\n{BLUE}====================================================={RESET}")
        print(f"{BLUE}>>> เริ่มกระบวนการสำหรับ ProvCode: {ProvCode}{RESET}")
        print(f"{BLUE}====================================================={RESET}")

        time_begin = datetime.datetime.now()
        print("เวลาเริ่มต้น:", time_begin)

        # ดึงข้อมูลจาก r_building
        query_data = f'''SELECT * FROM r_online_survey WHERE (ProvCode = '{ProvCode}') '''
        data = pd.read_sql(query_data, sql_conn)
        print("Shape of df:", data.shape)

        # ==========================================================


        def format_building_type(x):
            # ถ้าเป็น NaN คืนค่าเดิม
            if pd.isna(x):
                return x
            # แปลงเป็น string และลบช่องว่าง
            x_str = str(x).strip()
            # ตรวจสอบว่าเป็นตัวเลข (อนุญาตทศนิยม)
            try:
                num = int(float(x_str))  # แปลงเป็น float แล้วเป็น int
                return str(num).zfill(2)  # เติม 0 หน้าให้ครบ 2 หลัก
            except ValueError:
                return x  # ถ้าไม่ใช่ตัวเลข คืนค่าเดิม


        # ใช้ apply กับคอลัมน์
        data['BuildingType'] = data['BuildingType'].apply(format_building_type)


        # data['Residing'] = data['Residing'].apply(
        # lambda x: str(int(float(str(x).strip())))
        # if pd.notna(x) and str(x).strip().replace('.', '', 1).replace('-', '', 1).isdigit()
        # else x)

        def format_residing_type(x):
            # ถ้าเป็น NaN คืนค่าเดิม
            if pd.isna(x):
                return x
            # แปลงเป็น string และลบช่องว่าง
            x_str = str(x).strip()
            # ตรวจสอบว่าเป็นตัวเลข (อนุญาตทศนิยม)
            try:
                num = int(float(x_str))  # แปลงเป็น float แล้วเป็น int
                return str(num)
            except ValueError:
                return x  # ถ้าไม่ใช่ตัวเลข คืนค่าเดิม


        # ใช้ apply กับคอลัมน์
        data['Residing'] = data['Residing'].apply(format_residing_type)

        #### หมายเหตุ pd.to_datetime จะลองรับมิลลิวินาทีอยู่แล้ว สามารถเปรียบเทียบได้ในระดับมิลลิวินาทีได้เลย
        data['Population_CrtdDateStart'] = pd.to_datetime(data['Population_CrtdDateStart'], errors='coerce')
        data['Population_CrtdDateEnd'] = pd.to_datetime(data['Population_CrtdDateEnd'], errors='coerce')

        ## ส่วนที่ 1

        df1 = pd.DataFrame(data)

        df1['Household_No_new'] = df1['Household_No']
        mask_blank = df1['Household_No'].isna() | (df1['Household_No'].astype(str).str.strip() == '')
        df1.loc[mask_blank, 'Household_No_new'] = list(range(1, mask_blank.sum() + 1))

        df1['Population_No_new'] = df1['Population_No']
        mask_blank = df1['Population_No'].isna() | (df1['Population_No'].astype(str).str.strip() == '')
        df1.loc[mask_blank, 'Population_No_new'] = list(range(1, mask_blank.sum() + 1))

        df1_01_07_18 = df1[~df1['BuildingType'].isin(
            ['02', '03', '04', '05', '06', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '19'])].reset_index(
            drop=True)
        df1_01_07_18_POP = df1_01_07_18.groupby('EA_Code_15')['Population_No_new'].count().reset_index(name='All_POP_Count')
        df1_01_07_18_HH = df1_01_07_18.groupby('EA_Code_15')['Household_No_new'].nunique().reset_index(name='All_HH_Count')

        # เลือกประเภทบ้าน/อาคาร/สิ่งปลูกสร้าง BuildingType = 01,07,18,blank
        df1_not = df1[(~df1['BuildingType'].isin(
            ['02', '03', '04', '05', '06', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '19'])) & (
                        ~df1['Residing'].isin(['1']))].reset_index(drop=True)
        df1 = df1[(~df1['BuildingType'].isin(
            ['02', '03', '04', '05', '06', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '19'])) & (
                    df1['Residing'].isin(['1']))].reset_index(drop=True)
        df1 = df1.copy().reset_index(drop=True)
        df1 = df1.sort_values(
            by=['NsoBuilding_No', 'Building_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'MunTaoCode',
                'VilCode',
                'EA_Code_15', 'Household_No', 'Household_No_new', 'HouseNumber', 'RoomNumber', 'HouseholdMemberNumber'],
            ascending=True).reset_index(drop=True)

        # สร้างตัวแปรตรวจสอบความซ้ำซ้อนของข้อมูลระดับครัวเรือน
        key_columns_df1 = ['NsoBuilding_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'MunTaoCode',
                        'VilCode',
                        'EA_Code_15', 'HouseNumber', 'RoomNumber', 'RoadName', 'AlleyWayName', 'AlleyName', 'FirstName',
                        'LastName']
        # df1['key_HH_df1'] = df1[key_columns_df1].astype(str).agg(''.join, axis=1)
        df1['key_HH_df1'] = df1[key_columns_df1].fillna('').astype(str).sum(axis=1)

        # สร้างตัวแปรจำนวนข้อความที่ตอบในครัวเรือน ตั้งแต่ Title - Gender
        question_columns = ['Title', 'FirstName', 'LastName', 'Relationship', 'Sex', 'MonthOfBirth', 'YearOfBirth',
                            'Age_01', 'NationalityNumeric',
                            'EducationalAttainment', 'EmploymentStatus', 'NameInHouseholdRegister',
                            'NameInHouseholdRegisterOther',
                            'DurationOfResidence', 'MigrationCharecteristics', 'MovedFromProvince', 'MovedFromAbroad',
                            'MigrationReason', 'Gender']

        BLANK_VALUES = ['', 'nan', 'NaN', 'None']


        def clean_blank_df1(x):
            return pd.NA if str(x).strip() in BLANK_VALUES else x


        df1[question_columns] = df1[question_columns].apply(lambda col: col.map(clean_blank_df1)).astype("string")
        df1['answered_questions_person'] = df1[question_columns].notna().sum(axis=1)
        hh_answered = (
            df1.groupby('Household_No_new')['answered_questions_person'].sum().reset_index(name='answered_questions'))
        df1 = df1.merge(hh_answered, on='Household_No_new', how='left').reset_index(drop=True)

        ####log case 01_07_18##############################################################################################################################
        df1log = df1.groupby('Household_No_new')[['key_HH_df1', 'answered_questions', 'Population_CrtdDateEnd',
                                                'Population_CrtdDateStart']].first().reset_index()
        # 1. มี CrtdDateEnd มี CrtdDateStart เลือกข้อถามมากที่สุด
        result0_1 = df1log[~df1log['Population_CrtdDateEnd'].notna()].copy()
        result1_1 = df1log[
            (df1log['Population_CrtdDateEnd'].notna()) &
            (df1log['answered_questions'] == df1log.groupby('key_HH_df1')['answered_questions'].transform('max'))
            ].copy()
        result2_1 = pd.concat([result0_1, result1_1], ignore_index=True)
        # result2_1.duplicated(subset=["key_HH_df1"]).sum()

        # 2. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน เลือก CrtdDateEnd ล่าสุด
        result3_1 = result2_1[~result2_1['Population_CrtdDateEnd'].notna()].copy()
        result4_1 = result2_1[
            (result2_1['Population_CrtdDateEnd'].notna()) &
            (result2_1['Population_CrtdDateEnd'] == result2_1.groupby('key_HH_df1')['Population_CrtdDateEnd'].transform(
                'max'))
            ].copy()
        result5_1 = pd.concat([result3_1, result4_1], ignore_index=True)
        # result5_1.duplicated(subset=["key_HH_df1"]).sum()

        # 3. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateEnd เท่ากัน เลือก CrtdDateStart ล่าสุด
        result6_1 = result5_1[~result5_1['Population_CrtdDateEnd'].notna()].copy()
        result7_1 = result5_1[
            (result5_1['Population_CrtdDateEnd'].notna()) &
            (result5_1['Population_CrtdDateStart'] == result5_1.groupby('key_HH_df1')['Population_CrtdDateStart'].transform(
                'max'))
            ].copy()
        result8_1 = pd.concat([result6_1, result7_1], ignore_index=True)
        # result8_1.duplicated(subset=["key_HH_df1"]).sum()

        # 4. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateEnd เท่ากัน และ CrtdDateStart เท่ากัน เลือกมาก 1 ครัวเรือน
        result9_1 = result8_1[~result8_1['Population_CrtdDateEnd'].notna()].copy()
        result10_1 = result8_1[result8_1['Population_CrtdDateEnd'].notna()].sort_values(
            by=['key_HH_df1', 'answered_questions', 'Population_CrtdDateEnd', 'Population_CrtdDateStart'],
            ascending=False).drop_duplicates(subset=['key_HH_df1'], keep='first')
        result11_1 = pd.concat([result9_1, result10_1], ignore_index=True)
        # result11_1.duplicated(subset=["key_HH_df1"]).sum()

        # 5. ไม่มี CrtdDateEnd มี CrtdDateStart เลือกข้อถามมากที่สุด
        result12_1 = result11_1[~result11_1['Population_CrtdDateStart'].notna()].copy()
        result13_1 = result11_1[
            (result11_1['Population_CrtdDateStart'].notna()) &
            (result11_1['answered_questions'] == result11_1.groupby('key_HH_df1')['answered_questions'].transform('max'))
            ].copy()
        result14_1 = pd.concat([result12_1, result13_1], ignore_index=True)
        # result14_1.duplicated(subset=["key_HH_df1"]).sum()

        # 6. ไม่มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน เลือก CrtdDateStart ล่าสุด
        result15_1 = result14_1[~result14_1['Population_CrtdDateStart'].notna()].copy()
        result16_1 = result14_1[
            (result14_1['Population_CrtdDateStart'].notna()) &
            (result14_1['Population_CrtdDateStart'] == result14_1.groupby('key_HH_df1')[
                'Population_CrtdDateStart'].transform('max'))
            ].copy()
        result17_1 = pd.concat([result15_1, result16_1], ignore_index=True)
        # result17_1.duplicated(subset=["key_HH_df1"]).sum()

        # 7. ไม่มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateStart เท่ากัน เลือกมาก 1 ครัวเรือน
        result18_1 = result17_1[~result17_1['Population_CrtdDateStart'].notna()].copy()
        result19_1 = result17_1[result17_1['Population_CrtdDateStart'].notna()].sort_values(
            by=['key_HH_df1', 'answered_questions', 'Population_CrtdDateEnd', 'Population_CrtdDateStart'],
            ascending=False).drop_duplicates(subset=['key_HH_df1'], keep='first')
        result20_1 = pd.concat([result18_1, result19_1], ignore_index=True)
        # result20_1.duplicated(subset=["key_HH_df1"]).sum()

        # 8. มี CrtdDateEnd ไม่มี CrtdDateEnd เลือกที่มี CrtdDateEnd
        max_dates_1 = result20_1.groupby('key_HH_df1')['Population_CrtdDateEnd'].transform('max')
        mask_1 = (
                (result20_1['Population_CrtdDateEnd'] == max_dates_1) |
                (max_dates_1.isna() & result20_1['Population_CrtdDateEnd'].isna())
        )
        result21_1 = result20_1[mask_1].copy()
        # result21_1.duplicated(subset=["key_HH_df1"]).sum()

        # 9. มีการตอบข้อคำถาม ไม่มีการตอบข้อคำถาม เลือกที่มีการตอบข้อคำถาม
        result22_1 = result21_1[
            (result21_1['answered_questions'] == result21_1.groupby('key_HH_df1')['answered_questions'].transform('max'))
        ].copy()
        # result22_1.duplicated(subset=["key_HH_df1"]).sum()

        # 10. ไม่มี CrtdDateEnd ไม่ตอบข้อถาม มี CrtdDateStart เลือก CrtdDateStart ล่าสุด
        max_dates_1 = result22_1.groupby('key_HH_df1')['Population_CrtdDateStart'].transform('max')
        mask_1 = (
                (result22_1['Population_CrtdDateStart'] == max_dates_1) |
                (max_dates_1.isna() & result22_1['Population_CrtdDateStart'].isna())
        )
        result23_1 = result22_1[mask_1].copy()
        # result23_1.duplicated(subset=["key_HH_df1"]).sum()

        # 11. ไม่มี CrtdDateEnd ไม่การตอบข้อคำถาม ไม่มี CrtdDateStart เลือกมา 1 ครัวเรือน
        result24_1 = result23_1.sort_values(
            by=['key_HH_df1', 'answered_questions', 'Population_CrtdDateEnd', 'Population_CrtdDateStart'],
            ascending=False).drop_duplicates(subset=['key_HH_df1'], keep='first')
        # result24_1.duplicated(subset=["key_HH_df1"]).sum()

        results_log_1 = {
            "จำนวนครัวเรือนซ้ำทั้งหมด": df1log,
            "1. มี CrtdDateEnd มี CrtdDateStart เลือกข้อถามมากที่สุด": result2_1,
            "2. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน เลือก CrtdDateEnd ล่าสุด": result5_1,
            "3. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateEnd เท่ากัน เลือก CrtdDateStart ล่าสุด": result8_1,
            "4. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateEnd เท่ากัน และ CrtdDateStart เท่ากัน เลือกมาก 1 ครัวเรือน": result11_1,
            "5. ไม่มี CrtdDateEnd มี CrtdDateStart เลือกข้อถามมากที่สุด": result14_1,
            "6. ไม่มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน เลือก CrtdDateStart ล่าสุด": result17_1,
            "7. ไม่มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateStart เท่ากัน เลือกมาก 1 ครัวเรือน": result20_1,
            "8. มี CrtdDateEnd ไม่มี CrtdDateEnd เลือกที่มี CrtdDateEnd": result21_1,
            "9. มีการตอบข้อคำถาม ไม่มีการตอบข้อคำถาม เลือกที่มีการตอบข้อคำถาม": result22_1,
            "10. ไม่มี CrtdDateEnd ไม่ตอบข้อถาม มี CrtdDateStart เลือก CrtdDateStart ล่าสุด": result23_1,
            "11. ไม่มี CrtdDateEnd ไม่การตอบข้อคำถาม ไม่มี CrtdDateStart เลือกมา 1 ครัวเรือน": result24_1
        }
        df1_summary = pd.DataFrame([
            {
                "buildingtype": "01_07_18",
                "Case": name,
                "Duplicated": df.duplicated(subset=["key_HH_df1"]).sum()}
            for name, df in results_log_1.items()
        ])
        ###############################################################################################################################################
        # ลบครัวเรือนที่ซ้ำซ้อน
        df1_1 = df1.groupby('Household_No_new')[['key_HH_df1', 'answered_questions', 'Population_CrtdDateEnd',
                                                'Population_CrtdDateStart']].first().reset_index()


        def select_best_df1(group):
            with_end = group.dropna(subset=['Population_CrtdDateEnd'])

            def safe_filter_df1(df1, column):
                if df1.empty or column not in df1.columns:
                    return df1
                val = df1[column].max()
                return df1[df1[column] == val]

            if not with_end.empty:
                filtered = safe_filter_df1(with_end, 'answered_questions')
                if len(filtered) == 1:
                    return filtered.iloc[0]

                filtered = safe_filter_df1(filtered, 'Population_CrtdDateEnd')
                if len(filtered) == 1:
                    return filtered.iloc[0]

                filtered = safe_filter_df1(filtered, 'Population_CrtdDateStart')
                if len(filtered) == 1:
                    return filtered.iloc[0]
                else:
                    return group.iloc[0] if not group.empty else pd.Series(dtype=object)

            else:
                filtered = safe_filter_df1(group, 'answered_questions')
                if len(filtered) == 1:
                    return filtered.iloc[0]

                filtered = safe_filter_df1(filtered, 'Population_CrtdDateStart')
                if len(filtered) == 1:
                    return filtered.iloc[0]
                else:
                    return group.iloc[0] if not group.empty else pd.Series(dtype=object)


        result_df1 = (df1_1.groupby('key_HH_df1', group_keys=False).apply(select_best_df1).reset_index(drop=True))

        merged_df1 = pd.merge(df1, result_df1[['Household_No_new']], on='Household_No_new', how='left', indicator=True)
        df1['Merge_Flag'] = (merged_df1['_merge'] == 'both').astype(int).reset_index(drop=True)
        df1_0 = df1[df1['Merge_Flag'] == 1]

        df1_new = df1_0.copy().reset_index(drop=True)
        df1_new = df1_new.sort_values(
            by=['NsoBuilding_No', 'Building_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'MunTaoCode',
                'VilCode',
                'EA_Code_15', 'Household_No', 'Household_No_new', 'HouseNumber', 'RoomNumber', 'HouseholdMemberNumber'],
            ascending=True).reset_index(drop=True)
        df1_final = df1_new.copy().reset_index(drop=True)
        df1_final.drop(columns=['key_HH_df1', 'answered_questions', 'answered_questions_person', 'Merge_Flag'],
                    inplace=True)

        df1_combined = pd.concat([df1_final, df1_not], axis=0, ignore_index=True)
        ############################################################
        df1_final_01_07_18 = df1_combined.copy().reset_index(drop=True)
        df1_01_07_18_final_POP = df1_final_01_07_18.groupby('EA_Code_15')['Population_No_new'].count().reset_index(
            name='POP_Count')
        df1_01_07_18_final_HH = df1_final_01_07_18.groupby('EA_Code_15')['Household_No_new'].nunique().reset_index(
            name='HH_Count')
        ############################################################
        # df1_combined.drop(columns=['Household_No_new','Population_No_new'], inplace=True)
        df1_clearDup_01_07_18 = df1_combined.sort_values(
            by=['NsoBuilding_No', 'Building_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'MunTaoCode',
                'VilCode',
                'EA_Code_15', 'Household_No', 'HouseNumber', 'RoomNumber', 'HouseholdMemberNumber'],
            ascending=True).reset_index(drop=True)

        ## --------------------------------------------------------------------------------------------------------#

        ## ส่วนที่ 2

        df2 = pd.DataFrame(data)

        df2['Household_No_new'] = df2['Household_No']
        mask_blank = df2['Household_No'].isna() | (df2['Household_No'].astype(str).str.strip() == '')
        df2.loc[mask_blank, 'Household_No_new'] = list(range(1, mask_blank.sum() + 1))

        df2['Population_No_new'] = df2['Population_No']
        mask_blank = df2['Population_No'].isna() | (df2['Population_No'].astype(str).str.strip() == '')
        df2.loc[mask_blank, 'Population_No_new'] = list(range(1, mask_blank.sum() + 1))

        df2_02_06 = df2[df2['BuildingType'].isin(['02', '03', '04', '05', '06'])].reset_index(drop=True)
        df2_02_06_POP = df2_02_06.groupby('EA_Code_15')['Population_No_new'].count().reset_index(name='All_POP_Count')
        df2_02_06_HH = df2_02_06.groupby('EA_Code_15')['Household_No_new'].nunique().reset_index(name='All_HH_Count')

        # เลือกประเภทบ้าน/อาคาร/สิ่งปลูกสร้าง BuildingType = 02,03,04,05,06
        df2_not = df2[
            (df2['BuildingType'].isin(['02', '03', '04', '05', '06'])) & (~df2['Residing'].isin(['1']))].reset_index(
            drop=True)
        df2 = df2[(df2['BuildingType'].isin(['02', '03', '04', '05', '06'])) & (df2['Residing'].isin(['1']))].reset_index(
            drop=True)
        df2 = df2.copy().reset_index(drop=True)
        df2 = df2.sort_values(
            by=['NsoBuilding_No', 'Building_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'MunTaoCode',
                'VilCode',
                'EA_Code_15', 'Household_No', 'Household_No_new', 'HouseNumber', 'RoomNumber', 'HouseholdMemberNumber'],
            ascending=True).reset_index(drop=True)

        # สร้างตัวแปรตรวจสอบความซ้ำซ้อนของข้อมูลระดับครัวเรือน
        key_columns_df2 = ['NsoBuilding_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'MunTaoCode',
                        'VilCode',
                        'EA_Code_15', 'HouseNumber', 'RoomNumber', 'RoadName', 'AlleyWayName', 'AlleyName', 'FirstName',
                        'LastName']
        # df2['key_HH_df2'] = df2[key_columns_df2].astype(str).agg(''.join, axis=1)
        df2['key_HH_df2'] = df2[key_columns_df2].fillna('').astype(str).sum(axis=1)

        # สร้างตัวแปรจำนวนข้อความที่ตอบในครัวเรือน ตั้งแต่ Title - Gender
        question_columns = ['Title', 'FirstName', 'LastName', 'Relationship', 'Sex', 'MonthOfBirth', 'YearOfBirth',
                            'Age_01', 'NationalityNumeric',
                            'EducationalAttainment', 'EmploymentStatus', 'NameInHouseholdRegister',
                            'NameInHouseholdRegisterOther',
                            'DurationOfResidence', 'MigrationCharecteristics', 'MovedFromProvince', 'MovedFromAbroad',
                            'MigrationReason', 'Gender']

        BLANK_VALUES = ['', 'nan', 'NaN', 'None']


        def clean_blank_df2(x):
            return pd.NA if str(x).strip() in BLANK_VALUES else x


        df2[question_columns] = df2[question_columns].apply(lambda col: col.map(clean_blank_df2)).astype("string")
        df2['answered_questions_person'] = df2[question_columns].notna().sum(axis=1)
        hh_answered = (
            df2.groupby('Household_No_new')['answered_questions_person'].sum().reset_index(name='answered_questions'))
        df2 = df2.merge(hh_answered, on='Household_No_new', how='left').reset_index(drop=True)

        df2_answered_questions_0 = df2[df2['answered_questions'].isin([0])].reset_index(drop=True)

        df2 = df2[~df2['answered_questions'].isin([0])].reset_index(drop=True)

        ####log case 02_06#################################################################################################################################################
        df2log = df2.groupby('Household_No_new')[['key_HH_df2', 'answered_questions', 'Population_CrtdDateEnd',
                                                'Population_CrtdDateStart']].first().reset_index()
        # 1. มี CrtdDateEnd มี CrtdDateStart เลือกข้อถามมากที่สุด
        result0_2 = df2log[~df2log['Population_CrtdDateEnd'].notna()].copy()
        result1_2 = df2log[
            (df2log['Population_CrtdDateEnd'].notna()) &
            (df2log['answered_questions'] == df2log.groupby('key_HH_df2')['answered_questions'].transform('max'))
            ].copy()
        result2_2 = pd.concat([result0_2, result1_2], ignore_index=True)
        # result2_2.duplicated(subset=["key_HH_df2"]).sum()

        # 2. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน เลือก CrtdDateEnd ล่าสุด
        result3_2 = result2_2[~result2_2['Population_CrtdDateEnd'].notna()].copy()
        result4_2 = result2_2[
            (result2_2['Population_CrtdDateEnd'].notna()) &
            (result2_2['Population_CrtdDateEnd'] == result2_2.groupby('key_HH_df2')['Population_CrtdDateEnd'].transform(
                'max'))
            ].copy()
        result5_2 = pd.concat([result3_2, result4_2], ignore_index=True)
        # result5_2.duplicated(subset=["key_HH_df2"]).sum()

        # 3. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateEnd เท่ากัน เลือก CrtdDateStart ล่าสุด
        result6_2 = result5_2[~result5_2['Population_CrtdDateEnd'].notna()].copy()
        result7_2 = result5_2[
            (result5_2['Population_CrtdDateEnd'].notna()) &
            (result5_2['Population_CrtdDateStart'] == result5_2.groupby('key_HH_df2')['Population_CrtdDateStart'].transform(
                'max'))
            ].copy()
        result8_2 = pd.concat([result6_2, result7_2], ignore_index=True)
        # result8_2.duplicated(subset=["key_HH_df2"]).sum()

        # 4. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateEnd เท่ากัน และ CrtdDateStart เท่ากัน เลือกมาก 1 ครัวเรือน
        result9_2 = result8_2[~result8_2['Population_CrtdDateEnd'].notna()].copy()
        result10_2 = result8_2[result8_2['Population_CrtdDateEnd'].notna()].sort_values(
            by=['key_HH_df2', 'answered_questions', 'Population_CrtdDateEnd', 'Population_CrtdDateStart'],
            ascending=False).drop_duplicates(subset=['key_HH_df2'], keep='first')
        result11_2 = pd.concat([result9_2, result10_2], ignore_index=True)
        # result11_2.duplicated(subset=["key_HH_df2"]).sum()

        # 5. ไม่มี CrtdDateEnd มี CrtdDateStart เลือกข้อถามมากที่สุด
        result12_2 = result11_2[~result11_2['Population_CrtdDateStart'].notna()].copy()
        result13_2 = result11_2[
            (result11_2['Population_CrtdDateStart'].notna()) &
            (result11_2['answered_questions'] == result11_2.groupby('key_HH_df2')['answered_questions'].transform('max'))
            ].copy()
        result14_2 = pd.concat([result12_2, result13_2], ignore_index=True)
        # result14_2.duplicated(subset=["key_HH_df2"]).sum()

        # 6. ไม่มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน เลือก CrtdDateStart ล่าสุด
        result15_2 = result14_2[~result14_2['Population_CrtdDateStart'].notna()].copy()
        result16_2 = result14_2[
            (result14_2['Population_CrtdDateStart'].notna()) &
            (result14_2['Population_CrtdDateStart'] == result14_2.groupby('key_HH_df2')[
                'Population_CrtdDateStart'].transform('max'))
            ].copy()
        result17_2 = pd.concat([result15_2, result16_2], ignore_index=True)
        # result17_2.duplicated(subset=["key_HH_df2"]).sum()

        # 7. ไม่มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateStart เท่ากัน เลือกมาก 1 ครัวเรือน
        result18_2 = result17_2[~result17_2['Population_CrtdDateStart'].notna()].copy()
        result19_2 = result17_2[result17_2['Population_CrtdDateStart'].notna()].sort_values(
            by=['key_HH_df2', 'answered_questions', 'Population_CrtdDateEnd', 'Population_CrtdDateStart'],
            ascending=False).drop_duplicates(subset=['key_HH_df2'], keep='first')
        result20_2 = pd.concat([result18_2, result19_2], ignore_index=True)
        # result20_2.duplicated(subset=["key_HH_df2"]).sum()

        # 8. มี CrtdDateEnd ไม่มี CrtdDateEnd เลือกที่มี CrtdDateEnd
        max_dates_2 = result20_2.groupby('key_HH_df2')['Population_CrtdDateEnd'].transform('max')
        mask_2 = (
                (result20_2['Population_CrtdDateEnd'] == max_dates_2) |
                (max_dates_2.isna() & result20_2['Population_CrtdDateEnd'].isna())
        )
        result21_2 = result20_2[mask_2].copy()
        # result21_2.duplicated(subset=["key_HH_df2"]).sum()

        # 9. มีการตอบข้อคำถาม ไม่มีการตอบข้อคำถาม เลือกที่มีการตอบข้อคำถาม
        result22_2 = result21_2[
            (result21_2['answered_questions'] == result21_2.groupby('key_HH_df2')['answered_questions'].transform('max'))
        ].copy()
        result22_2.duplicated(subset=["key_HH_df2"]).sum()

        # 10. ไม่มี CrtdDateEnd ไม่ตอบข้อถาม มี CrtdDateStart เลือก CrtdDateStart ล่าสุด
        max_dates_2 = result22_2.groupby('key_HH_df2')['Population_CrtdDateStart'].transform('max')
        mask_2 = (
                (result22_2['Population_CrtdDateStart'] == max_dates_2) |
                (max_dates_2.isna() & result22_2['Population_CrtdDateStart'].isna())
        )
        result23_2 = result22_2[mask_2].copy()
        # result23_2.duplicated(subset=["key_HH_df2"]).sum()

        # 11. ไม่มี CrtdDateEnd ไม่การตอบข้อคำถาม ไม่มี CrtdDateStart เลือกมา 1 ครัวเรือน
        result24_2 = result23_2.sort_values(
            by=['key_HH_df2', 'answered_questions', 'Population_CrtdDateEnd', 'Population_CrtdDateStart'],
            ascending=False).drop_duplicates(subset=['key_HH_df2'], keep='first')
        result24_2.duplicated(subset=["key_HH_df2"]).sum()

        results_log_2 = {
            "จำนวนครัวเรือนซ้ำทั้งหมด": df2log,
            "1. มี CrtdDateEnd มี CrtdDateStart เลือกข้อถามมากที่สุด": result2_2,
            "2. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน เลือก CrtdDateEnd ล่าสุด": result5_2,
            "3. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateEnd เท่ากัน เลือก CrtdDateStart ล่าสุด": result8_2,
            "4. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateEnd เท่ากัน และ CrtdDateStart เท่ากัน เลือกมาก 1 ครัวเรือน": result11_2,
            "5. ไม่มี CrtdDateEnd มี CrtdDateStart เลือกข้อถามมากที่สุด": result14_2,
            "6. ไม่มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน เลือก CrtdDateStart ล่าสุด": result17_2,
            "7. ไม่มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateStart เท่ากัน เลือกมาก 1 ครัวเรือน": result20_2,
            "8. มี CrtdDateEnd ไม่มี CrtdDateEnd เลือกที่มี CrtdDateEnd": result21_2,
            "9. มีการตอบข้อคำถาม ไม่มีการตอบข้อคำถาม เลือกที่มีการตอบข้อคำถาม": result22_2,
            "10. ไม่มี CrtdDateEnd ไม่ตอบข้อถาม มี CrtdDateStart เลือก CrtdDateStart ล่าสุด": result23_2,
            "11. ไม่มี CrtdDateEnd ไม่การตอบข้อคำถาม ไม่มี CrtdDateStart เลือกมา 1 ครัวเรือน": result24_2
        }
        df2_summary = pd.DataFrame([
            {
                "buildingtype": "02_06",
                "Case": name,
                "Duplicated": df.duplicated(subset=["key_HH_df2"]).sum()}
            for name, df in results_log_2.items()
        ])
        ###############################################################################################################################################

        # ลบครัวเรือนที่ซ้ำซ้อน
        df2_1 = df2.groupby('Household_No_new')[['key_HH_df2', 'answered_questions', 'Population_CrtdDateEnd',
                                                'Population_CrtdDateStart']].first().reset_index()


        def select_best_df2(group):
            with_end = group.dropna(subset=['Population_CrtdDateEnd'])

            def safe_filter_df2(df2, column):
                if df2.empty or column not in df2.columns:
                    return df2
                val = df2[column].max()
                return df2[df2[column] == val]

            if not with_end.empty:
                filtered = safe_filter_df2(with_end, 'answered_questions')
                if len(filtered) == 1:
                    return filtered.iloc[0]

                filtered = safe_filter_df2(filtered, 'Population_CrtdDateEnd')
                if len(filtered) == 1:
                    return filtered.iloc[0]

                filtered = safe_filter_df2(filtered, 'Population_CrtdDateStart')
                if len(filtered) == 1:
                    return filtered.iloc[0]
                else:
                    return group.iloc[0] if not group.empty else pd.Series(dtype=object)

            else:
                filtered = safe_filter_df2(group, 'answered_questions')
                if len(filtered) == 1:
                    return filtered.iloc[0]

                filtered = safe_filter_df2(filtered, 'Population_CrtdDateStart')
                if len(filtered) == 1:
                    return filtered.iloc[0]
                else:
                    return group.iloc[0] if not group.empty else pd.Series(dtype=object)


        result_df2 = (df2_1.groupby('key_HH_df2', group_keys=False).apply(select_best_df2).reset_index(drop=True))

        merged_df2 = pd.merge(df2, result_df2[['Household_No_new']], on='Household_No_new', how='left', indicator=True)
        df2['Merge_Flag'] = (merged_df2['_merge'] == 'both').astype(int).reset_index(drop=True)
        df2_0 = df2[df2['Merge_Flag'] == 1]

        df2_new = df2_0.copy().reset_index(drop=True)
        df2_new = df2_new.sort_values(
            by=['NsoBuilding_No', 'Building_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'MunTaoCode',
                'VilCode',
                'EA_Code_15', 'Household_No', 'Household_No_new', 'HouseNumber', 'RoomNumber', 'HouseholdMemberNumber'],
            ascending=True).reset_index(drop=True)

        df2_final = df2_new.copy().reset_index(drop=True)
        df2_final.drop(columns=['key_HH_df2', 'answered_questions', 'answered_questions_person', 'Merge_Flag'],
                    inplace=True)
        df2_answered_questions_0.drop(columns=['key_HH_df2', 'answered_questions', 'answered_questions_person'],
                                    inplace=True)

        df2_combined = pd.concat([df2_final, df2_answered_questions_0, df2_not], axis=0, ignore_index=True)
        ############################################################
        df2_final_02_06 = df2_combined.copy().reset_index(drop=True)
        df2_02_06_final_POP = df2_final_02_06.groupby('EA_Code_15')['Population_No_new'].count().reset_index(
            name='POP_Count')
        df2_02_06_final_HH = df2_final_02_06.groupby('EA_Code_15')['Household_No_new'].nunique().reset_index(
            name='HH_Count')
        ############################################################
        # df2_combined.drop(columns=['Household_No_new','Population_No_new'], inplace=True)
        df2_clearDup_02_06 = df2_combined.sort_values(
            by=['NsoBuilding_No', 'Building_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'MunTaoCode',
                'VilCode',
                'EA_Code_15', 'Household_No', 'HouseNumber', 'RoomNumber', 'HouseholdMemberNumber'],
            ascending=True).reset_index(drop=True)

        ## ********************************************************************************##
        ## ส่วนที่ 3

        df3 = pd.DataFrame(data)

        df3['Population_CrtdDateStart'] = pd.to_datetime(df3['Population_CrtdDateStart'], format='%d/%m/%Y %H:%M:%S.%f',
                                                        errors='coerce')
        df3['Population_CrtdDateEnd'] = pd.to_datetime(df3['Population_CrtdDateEnd'], format='%d/%m/%Y %H:%M:%S.%f',
                                                    errors='coerce')

        df3['Household_No_new'] = df3['Household_No']
        mask_blank = df3['Household_No'].isna() | (df3['Household_No'].astype(str).str.strip() == '')
        df3.loc[mask_blank, 'Household_No_new'] = list(range(1, mask_blank.sum() + 1))

        df3['Population_No_new'] = df3['Population_No']
        mask_blank = df3['Population_No'].isna() | (df3['Population_No'].astype(str).str.strip() == '')
        df3.loc[mask_blank, 'Population_No_new'] = list(range(1, mask_blank.sum() + 1))

        df3_08_17 = df3[df3['BuildingType'].isin(['08', '09', '10', '11', '12', '13', '14', '15', '16', '17'])].reset_index(
            drop=True)
        df3_08_17_POP = df3_08_17.groupby('EA_Code_15')['Population_No_new'].count().reset_index(name='All_POP_Count')
        df3_08_17_HH = df3_08_17.groupby('EA_Code_15')['Household_No_new'].nunique().reset_index(name='All_HH_Count')

        # เลือกประเภทบ้าน/อาคาร/สิ่งปลูกสร้าง BuildingType = 08,09,10,11,12,13,14,15,16,17
        df3_not = df3[(df3['BuildingType'].isin(['08', '09', '10', '11', '12', '13', '14', '15', '16', '17'])) & (
            ~df3['Residing'].isin(['1']))].reset_index(drop=True)
        df3 = df3[(df3['BuildingType'].isin(['08', '09', '10', '11', '12', '13', '14', '15', '16', '17'])) & (
            df3['Residing'].isin(['1']))].reset_index(drop=True)
        df3 = df3.copy().reset_index(drop=True)
        df3 = df3.sort_values(
            by=['NsoBuilding_No', 'Building_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'MunTaoCode',
                'VilCode',
                'EA_Code_15', 'Household_No', 'Household_No_new', 'HouseNumber', 'RoomNumber', 'HouseholdMemberNumber'],
            ascending=True).reset_index(drop=True)

        # สร้างตัวแปรตรวจสอบความซ้ำซ้อนของข้อมูลระดับครัวเรือน
        key_columns_df3 = ['NsoBuilding_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'MunTaoCode',
                        'VilCode',
                        'EA_Code_15', 'HouseNumber', 'RoomNumber', 'RoadName', 'AlleyWayName', 'AlleyName', 'FirstName',
                        'LastName']
        # df3['key_HH_df3'] = df3[key_columns_df3].astype(str).agg(''.join, axis=1)
        df3['key_HH_df3'] = df3[key_columns_df3].fillna('').astype(str).sum(axis=1)

        # สร้างตัวแปรจำนวนข้อความที่ตอบในครัวเรือน ตั้งแต่ Title - Gender
        question_columns = ['Title', 'FirstName', 'LastName', 'Relationship', 'Sex', 'MonthOfBirth', 'YearOfBirth',
                            'Age_01', 'NationalityNumeric',
                            'EducationalAttainment', 'EmploymentStatus', 'NameInHouseholdRegister',
                            'NameInHouseholdRegisterOther',
                            'DurationOfResidence', 'MigrationCharecteristics', 'MovedFromProvince', 'MovedFromAbroad',
                            'MigrationReason', 'Gender']

        BLANK_VALUES = ['', 'nan', 'NaN', 'None']


        def clean_blank_df3(x):
            return pd.NA if str(x).strip() in BLANK_VALUES else x


        df3[question_columns] = df3[question_columns].apply(lambda col: col.map(clean_blank_df3)).astype("string")
        df3['answered_questions_person'] = df3[question_columns].notna().sum(axis=1)
        hh_answered = (
            df3.groupby('Household_No_new')['answered_questions_person'].sum().reset_index(name='answered_questions'))

        df3 = df3.merge(hh_answered, on='Household_No_new', how='left').reset_index(drop=True)
        df3_answered_questions_0 = df3[df3['answered_questions'].isin([0])].reset_index(drop=True)
        df3 = df3[~df3['answered_questions'].isin([0])].reset_index(drop=True)

        ####log case 08_17#################################################################################################################################################
        df3log = df3.groupby('Household_No_new')[['key_HH_df3', 'answered_questions', 'Population_CrtdDateEnd',
                                                'Population_CrtdDateStart']].first().reset_index()
        # 1. มี CrtdDateEnd มี CrtdDateStart เลือกข้อถามมากที่สุด
        result0_3 = df3log[~df3log['Population_CrtdDateEnd'].notna()].copy()
        result1_3 = df3log[
            (df3log['Population_CrtdDateEnd'].notna()) &
            (df3log['answered_questions'] == df3log.groupby('key_HH_df3')['answered_questions'].transform('max'))
            ].copy()
        result2_3 = pd.concat([result0_3, result1_3], ignore_index=True)
        # result2_3.duplicated(subset=["key_HH_df3"]).sum()

        # 2. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน เลือก CrtdDateEnd ล่าสุด
        result3_3 = result2_3[~result2_3['Population_CrtdDateEnd'].notna()].copy()
        result4_3 = result2_3[
            (result2_3['Population_CrtdDateEnd'].notna()) &
            (result2_3['Population_CrtdDateEnd'] == result2_3.groupby('key_HH_df3')['Population_CrtdDateEnd'].transform(
                'max'))
            ].copy()
        result5_3 = pd.concat([result3_3, result4_3], ignore_index=True)
        # result5_3.duplicated(subset=["key_HH_df3"]).sum()

        # 3. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateEnd เท่ากัน เลือก CrtdDateStart ล่าสุด
        result6_3 = result5_3[~result5_3['Population_CrtdDateEnd'].notna()].copy()
        result7_3 = result5_3[
            (result5_3['Population_CrtdDateEnd'].notna()) &
            (result5_3['Population_CrtdDateStart'] == result5_3.groupby('key_HH_df3')['Population_CrtdDateStart'].transform(
                'max'))
            ].copy()
        result8_3 = pd.concat([result6_3, result7_3], ignore_index=True)
        # result8_3.duplicated(subset=["key_HH_df3"]).sum()

        # 4. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateEnd เท่ากัน และ CrtdDateStart เท่ากัน เลือกมาก 1 ครัวเรือน
        result9_3 = result8_3[~result8_3['Population_CrtdDateEnd'].notna()].copy()
        result10_3 = result8_3[result8_3['Population_CrtdDateEnd'].notna()].sort_values(
            by=['key_HH_df3', 'answered_questions', 'Population_CrtdDateEnd', 'Population_CrtdDateStart'],
            ascending=False).drop_duplicates(subset=['key_HH_df3'], keep='first')
        result11_3 = pd.concat([result9_3, result10_3], ignore_index=True)
        # result11_3.duplicated(subset=["key_HH_df3"]).sum()

        # 5. ไม่มี CrtdDateEnd มี CrtdDateStart เลือกข้อถามมากที่สุด
        result12_3 = result11_3[~result11_3['Population_CrtdDateStart'].notna()].copy()
        result13_3 = result11_3[
            (result11_3['Population_CrtdDateStart'].notna()) &
            (result11_3['answered_questions'] == result11_3.groupby('key_HH_df3')['answered_questions'].transform('max'))
            ].copy()
        result14_3 = pd.concat([result12_3, result13_3], ignore_index=True)
        # result14_3.duplicated(subset=["key_HH_df3"]).sum()

        # 6. ไม่มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน เลือก CrtdDateStart ล่าสุด
        result15_3 = result14_3[~result14_3['Population_CrtdDateStart'].notna()].copy()
        result16_3 = result14_3[
            (result14_3['Population_CrtdDateStart'].notna()) &
            (result14_3['Population_CrtdDateStart'] == result14_3.groupby('key_HH_df3')[
                'Population_CrtdDateStart'].transform('max'))
            ].copy()
        result17_3 = pd.concat([result15_3, result16_3], ignore_index=True)
        # result17_3.duplicated(subset=["key_HH_df3"]).sum()

        # 7. ไม่มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateStart เท่ากัน เลือกมาก 1 ครัวเรือน
        result18_3 = result17_3[~result17_3['Population_CrtdDateStart'].notna()].copy()
        result19_3 = result17_3[result17_3['Population_CrtdDateStart'].notna()].sort_values(
            by=['key_HH_df3', 'answered_questions', 'Population_CrtdDateEnd', 'Population_CrtdDateStart'],
            ascending=False).drop_duplicates(subset=['key_HH_df3'], keep='first')
        result20_3 = pd.concat([result18_3, result19_3], ignore_index=True)
        # result20_3.duplicated(subset=["key_HH_df3"]).sum()

        # 8. มี CrtdDateEnd ไม่มี CrtdDateEnd เลือกที่มี CrtdDateEnd
        max_dates_3 = result20_3.groupby('key_HH_df3')['Population_CrtdDateEnd'].transform('max')
        mask_3 = (
                (result20_3['Population_CrtdDateEnd'] == max_dates_3) |
                (max_dates_3.isna() & result20_3['Population_CrtdDateEnd'].isna())
        )
        result21_3 = result20_3[mask_3].copy()
        # result21_3.duplicated(subset=["key_HH_df3"]).sum()

        # 9. มีการตอบข้อคำถาม ไม่มีการตอบข้อคำถาม เลือกที่มีการตอบข้อคำถาม
        result22_3 = result21_3[
            (result21_3['answered_questions'] == result21_3.groupby('key_HH_df3')['answered_questions'].transform('max'))
        ].copy()
        # result22_3.duplicated(subset=["key_HH_df3"]).sum()

        # 10. ไม่มี CrtdDateEnd ไม่ตอบข้อถาม มี CrtdDateStart เลือก CrtdDateStart ล่าสุด
        max_dates_3 = result22_3.groupby('key_HH_df3')['Population_CrtdDateStart'].transform('max')
        mask_3 = (
                (result22_3['Population_CrtdDateStart'] == max_dates_3) |
                (max_dates_3.isna() & result22_3['Population_CrtdDateStart'].isna())
        )
        result23_3 = result22_3[mask_3].copy()
        # result23_3.duplicated(subset=["key_HH_df3"]).sum()

        # 11. ไม่มี CrtdDateEnd ไม่การตอบข้อคำถาม ไม่มี CrtdDateStart เลือกมา 1 ครัวเรือน
        result24_3 = result23_3.sort_values(
            by=['key_HH_df3', 'answered_questions', 'Population_CrtdDateEnd', 'Population_CrtdDateStart'],
            ascending=False).drop_duplicates(subset=['key_HH_df3'], keep='first')
        # result24_3.duplicated(subset=["key_HH_df3"]).sum()

        results_log_3 = {
            "จำนวนครัวเรือนซ้ำทั้งหมด": df3log,
            "1. มี CrtdDateEnd มี CrtdDateStart เลือกข้อถามมากที่สุด": result2_3,
            "2. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน เลือก CrtdDateEnd ล่าสุด": result5_3,
            "3. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateEnd เท่ากัน เลือก CrtdDateStart ล่าสุด": result8_3,
            "4. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateEnd เท่ากัน และ CrtdDateStart เท่ากัน เลือกมาก 1 ครัวเรือน": result11_3,
            "5. ไม่มี CrtdDateEnd มี CrtdDateStart เลือกข้อถามมากที่สุด": result14_3,
            "6. ไม่มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน เลือก CrtdDateStart ล่าสุด": result17_3,
            "7. ไม่มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateStart เท่ากัน เลือกมาก 1 ครัวเรือน": result20_3,
            "8. มี CrtdDateEnd ไม่มี CrtdDateEnd เลือกที่มี CrtdDateEnd": result21_3,
            "9. มีการตอบข้อคำถาม ไม่มีการตอบข้อคำถาม เลือกที่มีการตอบข้อคำถาม": result22_3,
            "10. ไม่มี CrtdDateEnd ไม่ตอบข้อถาม มี CrtdDateStart เลือก CrtdDateStart ล่าสุด": result23_3,
            "11. ไม่มี CrtdDateEnd ไม่การตอบข้อคำถาม ไม่มี CrtdDateStart เลือกมา 1 ครัวเรือน": result24_3
        }
        df3_summary = pd.DataFrame([
            {
                "buildingtype": "08_17",
                "Case": name,
                "Duplicated": df.duplicated(subset=["key_HH_df3"]).sum()}
            for name, df in results_log_3.items()
        ])
        ###############################################################################################################################################

        # ลบครัวเรือนที่ซ้ำซ้อน
        df3_1 = df3.groupby('Household_No_new')[['key_HH_df3', 'answered_questions', 'Population_CrtdDateEnd',
                                                'Population_CrtdDateStart']].first().reset_index()


        def select_best_df3(group):
            with_end = group.dropna(subset=['Population_CrtdDateEnd'])

            def safe_filter_df3(df3, column):
                if df3.empty or column not in df3.columns:
                    return df3
                val = df3[column].max()
                return df3[df3[column] == val]

            if not with_end.empty:
                filtered = safe_filter_df3(with_end, 'answered_questions')
                if len(filtered) == 1:
                    return filtered.iloc[0]

                filtered = safe_filter_df3(filtered, 'Population_CrtdDateEnd')
                if len(filtered) == 1:
                    return filtered.iloc[0]

                filtered = safe_filter_df3(filtered, 'Population_CrtdDateStart')
                if len(filtered) == 1:
                    return filtered.iloc[0]
                else:
                    return group.iloc[0] if not group.empty else pd.Series(dtype=object)

            else:
                filtered = safe_filter_df3(group, 'answered_questions')
                if len(filtered) == 1:
                    return filtered.iloc[0]

                filtered = safe_filter_df3(filtered, 'Population_CrtdDateStart')
                if len(filtered) == 1:
                    return filtered.iloc[0]
                else:
                    return group.iloc[0] if not group.empty else pd.Series(dtype=object)


        result_df3 = (
            df3_1.groupby('key_HH_df3', group_keys=False).apply(select_best_df3, include_groups=False).reset_index(
                drop=True))
        merged_df3 = pd.merge(df3, result_df3[['Household_No_new']], on='Household_No_new', how='left', indicator=True)
        df3['Merge_Flag'] = (merged_df3['_merge'] == 'both').astype(int).reset_index(drop=True)
        df3_0 = df3[df3['Merge_Flag'] == 1]

        df3_new = df3_0.copy().reset_index(drop=True)
        df3_new = df3_new.sort_values(
            by=['NsoBuilding_No', 'Building_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'MunTaoCode',
                'VilCode',
                'EA_Code_15', 'Household_No', 'Household_No_new', 'HouseNumber', 'RoomNumber', 'HouseholdMemberNumber'],
            ascending=True).reset_index(drop=True)
        df3_final = df3_new.copy().reset_index(drop=True)
        df3_final.drop(columns=['key_HH_df3', 'answered_questions', 'answered_questions_person', 'Merge_Flag'],
                    inplace=True)
        df3_answered_questions_0.drop(columns=['key_HH_df3', 'answered_questions', 'answered_questions_person'],
                                    inplace=True)

        df3_combined = pd.concat([df3_final, df3_answered_questions_0, df3_not], axis=0, ignore_index=True)
        ############################################################
        df3_final_08_17 = df3_combined.copy().reset_index(drop=True)
        df3_08_17_final_POP = df3_final_08_17.groupby('EA_Code_15')['Population_No_new'].count().reset_index(
            name='POP_Count')
        df3_08_17_final_HH = df3_final_08_17.groupby('EA_Code_15')['Household_No_new'].nunique().reset_index(
            name='HH_Count')
        ############################################################
        # df3_combined.drop(columns=['Household_No_new','Population_No_new'], inplace=True)
        df3_clearDup_08_17 = df3_combined.sort_values(
            by=['NsoBuilding_No', 'Building_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'MunTaoCode',
                'VilCode',
                'EA_Code_15', 'Household_No', 'HouseNumber', 'RoomNumber', 'HouseholdMemberNumber'],
            ascending=True).reset_index(drop=True)

        ## @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@##

        ## ส่วนที่ 4

        df4 = pd.DataFrame(data)

        df4['Household_No_new'] = df4['Household_No']
        mask_blank = df4['Household_No'].isna() | (df4['Household_No'].astype(str).str.strip() == '')
        df4.loc[mask_blank, 'Household_No_new'] = list(range(1, mask_blank.sum() + 1))

        df4['Population_No_new'] = df4['Population_No']
        mask_blank = df4['Population_No'].isna() | (df4['Population_No'].astype(str).str.strip() == '')
        df4.loc[mask_blank, 'Population_No_new'] = list(range(1, mask_blank.sum() + 1))

        df4_19 = df4[df4['BuildingType'].isin(['19'])].reset_index(drop=True)
        if df4_19.empty:

            df4_final = pd.DataFrame(columns=df4.columns)
            df4_combined = df4_final.copy()
            df4_clearDup_19 = df4_final.copy()

            df4_19_POP = pd.DataFrame(columns=['EA_Code_15', 'All_POP_Count'])
            df4_19_HH = pd.DataFrame(columns=['EA_Code_15', 'All_HH_Count'])
            df4_19_final_POP = pd.DataFrame(columns=['EA_Code_15', 'POP_Count'])
            df4_19_final_HH = pd.DataFrame(columns=['EA_Code_15', 'HH_Count'])

            df4_summary = pd.DataFrame(columns=['buildingtype', 'Case', 'Duplicated'])
        else:
            df4_19 = df4[df4['BuildingType'].isin(['19'])].reset_index(drop=True)
            df4_19_POP = df4_19.groupby('EA_Code_15')['Population_No_new'].count().reset_index(name='All_POP_Count')
            df4_19_HH = df4_19.groupby('EA_Code_15')['Household_No_new'].nunique().reset_index(name='All_HH_Count')

            # เลือกประเภทบ้าน/อาคาร/สิ่งปลูกสร้าง BuildingType = 19
            df4_not = df4[(df4['BuildingType'].isin(['19'])) & (~df4['Residing'].isin(['1']))].reset_index(drop=True)
            # df4 = df4[(df4['BuildingType'].isin([19]))&(df4['Residing'].isin([1]))].reset_index(drop=True)
            df4 = df4[(df4['BuildingType'].isin(['19'])) & (df4['Residing'].isin(['1']))]
            df4 = df4.copy().reset_index(drop=True)
            df4 = df4.sort_values(
                by=['NsoBuilding_No', 'Building_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode',
                    'MunTaoCode', 'VilCode',
                    'EA_Code_15', 'Household_No', 'Household_No_new', 'HouseNumber', 'RoomNumber', 'HouseholdMemberNumber'],
                ascending=True).reset_index(drop=True)

            # สร้างตัวแปรตรวจสอบความซ้ำซ้อนของข้อมูลระดับครัวเรือน
            key_columns_df4 = ['NsoBuilding_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'MunTaoCode',
                            'VilCode',
                            'EA_Code_15', 'HouseNumber', 'RoomNumber', 'RoadName', 'AlleyWayName', 'AlleyName',
                            'FirstName', 'LastName']
            # df4['key_HH_df4'] = df4[key_columns_df4].astype(str).agg(''.join, axis=1)
            df4['key_HH_df4'] = df4[key_columns_df4].fillna('').astype(str).sum(axis=1)

            # สร้างตัวแปรจำนวนข้อความที่ตอบในครัวเรือน ตั้งแต่ Title - Gender
            question_columns = ['Title', 'FirstName', 'LastName', 'Relationship', 'Sex', 'MonthOfBirth', 'YearOfBirth',
                                'Age_01', 'NationalityNumeric',
                                'EducationalAttainment', 'EmploymentStatus', 'NameInHouseholdRegister',
                                'NameInHouseholdRegisterOther',
                                'DurationOfResidence', 'MigrationCharecteristics', 'MovedFromProvince', 'MovedFromAbroad',
                                'MigrationReason', 'Gender']

            BLANK_VALUES = ['', 'nan', 'NaN', 'None']


            def clean_blank_df4(x):
                return pd.NA if str(x).strip() in BLANK_VALUES else x


            df4[question_columns] = df4[question_columns].apply(lambda col: col.map(clean_blank_df4)).astype("string")
            df4['answered_questions_person'] = df4[question_columns].notna().sum(axis=1)
            hh_answered = (
                df4.groupby('Household_No_new')['answered_questions_person'].sum().reset_index(name='answered_questions'))
            df4 = df4.merge(hh_answered, on='Household_No_new', how='left').reset_index(drop=True)

            df4_answered_questions_0 = df4[df4['answered_questions'].isin([0])].reset_index(drop=True)
            df4 = df4[~df4['answered_questions'].isin([0])].reset_index(drop=True)

            ####log case 19#################################################################################################################################################
            df4log = df4.groupby('Household_No_new')[['key_HH_df4', 'answered_questions', 'Population_CrtdDateEnd',
                                                    'Population_CrtdDateStart']].first().reset_index()
            # 1. มี CrtdDateEnd มี CrtdDateStart เลือกข้อถามมากที่สุด
            result0_4 = df4log[~df4log['Population_CrtdDateEnd'].notna()].copy()
            result1_4 = df4log[
                (df4log['Population_CrtdDateEnd'].notna()) &
                (df4log['answered_questions'] == df4log.groupby('key_HH_df4')['answered_questions'].transform('max'))
                ].copy()
            result2_4 = pd.concat([result0_4, result1_4], ignore_index=True)
            # result2_4.duplicated(subset=["key_HH_df4"]).sum()

            # 2. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน เลือก CrtdDateEnd ล่าสุด
            result3_4 = result2_4[~result2_4['Population_CrtdDateEnd'].notna()].copy()
            result4_4 = result2_4[
                (result2_4['Population_CrtdDateEnd'].notna()) &
                (result2_4['Population_CrtdDateEnd'] == result2_4.groupby('key_HH_df4')['Population_CrtdDateEnd'].transform(
                    'max'))
                ].copy()
            result5_4 = pd.concat([result3_4, result4_4], ignore_index=True)
            # result5_4.duplicated(subset=["key_HH_df4"]).sum()

            # 3. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateEnd เท่ากัน เลือก CrtdDateStart ล่าสุด
            result6_4 = result5_4[~result5_4['Population_CrtdDateEnd'].notna()].copy()
            result7_4 = result5_4[
                (result5_4['Population_CrtdDateEnd'].notna()) &
                (result5_4['Population_CrtdDateStart'] == result5_4.groupby('key_HH_df4')[
                    'Population_CrtdDateStart'].transform('max'))
                ].copy()
            result8_4 = pd.concat([result6_4, result7_4], ignore_index=True)
            # result8_4.duplicated(subset=["key_HH_df4"]).sum()

            # 4. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateEnd เท่ากัน และ CrtdDateStart เท่ากัน เลือกมาก 1 ครัวเรือน
            result9_4 = result8_4[~result8_4['Population_CrtdDateEnd'].notna()].copy()
            result10_4 = result8_4[result8_4['Population_CrtdDateEnd'].notna()].sort_values(
                by=['key_HH_df4', 'answered_questions', 'Population_CrtdDateEnd', 'Population_CrtdDateStart'],
                ascending=False).drop_duplicates(subset=['key_HH_df4'], keep='first')
            result11_4 = pd.concat([result9_4, result10_4], ignore_index=True)
            # result11_4.duplicated(subset=["key_HH_df4"]).sum()

            # 5. ไม่มี CrtdDateEnd มี CrtdDateStart เลือกข้อถามมากที่สุด
            result12_4 = result11_4[~result11_4['Population_CrtdDateStart'].notna()].copy()
            result13_4 = result11_4[
                (result11_4['Population_CrtdDateStart'].notna()) &
                (result11_4['answered_questions'] == result11_4.groupby('key_HH_df4')['answered_questions'].transform(
                    'max'))
                ].copy()
            result14_4 = pd.concat([result12_4, result13_4], ignore_index=True)
            # result14_4.duplicated(subset=["key_HH_df4"]).sum()

            # 6. ไม่มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน เลือก CrtdDateStart ล่าสุด
            result15_4 = result14_4[~result14_4['Population_CrtdDateStart'].notna()].copy()
            result16_4 = result14_4[
                (result14_4['Population_CrtdDateStart'].notna()) &
                (result14_4['Population_CrtdDateStart'] == result14_4.groupby('key_HH_df4')[
                    'Population_CrtdDateStart'].transform('max'))
                ].copy()
            result17_4 = pd.concat([result15_4, result16_4], ignore_index=True)
            # result17_4.duplicated(subset=["key_HH_df4"]).sum()

            # 7. ไม่มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateStart เท่ากัน เลือกมาก 1 ครัวเรือน
            result18_4 = result17_4[~result17_4['Population_CrtdDateStart'].notna()].copy()
            result19_4 = result17_4[result17_4['Population_CrtdDateStart'].notna()].sort_values(
                by=['key_HH_df4', 'answered_questions', 'Population_CrtdDateEnd', 'Population_CrtdDateStart'],
                ascending=False).drop_duplicates(subset=['key_HH_df4'], keep='first')
            result20_4 = pd.concat([result18_4, result19_4], ignore_index=True)
            # result20_4.duplicated(subset=["key_HH_df4"]).sum()

            # 8. มี CrtdDateEnd ไม่มี CrtdDateEnd เลือกที่มี CrtdDateEnd
            max_dates_4 = result20_4.groupby('key_HH_df4')['Population_CrtdDateEnd'].transform('max')
            mask_4 = (
                    (result20_4['Population_CrtdDateEnd'] == max_dates_4) |
                    (max_dates_4.isna() & result20_4['Population_CrtdDateEnd'].isna())
            )
            result21_4 = result20_4[mask_4].copy()
            # result21_4.duplicated(subset=["key_HH_df4"]).sum()

            # 9. มีการตอบข้อคำถาม ไม่มีการตอบข้อคำถาม เลือกที่มีการตอบข้อคำถาม
            result22_4 = result21_4[
                (result21_4['answered_questions'] == result21_4.groupby('key_HH_df4')['answered_questions'].transform(
                    'max'))
            ].copy()
            # result22_4.duplicated(subset=["key_HH_df4"]).sum()

            # 10. ไม่มี CrtdDateEnd ไม่ตอบข้อถาม มี CrtdDateStart เลือก CrtdDateStart ล่าสุด
            max_dates_4 = result22_4.groupby('key_HH_df4')['Population_CrtdDateStart'].transform('max')
            mask_4 = (
                    (result22_4['Population_CrtdDateStart'] == max_dates_4) |
                    (max_dates_4.isna() & result22_4['Population_CrtdDateStart'].isna())
            )
            result23_4 = result22_4[mask_4].copy()
            # result23_4.duplicated(subset=["key_HH_df4"]).sum()

            # 11. ไม่มี CrtdDateEnd ไม่การตอบข้อคำถาม ไม่มี CrtdDateStart เลือกมา 1 ครัวเรือน
            result24_4 = result23_4.sort_values(
                by=['key_HH_df4', 'answered_questions', 'Population_CrtdDateEnd', 'Population_CrtdDateStart'],
                ascending=False).drop_duplicates(subset=['key_HH_df4'], keep='first')
            # result24_4.duplicated(subset=["key_HH_df4"]).sum()

            results_log_4 = {
                "จำนวนครัวเรือนซ้ำทั้งหมด": df4log,
                "1. มี CrtdDateEnd มี CrtdDateStart เลือกข้อถามมากที่สุด": result2_4,
                "2. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน เลือก CrtdDateEnd ล่าสุด": result5_4,
                "3. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateEnd เท่ากัน เลือก CrtdDateStart ล่าสุด": result8_4,
                "4. มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateEnd เท่ากัน และ CrtdDateStart เท่ากัน เลือกมาก 1 ครัวเรือน": result11_4,
                "5. ไม่มี CrtdDateEnd มี CrtdDateStart เลือกข้อถามมากที่สุด": result14_4,
                "6. ไม่มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน เลือก CrtdDateStart ล่าสุด": result17_4,
                "7. ไม่มี CrtdDateEnd มี CrtdDateStart จำนวนข้อถามเท่ากัน CrtdDateStart เท่ากัน เลือกมาก 1 ครัวเรือน": result20_4,
                "8. มี CrtdDateEnd ไม่มี CrtdDateEnd เลือกที่มี CrtdDateEnd": result21_4,
                "9. มีการตอบข้อคำถาม ไม่มีการตอบข้อคำถาม เลือกที่มีการตอบข้อคำถาม": result22_4,
                "10. ไม่มี CrtdDateEnd ไม่ตอบข้อถาม มี CrtdDateStart เลือก CrtdDateStart ล่าสุด": result23_4,
                "11. ไม่มี CrtdDateEnd ไม่การตอบข้อคำถาม ไม่มี CrtdDateStart เลือกมา 1 ครัวเรือน": result24_4
            }
            df4_summary = pd.DataFrame([
                {
                    "buildingtype": "19",
                    "Case": name,
                    "Duplicated": df.duplicated(subset=["key_HH_df4"]).sum()}
                for name, df in results_log_4.items()
            ])
            ###############################################################################################################################################

            # ลบครัวเรือนที่ซ้ำซ้อน
            df4_1 = df4.groupby('Household_No_new')[['key_HH_df4', 'answered_questions', 'Population_CrtdDateEnd',
                                                    'Population_CrtdDateStart']].first().reset_index()


            def select_best_df4(group):
                with_end = group.dropna(subset=['Population_CrtdDateEnd'])

                def safe_filter_df4(df4, column):
                    if df4.empty or column not in df4.columns:
                        return df4
                    val = df4[column].max()
                    return df4[df4[column] == val]

                if not with_end.empty:
                    filtered = safe_filter_df4(with_end, 'answered_questions')
                    if len(filtered) == 1:
                        return filtered.iloc[0]

                    filtered = safe_filter_df4(filtered, 'Population_CrtdDateEnd')
                    if len(filtered) == 1:
                        return filtered.iloc[0]

                    filtered = safe_filter_df4(filtered, 'Population_CrtdDateStart')
                    if len(filtered) == 1:
                        return filtered.iloc[0]
                    else:
                        return group.iloc[0] if not group.empty else pd.Series(dtype=object)

                else:
                    filtered = safe_filter_df4(group, 'answered_questions')
                    if len(filtered) == 1:
                        return filtered.iloc[0]

                    filtered = safe_filter_df4(filtered, 'Population_CrtdDateStart')
                    if len(filtered) == 1:
                        return filtered.iloc[0]
                    else:
                        return group.iloc[0] if not group.empty else pd.Series(dtype=object)


            result_df4 = (
                df4_1.groupby('key_HH_df4', group_keys=False).apply(select_best_df4, include_groups=False).reset_index(
                    drop=True))
            merged_df4 = pd.merge(df4, result_df4[['Household_No_new']], on='Household_No_new', how='left', indicator=True)
            df4['Merge_Flag'] = (merged_df4['_merge'] == 'both').astype(int).reset_index(drop=True)

            df4_0 = df4[df4['Merge_Flag'] == 1]
            df4_new = df4_0.copy().reset_index(drop=True)
            df4_new = df4_new.sort_values(
                by=['NsoBuilding_No', 'Building_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode',
                    'MunTaoCode', 'VilCode',
                    'EA_Code_15', 'Household_No', 'Household_No_new', 'HouseNumber', 'RoomNumber', 'HouseholdMemberNumber'],
                ascending=True).reset_index(drop=True)
            df4_final = df4_new.copy().reset_index(drop=True)
            df4_final.drop(columns=['key_HH_df4', 'answered_questions', 'answered_questions_person', 'Merge_Flag'],
                        inplace=True)
            df4_answered_questions_0.drop(columns=['key_HH_df4', 'answered_questions', 'answered_questions_person'],
                                        inplace=True)

            df4_combined = pd.concat([df4_final, df4_answered_questions_0, df4_not], axis=0, ignore_index=True)
            ############################################################
            df4_final_19 = df4_combined.copy().reset_index(drop=True)
            df4_19_final_POP = df4_final_19.groupby('EA_Code_15')['Population_No_new'].count().reset_index(name='POP_Count')
            df4_19_final_HH = df4_final_19.groupby('EA_Code_15')['Household_No_new'].nunique().reset_index(name='HH_Count')
            ############################################################
            # df4_combined.drop(columns=['Household_No_new','Population_No_new'], inplace=True)
            df4_clearDup_19 = df4_combined.sort_values(
                by=['NsoBuilding_No', 'Building_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode',
                    'MunTaoCode', 'VilCode',
                    'EA_Code_15', 'Household_No', 'HouseNumber', 'RoomNumber', 'HouseholdMemberNumber'],
                ascending=True).reset_index(drop=True)

        ## /////////////////////////////////////////////////////////////////////#
        ## ส่วนที่ 5

        # รวมข้อมูลหลังจากตรวจสอบความซ้ำซ้อนของข้อมูลทุกประเภทบ้าน/อาคาร/สิ่งปลูกสร้าง BuildingType = 01-19
        df_combined = pd.concat([df1_clearDup_01_07_18, df2_clearDup_02_06, df3_clearDup_08_17, df4_clearDup_19], axis=0,
                                ignore_index=True)
        df_final = df_combined.sort_values(
            by=['NsoBuilding_No', 'Building_No', 'RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'MunTaoCode',
                'VilCode',
                'EA_Code_15', 'Household_No', 'HouseNumber', 'RoomNumber', 'HouseholdMemberNumber'],
            ascending=True).reset_index(drop=True)
        #df_final.to_csv(f'check_dup_blue_update2_ProvCode{ProvCode}_clearDup_test_{current_datetime_str}.csv', index=False,
                        #encoding='utf-8-sig')



        print("รายงาน Excel ถูกสร้างเรียบร้อยแล้ว")

        print("Shape of df_final_combined:", df_final.shape)

        ## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

        # log

        # สร้างลิสต์รวม DataFrames
        dfs_1 = [df1_01_07_18_POP, df1_01_07_18_HH, df1_01_07_18_final_POP, df1_01_07_18_final_HH]
        dfs_2 = [df2_02_06_POP, df2_02_06_HH, df2_02_06_final_POP, df2_02_06_final_HH]
        dfs_3 = [df3_08_17_POP, df3_08_17_HH, df3_08_17_final_POP, df3_08_17_final_HH]
        dfs_4 = [df4_19_POP, df4_19_HH, df4_19_final_POP, df4_19_final_HH]

        # รวมทุกตารางด้วย merge แบบ outer บนคีย์ 'EA_Code_15'
        df_combined_1 = reduce(lambda left, right: pd.merge(left, right, on='EA_Code_15', how='outer'), dfs_1)
        df_combined_2 = reduce(lambda left, right: pd.merge(left, right, on='EA_Code_15', how='outer'), dfs_2)
        df_combined_3 = reduce(lambda left, right: pd.merge(left, right, on='EA_Code_15', how='outer'), dfs_3)
        df_combined_4 = reduce(lambda left, right: pd.merge(left, right, on='EA_Code_15', how='outer'), dfs_4)

        df_combined_1['BuildingType'] = '01_07_18'
        df_combined_2['BuildingType'] = '02_06'
        df_combined_3['BuildingType'] = '08_17'
        df_combined_4['BuildingType'] = '19'

        df_all_combined = pd.concat([df_combined_1, df_combined_2, df_combined_3, df_combined_4], ignore_index=True)
        df_all_combined.to_csv(f'log_online_survey_check_dup_blue_update2_ProvCode{ProvCode}_clearDup_test_{current_datetime_str}.csv',
                            index=False, encoding='utf-8-sig')

        ## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

        # log case

        log_by_case = pd.concat([df1_summary, df2_summary, df3_summary, df4_summary], ignore_index=True)
        log_by_case.to_csv(f'logbycase_online_survey_check_dup_blue_update2_ProvCode{ProvCode}_clearDup_test_{current_datetime_str}.csv',
                        index=False, encoding='utf-8-sig')

        # จัดเรียงข้อมูล
        # df_final = df_final.sort_values(
        #    by=['RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'EA_No', 'VilCode', 'Household_CrtdByName', 'Building_No', 'Household_No', 'HouseholdMemberNumber'])
        df_final = df_final.sort_values(
            by=['RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'EA_No', 'VilCode', 'Household_CrtdByName',
                'Building_No', 'Household_No', 'HouseholdMemberNumber'])

        print('sort data เสร็จแล้ว ถัดไป เลือกคอลัมน์')

        # เลือกคอลัมน์ที่ต้องการ Insert/Update
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
            'NumberOfHousueholdMember', 'HouseholdStatus', 'Household_IsActive', 'Household_Note', 'Household_CrtdByCode',
            'Household_CrtdByName', 'Household_CrtdDateStart', 'Household_CrtdDateEnd', 'Household_UpdateDateStart',
            'Household_UpdateDateEnd', 'Population_No', 'Building_No_POP', 'Household_No_POP', 'HouseholdMemberNumber',
            'Title', 'TitleOther', 'FirstName', 'LastName', 'Relationship', 'Sex', 'MonthOfBirth', 'YearOfBirth', 'Age_01',
            'NationalityNumeric', 'EducationalAttainment', 'EmploymentStatus', 'NameInHouseholdRegister',
            'NameInHouseholdRegisterOther', 'DurationOfResidence', 'DurationOfResidence_Text', 'MigrationCharecteristics',
            'MovedFromProvince', 'MovedFromAbroad', 'MigrationReason', 'MigrationReasonOther', 'Gender', 'PopulationStatus',
            'Population_IsActive', 'Population_Note', 'Longitude_POP', 'Population_CrtdByCode', 'Population_CrtdByName',
            'Population_CrtdDateStart',
            'Population_CrtdDateEnd', 'Population_UpdateDateStart', 'Population_UpdateDateEnd', 'ApproveStatusSup',
            'ApproveDateSup',
            'ApproveStatusProv', 'ApproveDateProv', 'm_control_ea_CrtdByCode', 'ADV_EA', 'InternetAr', 'DataSources'
        ]
        df_filtered = df_final[selected_columns].copy()
        print("Shape of df_filtered:", df_filtered.shape)

        time_end_check_dup = datetime.datetime.now()
        print("เวลาสิ้นสุด check dup:", time_end_check_dup)

        time_difference_check_dup = time_end_check_dup - time_begin
        print("ความต่างของเวลา:", time_difference_check_dup)

        # --- เพิ่มโค้ดแก้ไข NaT ตรงนี้ ---
        print("กำลังตรวจสอบและแก้ไขค่าวันที่/เวลา (NaT) ก่อนส่งเข้าฐานข้อมูล...")
        for col in df_filtered.columns:
            if pd.api.types.is_datetime64_any_dtype(df_filtered[col]):
                initial_nat_count = df_filtered[col].isna().sum()
                if initial_nat_count > 0:
                    df_filtered[col] = df_filtered[col].replace({pd.NaT: None})  # หรือ datetime.datetime(1900, 1, 1)
                    print(f"  แก้ไข NaT ในคอลัมน์ '{col}': {initial_nat_count} รายการ")
        # --- สิ้นสุดโค้ดแก้ไข NaT ---

        print("กำลังแปลงค่าว่าง (pd.NA) ให้เป็น '' (Blank) ก่อนส่งเข้าฐานข้อมูล...")
        df_filtered = df_filtered.replace({pd.NA: ''})

        # --- ซิงค์ข้อมูลกับฐานข้อมูล r_online_survey_chk_dup (เฉพาะการเพิ่มข้อมูล) ---

        # try:
        #     placeholders = ', '.join(['?'] * len(selected_columns))
        #     insert_query_r_online_survey_chk_dup = f"INSERT INTO r_online_survey_chk_dup ({', '.join(selected_columns)}) VALUES ({placeholders})"

        #     total_inserted_count = 0

        #     # --- ตรวจสอบและ Insert ลง r_online_survey_chk_dup ---
        #     print(f"\n{BLUE}กำลังตรวจสอบว่ามี ProvCode = '{ProvCode}' ในฐานข้อมูล r_online_survey_chk_dup หรือไม่...{RESET}")
        #     query_check_r_online_survey_chk_dup_existing = f"SELECT COUNT(*) FROM r_online_survey_chk_dup WHERE ProvCode = '{ProvCode}'"
        #     cursor.execute(query_check_r_online_survey_chk_dup_existing)
        #     r_online_survey_chk_dup_existing_count = cursor.fetchone()[0]

        #     if r_online_survey_chk_dup_existing_count == 0:
        #         print(f"{YELLOW}ไม่พบข้อมูลสำหรับ ProvCode = '{ProvCode}' ใน r_online_survey_chk_dup จะทำการ INSERT ข้อมูลทั้งหมด{RESET}")
        #         inserted_r_online_survey_chk_dup_count = 0
        #         for index, row in df_filtered.iterrows():
        #             try:
        #                 cursor.execute(insert_query_r_online_survey_chk_dup, [row[col] for col in selected_columns])
        #                 inserted_r_online_survey_chk_dup_count += 1
        #             except pyodbc.Error as ex:
        #                 sqlstate = ex.args[0]
        #                 print(
        #                     f"{RED}เกิดข้อผิดพลาดในการ INSERT ข้อมูลลง r_online_survey_chk_dup: {sqlstate} - {ex} - Values: {list(row)}{RESET}")
        #                 sql_conn.rollback()
        #                 raise
        #         print(
        #             f"{PINK}Inserted {inserted_r_online_survey_chk_dup_count} new records for ProvCode = '{ProvCode}' into r_online_survey_chk_dup.{RESET}")
        #         total_inserted_count += inserted_r_online_survey_chk_dup_count
        #     else:
        #         print(
        #             f"{YELLOW}พบข้อมูลสำหรับ ProvCode = '{ProvCode}' ใน r_online_survey_chk_dup แล้ว จะไม่ดำเนินการ INSERT เพิ่มเติม{RESET}")

        #     sql_conn.commit()
        #     print(
        #         f"{GREEN}การดำเนินการ INSERT ข้อมูลสำหรับ ProvCode = '{ProvCode}' ใน r_online_survey_chk_dup เสร็จสมบูรณ์ (รวม {total_inserted_count} รายการใหม่){RESET}")

        # except pyodbc.Error as ex:
        #     sqlstate = ex.args[0]
        #     print(f"{RED}เกิดข้อผิดพลาดในการซิงค์ข้อมูล: {sqlstate} - {ex}{RESET}")
        #     sql_conn.rollback()
        #     raise
        # except Exception as e:
        #     print(f"{RED}เกิดข้อผิดพลาดที่ไม่คาดคิดในการซิงค์ข้อมูล: {e}{RESET}")
        #     if sql_conn:
        #         sql_conn.rollback()
        #     raise

        # --- สิ้นสุดการซิงค์ข้อมูล ---

        time_end = datetime.datetime.now()
        print("เวลาสิ้นสุด:", time_end)
        time_difference = time_end - time_begin
        print("ความต่างของเวลา:", time_difference)

        single_report_df = pd.DataFrame([{
            'ProvCode': ProvCode,
            'time_begin': time_begin,
            'time_end_check_dup': time_end_check_dup,
            'time_difference_check_dup': time_difference_check_dup,
            'time_end': time_end,
            'time_difference': str(time_difference),
            'Shape of df': str(data.shape),
            'Shape of df_final_combined': str(df_final.shape),
            'Shape of df_filtered': str(df_filtered.shape)
        }], columns=report_columns)
        
        # ตรวจสอบว่าไฟล์ report.csv มีอยู่แล้วหรือยัง
        # ถ้ายังไม่มี ให้สร้างใหม่พร้อม header
        # ถ้ามีอยู่แล้ว ให้ต่อท้ายข้อมูล (append) โดยไม่ใส่ header
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
