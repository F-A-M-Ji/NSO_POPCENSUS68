import pyodbc
import pandas as pd
import warnings
import datetime
import openpyxl
import numpy as np
import os

warnings.filterwarnings("ignore")
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

# --------------------------- แก้ array เป็นรหัสจังหวัด ---------------------------
PROVINCE_CODES_TO_RUN = ["82", "84", "85", "86"]

# --------------------------- แก้ ที่อยู่ไฟล์ report.xlsx ---------------------------
EXCEL_REPORT_PATH = "C:/Users/NSO/Desktop/pop_run_data/popcensus/report.csv"

# ProvCode = '16'
current_datetime_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

# ANSI escape codes สำหรับสีต่างๆ
GREEN = "\033[92m"
RED = "\033[91m"
BLUE = "\033[94m"
YELLOW = "\033[93m"
RESET = "\033[0m"
ORANGE = "\033[38;2;255;165;0m"
PINK = "\033[38;2;255;105;180m"

unique_columns = ["Population_No"]  # ใช้สำหรับระบุคอลัมน์ที่เป็นคีย์หลักในการ UPDATE

sql_conn = None
cursor = None

try:
    
    # --------------------------- แก้ server ---------------------------
    sql_conn = pyodbc.connect(
        "DRIVER={SQL Server};SERVER=192.168.0.204;DATABASE=pop6768;UID=pdan;PWD=P@ssw0rd12#$"
    )
    print("Connection successful")
    cursor = sql_conn.cursor()

        # ---

    for ProvCode in PROVINCE_CODES_TO_RUN:
        print(f"\n{BLUE}====================================================={RESET}")
        print(f"{BLUE}>>> เริ่มกระบวนการสำหรับ ProvCode: {ProvCode}{RESET}")
        print(f"{BLUE}====================================================={RESET}")

        # ---

        time_begin = datetime.datetime.now()
        print("เวลาเริ่มต้น:", time_begin)
        # ========================================== เวลาเริ่มต้น(time_begin)

        # ดึงข้อมูลจาก r_building
        query_building = f"""SELECT
                                   EA_Code_15, Building_No, BuildingCode, NsoBuilding_No, RegCode, RegName, ProvCode, ProvName, DistCode, 
                                   DistName, SubDistCode, SubDistName, AreaCode, AreaName, VilCode, VilName, 
                                   HouseNumber, RoomNumber, RoadName, AlleyWayName, AlleyName, BuildingName, BuildingNumber, 
                                   BuildingType, BuildingTypeOther, Residing, HouseholdEnumeration, HouseholdEnumerationOther, 
                                   HouseholdEnumeration_1, HouseholdEnumeration_2, HouseholdEnumeration_3, HouseholdType, 
                                   NumberOfHousehold, TotalRoom, RoomVacant, RoomResidence, NewBuilding, BuildingStatus, 
                                   Respondent, ResponseChannel, IsBuilding, IsActive, Note, CrtdByCode, CrtdByName, Building_No_Old, 
                                   TotalPopulation, TotalMale, TotalFemale, ApproveStatusSupCode, IsMapping, CrtdDateStart, CrtdDateEnd, UpdateDateStart, UpdateDateEnd
                              FROM r_building
                              WHERE (ProvCode = '{ProvCode}') AND IsActive = 1 AND (AssignDetailId != '' or AssignDetailId IS NOT NULL) AND 
                              (ApproveStatusSupCode = 'W' OR ApproveStatusSupCode = '') """
        df_building = pd.read_sql(query_building, sql_conn).fillna("")
        print("Shape of df_building:", df_building.shape)
        # ============================================== ข้อมูลจากตาราง Building (Shape of df_building)

        # เปลี่ยนชื่อคอลัมน์ 'IsActive' เป็น 'IsActive_Buil'
        df_building.rename(
            columns={
                "IsActive": "Building_IsActive",
                "Note": "Building_Note",
                "CrtdByCode": "Building_CrtdByCode",
                "CrtdByName": "Building_CrtdByName",
                "CrtdDateStart": "Building_CrtdDateStart",
                "CrtdDateEnd": "Building_CrtdDateEnd",
                "UpdateDateStart": "Building_UpdateDateStart",
                "UpdateDateEnd": "Building_UpdateDateEnd",
            },
            inplace=True,
        )

        print("กำลัง Impute ค่า...")
        if not df_building.empty:
            df_building.loc[(df_building["AreaCode"] == "1"), "VilCode"] = "00"
            print("Impute ค่า VilCode เป็น '00' เมื่อ AreaCode เป็น '1' แล้ว")
        else:
            print("DataFrame df_building ว่าง ไม่สามารถ Impute ค่า VilCode ได้")

        # ลบคำว่า "เขต" ออกจากคอลัมน์ 'distric' โดยใช้ lstrip
        # df_building['DistName'] = df_building['DistName'].str.lstrip('เขต')

        if "ProvCode" in df_building.columns:
            df_building.loc[df_building["ProvCode"] == 10, "DistName"] = (
                df_building.loc[df_building["ProvCode"] == 10, "DistName"].str.lstrip(
                    "เขต"
                )
            )
        else:
            print("คอลัมน์ 'ProvCode' ไม่มีอยู่ใน DataFrame.")

        # Impute RegCode And ProvCode
        # --------------------------- แก้ ที่อยู่ไฟล์ reg_prov.csv ---------------------------
        df_reg_prov = pd.read_csv(
            "C:/Users/NSO/Desktop/pop_run_data/popcensus/data/reg_prov.csv",
            encoding="utf-8",
        )
        df_reg_prov["RegCode"] = df_reg_prov["RegCode"].astype(str)
        df_reg_prov["ProvCode"] = df_reg_prov["ProvCode"].astype(str)

        try:
            df_merged = pd.merge(
                df_building,
                df_reg_prov,
                on=["RegCode", "ProvCode"],
                how="left",
                suffixes=("_building", "_control"),
            )

            mismatched_regname = df_merged[
                (df_merged["RegName_building"] != df_merged["RegName_control"])
                & df_merged["RegName_control"].notna()
            ]
            mismatched_provname = df_merged[
                (df_merged["ProvName_building"] != df_merged["ProvName_control"])
                & df_merged["ProvName_control"].notna()
            ]

            if not mismatched_regname.empty:
                print(
                    f"\n{RED}--- ค่า RegName ที่ไม่ตรงกัน (df_building vs m_control_ea) ---{RESET}"
                )
                print(
                    mismatched_regname[
                        ["RegCode", "ProvCode", "RegName_building", "RegName_control"]
                    ].drop_duplicates()
                )
            else:
                print(f"\n{GREEN}ไม่พบค่า RegName ที่ไม่ตรงกัน{RESET}")

            if not mismatched_provname.empty:
                print(
                    f"\n{RED}--- ค่า ProvName ที่ไม่ตรงกัน (df_building vs m_control_ea) ---{RESET}"
                )
                print(
                    mismatched_provname[
                        ["RegCode", "ProvCode", "ProvName_building", "ProvName_control"]
                    ].drop_duplicates()
                )
            else:
                print(f"\n{GREEN}ไม่พบค่า ProvName ที่ไม่ตรงกัน{RESET}")

            all_mismatched_indices = mismatched_regname.index.union(
                mismatched_provname.index
            )

            if not all_mismatched_indices.empty:
                print(
                    f"{RED}พบค่า RegName หรือ ProvName ที่ไม่ตรงกันจำนวน {len(all_mismatched_indices)} รายการ:{RESET}"
                )
                mismatched_locations = df_merged.loc[
                    all_mismatched_indices, ["RegCode", "ProvCode"]
                ].drop_duplicates()

                for _, row_loc in mismatched_locations.iterrows():
                    reg_code = row_loc["RegCode"]
                    prov_code = row_loc["ProvCode"]
                    correct_data = df_reg_prov[
                        (df_reg_prov["RegCode"] == reg_code)
                        & (df_reg_prov["ProvCode"] == prov_code)
                    ]

                    if not correct_data.empty:
                        correct_regname = correct_data["RegName"].iloc[0]
                        correct_provname = correct_data["ProvName"].iloc[0]
                        rows_to_update = df_building[
                            (df_building["RegCode"] == reg_code)
                            & (df_building["ProvCode"] == prov_code)
                        ].index
                        if not rows_to_update.empty:
                            df_building.loc[rows_to_update, "RegName"] = correct_regname
                            df_building.loc[rows_to_update, "ProvName"] = (
                                correct_provname
                            )
                            print(
                                f"  {ORANGE}แก้ไข RegName และ ProvName สำหรับ RegCode: {reg_code}, ProvCode: {prov_code} เป็น RegName: '{correct_regname}', ProvName: '{correct_provname}'{RESET}"
                            )
                        else:
                            print(
                                f"  {YELLOW}ไม่พบข้อมูลใน df_building สำหรับ RegCode: {reg_code}, ProvCode: {prov_code} เพื่อแก้ไข{RESET}"
                            )
                    else:
                        print(
                            f"  {YELLOW}ไม่พบค่าที่ถูกต้องใน m_control_ea สำหรับ RegCode: {reg_code}, ProvCode: {prov_code}{RESET}"
                        )
            else:
                print(f"{GREEN}ไม่พบค่า RegName, ProvName ที่ไม่ตรงกัน ข้อมูลถูกต้องแล้ว{RESET}")
        except Exception as e:
            print(f"{RED}เกิดข้อผิดพลาดในการตรวจสอบและแก้ไขข้อมูล: {e}{RESET}")

        # ดึงข้อมูลจาก r_household
        query_household = f"""SELECT
                                   Household_No,
                                   Building_No,
                                   HouseholdNumber,
                                   HouseholdEnumeration,
                                   ConstructionMaterial,
                                   ConstructionMaterialOther,
                                   TenureResidence,
                                   TenureResidenceOther,
                                   TenureLand,
                                   TenureLandOther,
                                   NumberOfHousueholdMember,
                                   HouseholdStatus,
                                   IsActive,
                                   Note,
                                   CrtdByCode,
                                   CrtdByName,
                                   CrtdDateStart,
                                   CrtdDateEnd,
                                   UpdateDateStart,
                                   UpdateDateEnd
                            FROM r_household
                              WHERE (ProvCode = '{ProvCode}') AND IsActive = 1 AND (ApproveStatusSupCode = 'W' OR ApproveStatusSupCode = '') """
        df_household = pd.read_sql(query_household, sql_conn).fillna("")
        print("Shape of df_household:", df_household.shape)
        # ========================================== ข้อมูลจากตาราง Household (Shape of df_household)

        # เปลี่ยนชื่อคอลัมน์ 'IsActive' เป็น 'IsActive_Buil'
        df_household.rename(
            columns={
                "Building_No": "Building_No_HH",
                "HouseholdEnumeration": "HouseholdEnumeration_HH",
                "IsActive": "Household_IsActive",
                "Note": "Household_Note",
                "CrtdByCode": "Household_CrtdByCode",
                "CrtdByName": "Household_CrtdByName",
                "CrtdDateStart": "Household_CrtdDateStart",
                "CrtdDateEnd": "Household_CrtdDateEnd",
                "UpdateDateStart": "Household_UpdateDateStart",
                "UpdateDateEnd": "Household_UpdateDateEnd",
            },
            inplace=True,
        )

        # Merge df_building กับ df_household
        df_merged_bh = pd.merge(
            df_building,
            df_household,
            left_on="Building_No",
            right_on="Building_No_HH",
            how="left",
        )
        print("Shape of df_merged_bh:", df_merged_bh.shape)
        # ============================================= ข้อมูลจากการ join BH (Shape of df_merged_bh)

        # ดึงข้อมูลจาก r_population
        query_population = f"""SELECT 
                                   Population_No,
                                   Building_No,
                                   Household_No,
                                   HouseholdMemberNumber,
                                   Title,
                                   TitleOther,
                                   FirstName,
                                   LastName,
                                   Relationship,
                                   Sex,
                                   MonthOfBirth,
                                   YearOfBirth,
                                   Age_01,
                                   NationalityNumeric,
                                   EducationalAttainment,
                                   EmploymentStatus,
                                   NameInHouseholdRegister,
                                   NameInHouseholdRegisterOther,
                                   DurationOfResidence,
                                   DurationOfResidence_Text,
                                   MigrationCharecteristics,
                                   MovedFromProvince,
                                   MovedFromAbroad,
                                   MigrationReason,
                                   MigrationReasonOther,
                                   Gender,
                                   PopulationStatus,
                                   IsActive,
                                   Note,
                                   Longitude,
                                   CrtdByCode,
                                   CrtdByName,
                                   CrtdDateStart,
                                   CrtdDateEnd,
                                   UpdateDateStart,
                                   UpdateDateEnd
                               FROM r_population
                              WHERE (ProvCode = '{ProvCode}') AND IsActive = 1 AND (ApproveStatusSupCode = 'W' OR ApproveStatusSupCode = '') """
        df_population = pd.read_sql(query_population, sql_conn).fillna("")
        print("Shape of df_population:", df_population.shape)
        # ============================================= ข้อมูลจากตาราง Population (Shape of df_population)

        # เปลี่ยนชื่อคอลัมน์
        df_population.rename(
            columns={
                "Building_No": "Building_No_POP",
                "Household_No": "Household_No_POP",
                "IsActive": "Population_IsActive",
                "Note": "Population_Note",
                "CrtdByCode": "Population_CrtdByCode",
                "CrtdByName": "Population_CrtdByName",
                "CrtdDateStart": "Population_CrtdDateStart",
                "CrtdDateEnd": "Population_CrtdDateEnd",
                "UpdateDateStart": "Population_UpdateDateStart",
                "UpdateDateEnd": "Population_UpdateDateEnd",
                "Longitude": "Longitude_POP",
            },
            inplace=True,
        )

        str(df_population["Age_01"]).zfill(3)

        # Merge df_merged_bh กับ df_population
        df_merged_bhp = pd.merge(
            df_merged_bh,
            df_population,
            left_on=["Building_No", "Household_No"],
            right_on=["Building_No_POP", "Household_No_POP"],
            how="left",
        )
        df_merged_bhp = df_merged_bhp.fillna("")
        print("Shape of df_merged_bhp_merged:", df_merged_bhp.shape)
        # =============================================== ข้อมูลจากการ join BHP (Shape of df_merged_bhp_merged)

        print(
            "คอลัมน์ที่ซ้ำกันใน df_merged_bhp_merged:",
            df_merged_bhp.columns[df_merged_bhp.columns.duplicated()],
        )

        print("Join bhp เสร็จแล้ว ถัดไป Query m_control_ea")

        query_m_control_ea = f"""SELECT 
                                EA_CODE_15, ApproveStatusSup, ApproveDateSup, ApproveStatusProv, ApproveDateProv, MuniCode, 
                                MuniName, MunTaoCode, MunTaoName, SubAdminCode, SubAdminName, CommuCode, CommuName, EA_No, 
                                AreaCode, VillCode, VillName, CrtdByCode, ADV_EA, InternetAr 
                                FROM m_control_ea WHERE (ProvCode = '{ProvCode}') """

        df_m_control_ea = pd.read_sql(query_m_control_ea, sql_conn).fillna("")
        print("Shape of df_m_control_ea:", df_m_control_ea.shape)
        # ================================================ ข้อมูลจากตาราง m_control_ea (Shape of df_m_control_ea)

        df_m_control_ea.rename(
            columns={
                "CrtdByCode": "m_control_ea_CrtdByCode",
                "AreaCode": "AreaCode_m",
                "VillCode": "VilCode_m",
                "VillName": "VilName_m",
                "EA_CODE_15": "EA_Code_15_m",
            },
            inplace=True,
        )

        str(df_m_control_ea["EA_No"]).zfill(4)

        df_final_merged = pd.merge(
            df_merged_bhp,
            df_m_control_ea,
            left_on=["EA_Code_15"],
            right_on=["EA_Code_15_m"],
            how="left",
        )
        df_final_merged = df_final_merged.fillna("")
        print("Shape of df_final_merged:", df_final_merged.shape)
        # =========================================== ข้อมูลจากการ join BHPM (Shape of df_final_merged)

        print("Join bhp กับ m_control_ea เสร็จแล้ว ถัดไป Query กรองขั้นสุดท้าย")

        # กรองขั้นสุดท้าย
        df_final_merged = df_final_merged[
            (df_final_merged["ApproveStatusProv"] == "Y")
        ].copy()
        print("ขนาดของ df_final_merged หลังกรองตามเงื่อนไขทั้งหมด:", df_final_merged.shape)
        # =========================================== ข้อมูลหลังกรองตามเงื่อนไขทั้งหมด

        # print(df_final_merged.columns)

        print("กรองขั้นสุดท้าย เสร็จแล้ว ถัดไป สร้าง IDEN")

        # ฟังก์ชันสำหรับสร้างตัวเลขสุ่ม N หลักแบบ Unique ทั่วทั้ง DataFrame
        def generate_all_unique_random_suffixes(num_records, num_digits=8):
            max_num_possible = 10**num_digits

            if num_records > max_num_possible:
                raise ValueError(
                    f"{RED}ข้อผิดพลาดร้ายแรง: จำนวนเรคคอร์ดทั้งหมด ({num_records}) เกินกว่าจำนวนตัวเลขสุ่ม {num_digits} หลัก ({max_num_possible}). "
                    f"ไม่สามารถสร้างเลขสุ่มที่ไม่ซ้ำกันสำหรับทุกแถวได้! กรุณาเพิ่มจำนวนหลักสุ่ม (num_digits).{RESET}"
                )

            # สร้างรายการของสตริงตัวเลข 0 ถึง (max_num_possible - 1)
            # แล้วสุ่มเลือกออกมา num_records ตัวโดยไม่ซ้ำกันสำหรับทุกแถวใน DataFrame
            return [
                str(i).zfill(num_digits)
                for i in np.random.choice(max_num_possible, num_records, replace=False)
            ]

        # กำหนด IDEN_prefix ให้เป็น EA_Code_15 เพียงอย่างเดียว
        df_final_merged["IDEN_prefix"] = df_final_merged["EA_Code_15"].astype(str)

        # สร้างเลขสุ่ม 8 หลักที่ไม่ซ้ำกันสำหรับทุกแถวใน DataFrame ทั้งหมด
        # ตรวจสอบขนาดของ DataFrame ก่อนเรียกฟังก์ชัน
        total_records = len(df_final_merged)
        print(f"กำลังสร้างเลขสุ่ม 8 หลักที่ไม่ซ้ำกันสำหรับ {total_records} แถว...")

        # ตรวจสอบว่าจำนวนเลขสุ่ม 8 หลักเพียงพอสำหรับทุกแถวใน DataFrame หรือไม่
        max_num_possible_suffixes = 10**8  # 100,000,000
        if total_records > max_num_possible_suffixes:
            print(
                f"{RED}**ข้อผิดพลาดร้ายแรง: จำนวนเรคคอร์ดทั้งหมด ({total_records}) เกินกว่าจำนวนตัวเลขสุ่ม 8 หลัก ({max_num_possible_suffixes}). "
                f"ไม่สามารถสร้างเลขสุ่มที่ไม่ซ้ำกันสำหรับทุกแถวได้! กรุณาเพิ่มจำนวนหลักสุ่มใน generate_all_unique_random_suffixes (เช่น เป็น 9 หรือ 10 หลัก).{RESET}"
            )
            raise ValueError("Suffix pool exhausted for all records.")

        # สร้าง Suffixes ชุดเดียวสำหรับ DataFrame ทั้งหมด
        random_suffixes = generate_all_unique_random_suffixes(
            total_records, num_digits=8
        )
        # กำหนด suffixes ให้กับ DataFrame โดยใช้ index เพื่อให้แน่ใจว่าค่าจะถูกจัดสรรอย่างถูกต้อง
        df_final_merged["IDEN_suffix"] = random_suffixes

        # รวม IDEN_prefix และ IDEN_suffix เข้าด้วยกันเพื่อสร้าง IDEN ที่สมบูรณ์
        df_final_merged["IDEN"] = (
            df_final_merged["IDEN_prefix"] + df_final_merged["IDEN_suffix"]
        )

        # ลบคอลัมน์ชั่วคราว
        df_final_merged = df_final_merged.drop(columns=["IDEN_prefix", "IDEN_suffix"])

        # ตรวจสอบความยาวของ IDEN สุดท้าย
        print("ความยาวของ IDEN แต่ละตัว:", df_final_merged["IDEN"].apply(len).unique())
        # ตรวจสอบว่ามีค่า IDEN ซ้ำกันหรือไม่ (ควรเป็น False เสมอหากไม่มี EA_Code_15 ที่ซ้ำกัน และเลขสุ่ม 8 หลักก็ไม่ซ้ำกันทั่วทั้ง DataFrame)
        print(
            "มีค่า IDEN ซ้ำกันหรือไม่ (หลังการสร้างและก่อน Export):",
            df_final_merged["IDEN"].duplicated().any(),
        )
        if df_final_merged["IDEN"].duplicated().any():
            print(f"{RED}**ยืนยัน: ค่า IDEN ยังคงซ้ำใน DataFrame ก่อน Export!**{RESET}")
            print(f"{YELLOW}ตัวอย่าง IDEN ที่ซ้ำกัน:{RESET}")
            print(
                df_final_merged[df_final_merged["IDEN"].duplicated(keep=False)]
                .sort_values("IDEN")
                .head()
            )

            # เพิ่มการตรวจสอบเพิ่มเติมหาก IDEN ยังซ้ำ (อาจเกิดจากข้อมูลต้นทาง หรือ EA_Code_15 ซ้ำกันอย่างมีนัยสำคัญ)
            duplicated_idens = df_final_merged[
                df_final_merged["IDEN"].duplicated(keep=False)
            ]
            if not duplicated_idens.empty:
                print(
                    f"{RED}**พบปัญหา: IDEN เดียวกันถูกใช้โดย Population_No ที่แตกต่างกัน หรือมี Population_No เป็นค่าว่าง หรือ EA_Code_15 ที่ซ้ำกันทำให้ IDEN ซ้ำ!**{RESET}"
                )
                print(
                    duplicated_idens[
                        ["IDEN", "Population_No", "EA_Code_15"]
                    ].sort_values("IDEN")
                )
        else:
            print(f"{GREEN}ยืนยัน: ไม่พบค่า IDEN ซ้ำกันใน DataFrame ก่อน Export.{RESET}")
        # --- สิ้นสุดส่วนการสร้าง IDEN ที่ปรับตามคำขอ ---

        print("สร้าง IDEN เสร็จแล้ว ถัดไป เติมค่าใน AreaCode VilCode และ VilName")

        print("เติมค่าใน AreaCode")
        ## เติมค่าใน AreaCode
        # สร้าง mapping series โดยใช้ EA_Code_15_m เป็น index
        area_mapping = df_m_control_ea.set_index("EA_Code_15_m")["AreaCode_m"]

        # ตรวจสอบว่าคอลัมน์ AreaCode เป็นประเภท object และมีค่าว่างเป็น ''
        df_final_merged["AreaCode"] = df_final_merged["AreaCode"].replace("", np.nan)

        # ใช้ map เพื่อเติมค่า AreaCode ที่เป็น NaN
        df_final_merged["AreaCode"] = df_final_merged["AreaCode"].fillna(
            df_final_merged["EA_Code_15"].map(area_mapping)
        )

        print("เติมค่าใน VilCode และ VilName")

        # ขั้นตอนที่ 2: เติมค่า AreaCode
        # สร้าง mapping series จาก df_m_control_ea โดยใช้ EA_Code_15_m เป็น index
        area_mapping = df_m_control_ea.set_index("EA_Code_15_m")["AreaCode_m"]

        # แปลงค่าว่าง '' ในคอลัมน์ AreaCode ของ df_final_merged ให้เป็น NaN
        df_final_merged["AreaCode"] = df_final_merged["AreaCode"].replace("", np.nan)

        # เติมค่า AreaCode ที่เป็น NaN ด้วยค่าจาก mapping
        df_final_merged["AreaCode"] = df_final_merged["AreaCode"].fillna(
            df_final_merged["EA_Code_15"].map(area_mapping)
        )

        # ขั้นตอนที่ 3: เติมค่า VilCode_m และ VilName_m โดยมีเงื่อนไขเพิ่มเติม
        # สร้าง mapping dataframe สำหรับ VilCode_m และ VilName_m
        vil_mapping = df_m_control_ea.set_index("EA_Code_15_m")[
            ["VilCode_m", "VilName_m"]
        ]

        # สร้างคอลัมน์ใหม่ใน df_final_merged เพื่อเก็บค่าที่มาจาก df_m_control_ea
        df_final_merged["VilCode_m"] = np.nan
        df_final_merged["VilName_m"] = np.nan

        # แปลงค่าว่าง '' ในคอลัมน์ VilCode ของ df_final_merged ให้เป็น NaN
        df_final_merged["VilCode"] = df_final_merged["VilCode"].replace("", np.nan)

        # สร้างเงื่อนไข: VilCode ใน df_final_merged เป็น NaN หรือความยาวน้อยกว่า 2
        condition = (df_final_merged["VilCode"].isna()) | (
            df_final_merged["VilCode"].str.len() < 2
        )

        # เติมค่า VilCode_m และ VilName_m เฉพาะแถวที่ตรงตามเงื่อนไข
        df_final_merged.loc[condition, "VilCode"] = df_final_merged.loc[
            condition, "EA_Code_15"
        ].map(vil_mapping["VilCode_m"])
        df_final_merged.loc[condition, "VilName"] = df_final_merged.loc[
            condition, "EA_Code_15"
        ].map(vil_mapping["VilName_m"])

        print("เติมค่าใน AreaCode VilCode และ VilName เสร็จแล้ว ถัดไป เพิ่มข้อมูลใน DataSources")

        ## เพิ่มข้อมูลใน DataSources

        # ตรวจสอบว่า DataFrame ไม่ว่างเปล่าก่อน
        # ทำความสะอาดข้อมูล: ลบช่องว่างที่อาจมี
        df_final_merged["Household_CrtdByName"] = df_final_merged[
            "Household_CrtdByName"
        ].str.strip()

        # เพิ่มเงื่อนไขใหม่โดยใช้ .str.contains()
        # df_final_merged['Household_CrtdByName'].str.contains('ประชาชน', na=False)
        # 'na=False' ใช้เพื่อกำหนดให้ค่า NaN เป็น False ในการตรวจสอบเงื่อนไข
        is_peoples = df_final_merged["Household_CrtdByName"].str.contains(
            "ประชาชน", na=False
        )

        # กำหนดค่าในคอลัมน์ DataSources ตามเงื่อนไข
        df_final_merged["DataSources"] = np.where(is_peoples, "2", "1")

        print("เพิ่มข้อมูลใน DataSources เสร็จแล้ว ถัดไป sort data")

        # จัดเรียงข้อมูล
        # df_final_merged = df_final_merged.sort_values(
        #    by=['RegCode', 'ProvCode', 'DistCode', 'SubDistCode', 'AreaCode', 'EA_No', 'VilCode', 'Household_CrtdByName', 'Building_No', 'Household_No', 'HouseholdMemberNumber'])
        df_final_merged = df_final_merged.sort_values(
            by=[
                "RegCode",
                "ProvCode",
                "DistCode",
                "SubDistCode",
                "AreaCode",
                "EA_No",
                "VilCode",
                "Household_CrtdByName",
                "Building_No",
                "Household_No",
                "HouseholdMemberNumber",
            ]
        )

        print("sort data เสร็จแล้ว ถัดไป เลือกคอลัมน์")

        # แปลงข้อมูลเป็น string และตัดส่วนทศนิยมที่ไม่จำเป็น
        df_final_merged['Longitude_POP'] = df_final_merged['Longitude_POP'].astype(str).str.replace(r'\.0+', '', regex=True) 

        # เลือกคอลัมน์ที่ต้องการ Insert/Update
        selected_columns = [
            "IDEN",
            "EA_Code_15",
            "Building_No",
            "BuildingCode",
            "NsoBuilding_No",
            "RegCode",
            "RegName",
            "ProvCode",
            "ProvName",
            "DistCode",
            "DistName",
            "SubDistCode",
            "SubDistName",
            "AreaCode",
            "AreaName",
            "MuniCode",
            "MuniName",
            "SubAdminCode",
            "SubAdminName",
            "MunTaoCode",
            "MunTaoName",
            "CommuCode",
            "CommuName",
            "EA_No",
            "VilCode",
            "VilName",
            "HouseNumber",
            "RoomNumber",
            "RoadName",
            "AlleyWayName",
            "AlleyName",
            "BuildingName",
            "BuildingNumber",
            "BuildingType",
            "BuildingTypeOther",
            "Residing",
            "HouseholdEnumeration",
            "HouseholdEnumerationOther",
            "HouseholdEnumeration_1",
            "HouseholdEnumeration_2",
            "HouseholdEnumeration_3",
            "HouseholdType",
            "NumberOfHousehold",
            "TotalRoom",
            "RoomVacant",
            "RoomResidence",
            "NewBuilding",
            "BuildingStatus",
            "IsBuilding",
            "Building_IsActive",
            "Building_Note",
            "Building_CrtdByCode",
            "Building_CrtdByName",
            "TotalPopulation",
            "TotalMale",
            "TotalFemale",
            "ApproveStatusSupCode",
            "IsMapping",
            "Building_CrtdDateStart",
            "Building_CrtdDateEnd",
            "Building_UpdateDateStart",
            "Building_UpdateDateEnd",
            "Household_No",
            "Building_No_HH",
            "HouseholdNumber",
            "HouseholdEnumeration_HH",
            "ConstructionMaterial",
            "ConstructionMaterialOther",
            "TenureResidence",
            "TenureResidenceOther",
            "TenureLand",
            "TenureLandOther",
            "NumberOfHousueholdMember",
            "HouseholdStatus",
            "Household_IsActive",
            "Household_Note",
            "Household_CrtdByCode",
            "Household_CrtdByName",
            "Household_CrtdDateStart",
            "Household_CrtdDateEnd",
            "Household_UpdateDateStart",
            "Household_UpdateDateEnd",
            "Population_No",
            "Building_No_POP",
            "Household_No_POP",
            "HouseholdMemberNumber",
            "Title",
            "TitleOther",
            "FirstName",
            "LastName",
            "Relationship",
            "Sex",
            "MonthOfBirth",
            "YearOfBirth",
            "Age_01",
            "NationalityNumeric",
            "EducationalAttainment",
            "EmploymentStatus",
            "NameInHouseholdRegister",
            "NameInHouseholdRegisterOther",
            "DurationOfResidence",
            "DurationOfResidence_Text",
            "MigrationCharecteristics",
            "MovedFromProvince",
            "MovedFromAbroad",
            "MigrationReason",
            "MigrationReasonOther",
            "Gender",
            "PopulationStatus",
            "Population_IsActive",
            "Population_Note",
            "Longitude_POP",
            "Population_CrtdByCode",
            "Population_CrtdByName",
            "Population_CrtdDateStart",
            "Population_CrtdDateEnd",
            "Population_UpdateDateStart",
            "Population_UpdateDateEnd",
            "ApproveStatusSup",
            "ApproveDateSup",
            "ApproveStatusProv",
            "ApproveDateProv",
            "m_control_ea_CrtdByCode",
            "ADV_EA",
            "InternetAr",
            "DataSources",
        ]
        df_filtered = df_final_merged[selected_columns].copy()
        print("Shape of df_filtered:", df_filtered.shape)

        # --- เพิ่มโค้ดแก้ไข NaT ตรงนี้ ---
        print("กำลังตรวจสอบและแก้ไขค่าวันที่/เวลา (NaT) ก่อนส่งเข้าฐานข้อมูล...")
        for col in df_filtered.columns:
            if pd.api.types.is_datetime64_any_dtype(df_filtered[col]):
                initial_nat_count = df_filtered[col].isna().sum()
                if initial_nat_count > 0:
                    df_filtered[col] = df_filtered[col].replace(
                        {pd.NaT: None}
                    )  # หรือ datetime.datetime(1900, 1, 1)
                    print(f"  แก้ไข NaT ในคอลัมน์ '{col}': {initial_nat_count} รายการ")
        # --- สิ้นสุดโค้ดแก้ไข NaT ---

        time_end_join = datetime.datetime.now()
        print("เวลาสิ้นสุดในการ Join:", time_end_join)
        # =================================================== เวลาสิ้นสุดในการ Join (time_end_join)

        time_difference_join = time_end_join - time_begin
        print("ความต่างของเวลาในการ Join:", time_difference_join)
        # =================================================== ความต่างของเวลาในการ Join (time_difference_join)

        try:
            placeholders = ", ".join(["?"] * len(selected_columns))
            insert_query_r_alldata = f"INSERT INTO r_alldata ({', '.join(selected_columns)}) VALUES ({placeholders})"
            # insert_query_r_alldata_edit = f"INSERT INTO r_alldata_edit ({', '.join(selected_columns)}) VALUES ({placeholders})"

            total_inserted_count = 0

            # --- ตรวจสอบและ Insert ลง r_alldata ---
            print(
                f"\n{BLUE}กำลังตรวจสอบว่ามี ProvCode = '{ProvCode}' ในฐานข้อมูล r_alldata หรือไม่...{RESET}"
            )
            query_check_r_alldata_existing = (
                f"SELECT COUNT(*) FROM r_alldata WHERE ProvCode = '{ProvCode}'"
            )
            cursor.execute(query_check_r_alldata_existing)
            r_alldata_existing_count = cursor.fetchone()[0]

            if r_alldata_existing_count == 0:
                print(
                    f"{YELLOW}ไม่พบข้อมูลสำหรับ ProvCode = '{ProvCode}' ใน r_alldata จะทำการ INSERT ข้อมูลทั้งหมด{RESET}"
                )
                inserted_r_alldata_count = 0
                for index, row in df_filtered.iterrows():
                    try:
                        cursor.execute(
                            insert_query_r_alldata,
                            [row[col] for col in selected_columns],
                        )
                        inserted_r_alldata_count += 1
                    except pyodbc.Error as ex:
                        sqlstate = ex.args[0]
                        print(
                            f"{RED}เกิดข้อผิดพลาดในการ INSERT ข้อมูลลง r_alldata: {sqlstate} - {ex} - Values: {list(row)}{RESET}"
                        )
                        sql_conn.rollback()
                        raise
                print(
                    f"{PINK}Inserted {inserted_r_alldata_count} new records for ProvCode = '{ProvCode}' into r_alldata.{RESET}"
                )
                total_inserted_count += inserted_r_alldata_count
            else:
                print(
                    f"{YELLOW}พบข้อมูลสำหรับ ProvCode = '{ProvCode}' ใน r_alldata แล้ว จะไม่ดำเนินการ INSERT เพิ่มเติม{RESET}"
                )

            sql_conn.commit()
            print(
                f"{GREEN}การดำเนินการ INSERT ข้อมูลสำหรับ ProvCode = '{ProvCode}' ใน r_alldata และ r_alldata_edit เสร็จสมบูรณ์ (รวม {total_inserted_count} รายการใหม่){RESET}"
            )

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
        print("เวลาสิ้นสุดทั้งหมด:", time_end)
        # ================================================== เวลาสิ้นสุดทั้งหมด (time_end)

        time_difference_insert = time_end - time_end_join
        print("ความต่างของเวลา insert:", time_difference_insert)
        # ================================================== ความต่างของเวลา insert (time_difference_insert)

        time_difference_all = time_end - time_begin
        print("ความต่างของเวลาทั้งหมด:", time_difference_all)
        # ================================================== ความต่างของเวลาทั้งหมด (time_difference_all)

        try:
            
            df_report_update = pd.read_csv(EXCEL_REPORT_PATH)
          
            # แปลง ProvCode เป็น str เพื่อให้เปรียบเทียบได้แน่นอน
            df_report_update['ProvCode'] = df_report_update['ProvCode'].astype(str)

            # ค้นหา index ของแถวที่ต้องการอัปเดต (แถวของจังหวัดปัจจุบัน)
            # เปลี่ยนจาก 'ProvCode' เป็น 'ProvCode' ตามชื่อคอลัมน์ในไฟล์ CSV ของคุณ
            index_to_update = df_report_update[df_report_update['ProvCode'] == ProvCode].index

            if not index_to_update.empty:
                # ใช้ index แรกที่เจอ
                idx = index_to_update[0]

                # อัปเดตข้อมูลทั้งหมดในแถวนั้น
                df_report_update.loc[idx, 'time_begin'] = time_begin.strftime('%Y-%m-%d %H:%M:%S')
                df_report_update.loc[idx, 'time_end_join'] = time_end_join.strftime('%Y-%m-%d %H:%M:%S')
                df_report_update.loc[idx, 'time_difference_join'] = str(time_difference_join)
                df_report_update.loc[idx, 'time_end'] = time_end.strftime('%Y-%m-%d %H:%M:%S')
                df_report_update.loc[idx, 'time_difference_insert'] = str(time_difference_insert)
                df_report_update.loc[idx, 'time_difference_all'] = str(time_difference_all)
                df_report_update.loc[idx, 'Shape of df_building'] = str(df_building.shape)
                df_report_update.loc[idx, 'Shape of df_household'] = str(df_household.shape)
                df_report_update.loc[idx, 'Shape of df_merged_bh'] = str(df_merged_bh.shape)
                df_report_update.loc[idx, 'Shape of df_population'] = str(df_population.shape)
                df_report_update.loc[idx, 'Shape of df_merged_bhp_merged'] = str(df_merged_bhp.shape)
                df_report_update.loc[idx, 'Shape of df_m_control_ea'] = str(df_m_control_ea.shape)
                df_report_update.loc[idx, 'Shape of df_final_merged'] = str(df_final_merged.shape)
                df_report_update.loc[idx, 'data_all_insert'] = str(df_final_merged.shape[0])

                # บันทึก DataFrame ที่อัปเดตแล้วกลับไปทับไฟล์ CSV เดิม
                df_report_update.to_csv(EXCEL_REPORT_PATH, index=False)
                print(f"{GREEN}อัปเดตข้อมูลของ ProvCode {ProvCode} ในไฟล์ CSV เรียบร้อยแล้ว{RESET}")
            else:
                print(f"{YELLOW}ไม่พบแถวสำหรับ ProvCode {ProvCode} เพื่ออัปเดตในไฟล์ CSV{RESET}")

        except FileNotFoundError:
            print(f"{RED}ไม่พบไฟล์ '{EXCEL_REPORT_PATH}'. กรุณาตรวจสอบว่าไฟล์อยู่ในตำแหน่งที่ถูกต้อง{RESET}")
        except Exception as e:
            print(f"{RED}เกิดข้อผิดพลาดในการอัปเดตไฟล์ CSV: {e}{RESET}")

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
