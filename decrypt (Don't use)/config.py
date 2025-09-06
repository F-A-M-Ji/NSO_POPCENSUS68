# config.py

import base64
import hashlib
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

# --- ตั้งค่าการเชื่อมต่อฐานข้อมูล SQL Server ---
DB_CONFIG = {
    'driver': '{SQL Server}',
    'server': '192.168.0.203',
    'database': 'pop6768',
    'uid': 'pdan',
    'pwd': 'P@ssw0rd12#$'
}

# --- ค่า Secret สำหรับการถอดรหัส (ต้องเหมือนกับใน PHP ทุกประการ) ---
SECRET_KEY = 'Pp9xeukV5j3pp89w'
SECRET_IV = 'T5eUeG3MsJkhr6sc'

def decrypt_string(encrypted_string: str) -> str:
    """
    ฟังก์ชันสำหรับถอดรหัสข้อความที่ถูกเข้ารหัสด้วยตรรกะเดียวกับ PHP
    :param encrypted_string: ข้อความที่เข้ารหัส (Base64 encoded)
    :return: ข้อความที่ถอดรหัสแล้ว หรือค่าว่างหากเกิดข้อผิดพลาด
    """
    if not encrypted_string:
        return ""

    try:
        # สร้าง Key จริง: นำ Secret Key มา hash ด้วย sha256
        key = hashlib.sha256(SECRET_KEY.encode('utf-8')).digest()

        # สร้าง IV จริง: นำ Secret IV มา hash ด้วย sha256 แล้วตัดเอา 16 bytes แรก
        iv = hashlib.sha256(SECRET_IV.encode('utf-8')).digest()[:16]

        # ถอดรหัส Base64 ก่อน
        encrypted_bytes = base64.b64decode(encrypted_string)

        # สร้างตัวถอดรหัส AES-256-CBC
        cipher = AES.new(key, AES.MODE_CBC, iv)

        # ทำการถอดรหัส และนำ Padding ที่เกินออก (unpad)
        decrypted_bytes = unpad(cipher.decrypt(encrypted_bytes), AES.block_size)
        
        # แปลงผลลัพธ์จาก bytes กลับเป็น string แล้วส่งค่ากลับ
        return decrypted_bytes.decode('utf-8')
        
    except (ValueError, KeyError, TypeError) as e:
        # หากมีข้อผิดพลาดในการถอดรหัส ให้ return ค่าว่าง
        print(f"เกิดข้อผิดพลาดในการถอดรหัสข้อมูล: '{encrypted_string}' - Error: {e}")
        return ""