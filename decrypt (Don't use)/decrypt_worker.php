<?php

// --- SCRIPT CONFIGURATION ---

// ตั้งค่าให้สคริปต์ทำงานได้ไม่จำกัดเวลาและใช้หน่วยความจำได้เต็มที่
set_time_limit(0);
ini_set('memory_limit', '-1');

// เปิดการแสดงผลข้อผิดพลาดทั้งหมดเพื่อการดีบัก
error_reporting(E_ALL);
ini_set('display_errors', 1);

// --- DATABASE CONNECTION SETTINGS ---
$serverName = "192.168.0.203"; // IP หรือ Hostname ของ SQL Server
$connectionInfo = array(
    "Database" => "pop6768",      // ชื่อฐานข้อมูล
    "UID" => "pdan",             // ชื่อผู้ใช้
    "PWD" => "P@ssw0rd12#$",     // รหัสผ่าน *กรุณาตรวจสอบความถูกต้อง*
    "CharacterSet" => "UTF-8"    // แนะนำให้ใช้เพื่อรองรับอักษรไทย
);

// --- PROVINCE CODES ---
// รายการรหัสจังหวัดที่ต้องการประมวลผล
$provinceCodes = [
    '31', '33', '35', '36', '37'
    // สามารถเพิ่มรหัสจังหวัดอื่นๆ ได้ที่นี่
];

// --- BATCH PROCESSING CONFIGURATION ---
define('BATCH_SIZE', 400); // จำนวนรายการที่จะอัปเดตในหนึ่งชุด (ปรับค่าได้ตามความเหมาะสม)

// --- LOGGING CONFIGURATION ---
$logFileName = 'update_log.csv'; // ชื่อไฟล์สำหรับบันทึกผล

/**
 * ฟังก์ชันสำหรับเข้ารหัสและถอดรหัสข้อความด้วย AES-256-CBC
 * @param string $action 'encrypt' หรือ 'decrypt'
 * @param string|null $string ข้อความที่ต้องการประมวลผล
 * @return string|false ผลลัพธ์ หรือ false หากล้มเหลว
 */
function encrypt_decrypt($action, $string)
{
    if ($string === null || $string === '') {
        return "";
    }

    $output = false;
    $encrypt_method = "AES-256-CBC";
    $secret_key = 'Pp9xeukV5j3pp89w';
    $secret_iv = 'T5eUeG3MsJkhr6sc';

    $key = hash('sha256', $secret_key);
    $iv = substr(hash('sha256', $secret_iv), 0, 16);

    if ($action == 'encrypt') {
        $output = openssl_encrypt($string, $encrypt_method, $key, 0, $iv);
        $output = base64_encode($output);
    } else if ($action == 'decrypt') {
        $decoded_string = @base64_decode($string, true);
        if ($decoded_string === false) {
            return false; // ไม่ใช่ base64 string ที่ถูกต้อง
        }
        $output = openssl_decrypt($decoded_string, $encrypt_method, $key, 0, $iv);
    }
    return $output;
}

/**
 * ประมวลผลข้อมูลเป็นชุดด้วยคำสั่ง UPDATE...CASE เพียงคำสั่งเดียว
 * @param resource $conn การเชื่อมต่อฐานข้อมูล
 * @param array $batchData ข้อมูลที่ต้องการอัปเดต
 * @return int จำนวนแถวที่อัปเดตสำเร็จ
 */
function process_batch($conn, &$batchData)
{
    if (empty($batchData)) {
        return 0;
    }

    $params = [];
    $idenList = [];
    $firstNameCase = "";
    $lastNameCase = "";

    foreach ($batchData as $record) {
        $idenList[] = $record['iden'];
        // สร้างส่วนของ CASE statement และเตรียม parameters
        $firstNameCase .= "WHEN ? THEN ? ";
        $lastNameCase  .= "WHEN ? THEN ? ";
        array_push($params, $record['iden'], $record['firstName'], $record['iden'], $record['lastName']);
    }

    // สร้าง SQL UPDATE แบบไดนามิก
    $inClause = implode(',', array_fill(0, count($idenList), '?'));
    $sql = "UPDATE r_alldata SET
                FirstName_D = CASE IDEN {$firstNameCase} END,
                LastName_D = CASE IDEN {$lastNameCase} END
            WHERE IDEN IN ({$inClause})";

    // เพิ่ม IDENs สำหรับ IN clause เข้าไปใน parameters
    $params = array_merge($params, $idenList);

    $stmt = sqlsrv_query($conn, $sql, $params);

    if ($stmt === false) {
        echo "Error executing batch update: \n";
        print_r(sqlsrv_errors(), true);
        return 0;
    }

    $rowsAffected = sqlsrv_rows_affected($stmt);
    sqlsrv_free_stmt($stmt);
    $batchData = []; // เคลียร์ข้อมูลในชุดเพื่อรอรับชุดต่อไป

    return $rowsAffected > 0 ? $rowsAffected : 0;
}


// --- MAIN SCRIPT EXECUTION ---

// 1. เริ่มต้นสร้างไฟล์ Log
$logFile = fopen($logFileName, 'w');
// เขียนหัวข้อของไฟล์ CSV
fputcsv($logFile, ['Province Code', 'Total Processed', 'Successfully Updated', 'Failed/Skipped', 'Timestamp']);


// 2. เชื่อมต่อฐานข้อมูล
echo "Connecting to SQL Server...\n";
$conn = sqlsrv_connect($serverName, $connectionInfo);

if ($conn) {
    echo "Connection established successfully.\n\n";
} else {
    echo "Connection could not be established.\n";
    $error_message = sqlsrv_errors();
    // เขียนข้อผิดพลาดลงไฟล์ log
    fputcsv($logFile, ['CONNECTION_ERROR', 0, 0, 0, date('Y-m-d H:i:s')]);
    die(print_r($error_message, true));
}

// 3. วนลูปประมวลผลทีละจังหวัด
$totalProvinces = count($provinceCodes);
$provinceCounter = 0;

foreach ($provinceCodes as $provCode) {
    $provinceCounter++;
    echo "============================================================\n";
    echo "Processing Province Code: {$provCode} ({$provinceCounter} of {$totalProvinces})\n";
    echo "============================================================\n";

    // ดึงข้อมูลของจังหวัดปัจจุบัน
    $sqlSelect = "SELECT IDEN, FirstName, LastName FROM r_alldata WHERE ProvCode = ?";
    $paramsSelect = array($provCode);
    $options = array("Scrollable" => SQLSRV_CURSOR_KEYSET);

    $stmtSelect = sqlsrv_query($conn, $sqlSelect, $paramsSelect, $options);

    if ($stmtSelect === false) {
        echo "Error executing SELECT query for province {$provCode}: \n";
        print_r(sqlsrv_errors(), true);
        continue; // ข้ามไปจังหวัดถัดไป
    }

    $rowCount = sqlsrv_num_rows($stmtSelect);
    if ($rowCount == 0) {
        echo "No records found for province {$provCode}. Skipping.\n\n";
        fputcsv($logFile, [$provCode, 0, 0, 0, date('Y-m-d H:i:s')]); // บันทึกลง log ว่าไม่พบข้อมูล
        continue;
    }

    echo "Found {$rowCount} records. Starting decryption and batch update process...\n";

    $totalUpdatedCount = 0;
    $failedCount = 0;
    $processedCount = 0;
    $batchData = [];

    // วนลูปดึงข้อมูลทีละแถวเพื่อเตรียมจัดกลุ่ม
    while ($row = sqlsrv_fetch_array($stmtSelect, SQLSRV_FETCH_ASSOC)) {
        $processedCount++;
        
        // ถอดรหัสข้อมูล
        $decryptedFirstName = encrypt_decrypt('decrypt', $row['FirstName']);
        $decryptedLastName = encrypt_decrypt('decrypt', $row['LastName']);

        if (!empty($decryptedFirstName) && !empty($decryptedLastName)) {
            // หากถอดรหัสสำเร็จ เพิ่มข้อมูลลงในชุดรออัปเดต
            $batchData[] = [
                'iden' => $row['IDEN'],
                'firstName' => $decryptedFirstName,
                'lastName' => $decryptedLastName,
            ];
        } else {
            $failedCount++;
        }

        // หากข้อมูลในชุดเต็มตามขนาดที่กำหนด หรือเป็นรายการสุดท้าย ให้ทำการอัปเดต
        if (count($batchData) >= BATCH_SIZE || $processedCount == $rowCount) {
            echo "Processing batch of " . count($batchData) . " records... ({$processedCount} / {$rowCount})\n";
            $updatedInBatch = process_batch($conn, $batchData);
            $totalUpdatedCount += $updatedInBatch;
        }
    }

    // สรุปผลของจังหวัดปัจจุบัน
    echo "\n--- Province {$provCode} Summary ---\n";
    echo "Total records processed: {$processedCount}\n";
    echo "Successfully updated:    {$totalUpdatedCount}\n";
    echo "Failed/Skipped records:  {$failedCount}\n";
    echo "-----------------------------------\n\n";
    
    // บันทึกสรุปผลลงไฟล์ CSV
    fputcsv($logFile, [$provCode, $processedCount, $totalUpdatedCount, $failedCount, date('Y-m-d H:i:s')]);
    sqlsrv_free_stmt($stmtSelect);
}

echo "============================================================\n";
echo "All provinces have been processed. Script finished.\n";
echo "Log file '{$logFileName}' has been created with the summary.\n";
echo "============================================================\n";

// 4. ปิดไฟล์ Log และการเชื่อมต่อฐานข้อมูล
fclose($logFile);
sqlsrv_close($conn);

?>

