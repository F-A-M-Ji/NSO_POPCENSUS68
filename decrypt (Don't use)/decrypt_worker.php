<?php

// --- SCRIPT CONFIGURATION ---

set_time_limit(0);
ini_set('memory_limit', '-1');
error_reporting(E_ALL);
ini_set('display_errors', 1);

// --- DATABASE CONNECTION SETTINGS ---
$serverName = "192.168.0.203"; // IP หรือ Hostname ของ SQL Server
$connectionInfo = array(
    "Database" => "pop6768",
    "UID" => "pdan",
    "PWD" => "P@ssw0rd12#$",
    "CharacterSet" => "UTF-8"
);

// --- PROVINCE CODES ---
$provinceCodes = [
    '31', '33', '35', '36', '37', '39', '40', '41', '42', '43' ,'45', '46', '47', '48'
];

// --- BATCH PROCESSING CONFIGURATION ---
define('BATCH_SIZE', 400); // สามารถปรับขนาด Batch ได้ตามความเหมาะสม

// --- LOGGING CONFIGURATION ---
$logFileName = 'update_log_2_pass.csv';

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
    if ($action == 'decrypt') {
        $decoded_string = @base64_decode($string, true);
        if ($decoded_string === false) {
            return false;
        }
        $output = openssl_decrypt($decoded_string, $encrypt_method, $key, 0, $iv);
    }
    return $output;
}

/**
 * ฟังก์ชันประมวลผล Batch สำหรับฟิลด์เดียว (Reusable)
 * @param resource $conn - Connection
 * @param array $batchData - ข้อมูล
 * @param string $columnToUpdate - ชื่อคอลัมน์ที่จะอัปเดต (เช่น 'FirstName_D')
 * @param string $dataKey - Key ของข้อมูลใน array (เช่น 'value')
 * @return int - จำนวนแถวที่อัปเดต
 */
function process_batch_single_field($conn, &$batchData, $columnToUpdate, $dataKey)
{
    if (empty($batchData)) {
        return 0;
    }
    $params = [];
    $idenList = [];
    $caseStatement = "";

    foreach ($batchData as $record) {
        $idenList[] = $record['iden'];
        $caseStatement .= "WHEN ? THEN ? ";
        array_push($params, $record['iden'], $record[$dataKey]);
    }

    $inClause = implode(',', array_fill(0, count($idenList), '?'));
    // สร้าง SQL แบบไดนามิกสำหรับฟิลด์เดียว
    $sql = "UPDATE r_online_survey SET {$columnToUpdate} = CASE IDEN {$caseStatement} END WHERE IDEN IN ({$inClause})";

    $params = array_merge($params, $idenList);
    $stmt = sqlsrv_query($conn, $sql, $params);

    if ($stmt === false) {
        echo "Error executing batch update for column {$columnToUpdate}: \n";
        print_r(sqlsrv_errors(), true);
        return 0;
    }

    $rowsAffected = sqlsrv_rows_affected($stmt);
    sqlsrv_free_stmt($stmt);
    $batchData = [];

    return $rowsAffected > 0 ? $rowsAffected : 0;
}

// --- MAIN SCRIPT EXECUTION ---

$logFile = fopen($logFileName, 'w');
fputcsv($logFile, ['Pass', 'Province Code', 'Total Records Updated', 'Timestamp']);

echo "Connecting to SQL Server...\n";
$conn = sqlsrv_connect($serverName, $connectionInfo);

if (!$conn) {
    echo "Connection could not be established.\n";
    fputcsv($logFile, ['Connection Error', 'N/A', 0, date('Y-m-d H:i:s')]);
    die(print_r(sqlsrv_errors(), true));
}
echo "Connection established successfully.\n\n";

// ===================================================================================
//                                  PASS 1: FirstName
// ===================================================================================
echo "############################################################\n";
echo "### STARTING PASS 1 : Processing FirstName_D             ###\n";
echo "############################################################\n\n";

foreach ($provinceCodes as $provCode) {
    echo "--- [PASS 1] Processing FirstName for Province: {$provCode} ---\n";

    $sqlSelect = "SELECT IDEN, FirstName FROM r_online_survey WHERE ProvCode = ?";
    $stmtSelect = sqlsrv_query($conn, $sqlSelect, [$provCode]);

    if ($stmtSelect === false) {
        echo "Error selecting data for province {$provCode}.\n";
        continue;
    }

    $totalUpdatedCount = 0;
    $processedCount = 0;
    $batchData = [];

    while ($row = sqlsrv_fetch_array($stmtSelect, SQLSRV_FETCH_ASSOC)) {
        $processedCount++;
        $decryptedValue = encrypt_decrypt('decrypt', $row['FirstName']);
        if ($decryptedValue === false) {
            $decryptedValue = '';
        }
        $batchData[] = ['iden' => $row['IDEN'], 'value' => $decryptedValue];

        if (count($batchData) >= BATCH_SIZE) {
            $updatedInBatch = process_batch_single_field($conn, $batchData, 'FirstName_D', 'value');
            $totalUpdatedCount += $updatedInBatch;
            echo "Processed {$provCode} {$processedCount} records...\n";
        }
    }
    if (!empty($batchData)) {
        $updatedInBatch = process_batch_single_field($conn, $batchData, 'FirstName_D', 'value');
        $totalUpdatedCount += $updatedInBatch;
    }

    echo "Finished Province {$provCode}. Total updated rows: {$totalUpdatedCount}\n\n";
    fputcsv($logFile, ['PASS 1', $provCode, $totalUpdatedCount, date('Y-m-d H:i:s')]);
    sqlsrv_free_stmt($stmtSelect);
}

// ===================================================================================
//                                  PASS 2: LastName
// ===================================================================================
echo "\n\n############################################################\n";
echo "### STARTING PASS 2 : Processing LastName_D              ###\n";
echo "############################################################\n\n";

foreach ($provinceCodes as $provCode) {
    echo "--- [PASS 2] Processing LastName for Province: {$provCode} ---\n";

    $sqlSelect = "SELECT IDEN, LastName FROM r_online_survey WHERE ProvCode = ?";
    $stmtSelect = sqlsrv_query($conn, $sqlSelect, [$provCode]);

    if ($stmtSelect === false) {
        echo "Error selecting data for province {$provCode}.\n";
        continue;
    }

    $totalUpdatedCount = 0;
    $processedCount = 0;
    $batchData = [];

    while ($row = sqlsrv_fetch_array($stmtSelect, SQLSRV_FETCH_ASSOC)) {
        $processedCount++;
        $decryptedValue = encrypt_decrypt('decrypt', $row['LastName']);
        if ($decryptedValue === false) {
            $decryptedValue = '';
        }
        $batchData[] = ['iden' => $row['IDEN'], 'value' => $decryptedValue];

        if (count($batchData) >= BATCH_SIZE) {
            $updatedInBatch = process_batch_single_field($conn, $batchData, 'LastName_D', 'value');
            $totalUpdatedCount += $updatedInBatch;
            echo "Processed {$provCode} {$processedCount} records...\n";
        }
    }
    if (!empty($batchData)) {
        $updatedInBatch = process_batch_single_field($conn, $batchData, 'LastName_D', 'value');
        $totalUpdatedCount += $updatedInBatch;
    }

    echo "Finished Province {$provCode}. Total updated rows: {$totalUpdatedCount}\n\n";
    fputcsv($logFile, ['PASS 2', $provCode, $totalUpdatedCount, date('Y-m-d H:i:s')]);
    sqlsrv_free_stmt($stmtSelect);
}

echo "============================================================\n";
echo "All provinces and passes have been processed. Script finished.\n";
echo "============================================================\n";

fclose($logFile);
sqlsrv_close($conn);

?>