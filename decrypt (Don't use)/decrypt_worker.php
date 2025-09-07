<?php

// --- SCRIPT CONFIGURATION ---

// Set script to run indefinitely and use maximum available memory
set_time_limit(0);
ini_set('memory_limit', '-1');

// Enable full error reporting for debugging
error_reporting(E_ALL);
ini_set('display_errors', 1);

// --- DATABASE CONNECTION SETTINGS ---
$serverName = "192.168.0.203"; // Your SQL Server IP or hostname
$connectionInfo = array(
    "Database" => "pop6768",   // Your database name
    "UID" => "pdan",           // Your username
    "PWD" => "P@ssw0rd12#$",    // Your password - *แก้ไขรหัสผ่านให้ถูกต้อง*
    "CharacterSet" => "UTF-8"  // Recommended for handling Thai characters
);

// --- PROVINCE CODES ---
// An array containing all province codes to be processed.
$provinceCodes = [
    '39', '41'
];


/**
 * Encrypts or decrypts a string using AES-256-CBC.
 *
 * @param string $action The action to perform: 'encrypt' or 'decrypt'.
 * @param string|null $string The string to process.
 * @return string|false The processed string, an empty string if input is null, or false on failure.
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

    // Create the key and initialization vector
    $key = hash('sha256', $secret_key);
    $iv = substr(hash('sha256', $secret_iv), 0, 16);

    if ($action == 'encrypt') {
        $output = openssl_encrypt($string, $encrypt_method, $key, 0, $iv);
        $output = base64_encode($output);
    } else if ($action == 'decrypt') {
        // Suppress warnings from openssl_decrypt for invalid base64 strings
        $decoded_string = @base64_decode($string, true);
        if ($decoded_string === false) {
            return false; // Not a valid base64 string
        }
        $output = openssl_decrypt($decoded_string, $encrypt_method, $key, 0, $iv);
    }
    return $output;
}

// --- MAIN SCRIPT EXECUTION ---

// 1. Establish database connection
echo "Connecting to SQL Server...\n";
$conn = sqlsrv_connect($serverName, $connectionInfo);

if ($conn) {
    echo "Connection established successfully.\n\n";
} else {
    echo "Connection could not be established.\n";
    die(print_r(sqlsrv_errors(), true));
}

// 2. Loop through each province code
$totalProvinces = count($provinceCodes);
$provinceCounter = 0;

foreach ($provinceCodes as $provCode) {
    $provinceCounter++;
    echo "============================================================\n";
    echo "Processing Province Code: {$provCode} ({$provinceCounter} of {$totalProvinces})\n";
    echo "============================================================\n";

    // 3. Select records for the current province
    // We select the primary key (IDEN) to perform the UPDATE later.
    $sqlSelect = "SELECT IDEN, FirstName, LastName FROM r_alldata WHERE ProvCode = ?";
    $paramsSelect = array($provCode);
    $options = array("Scrollable" => SQLSRV_CURSOR_KEYSET); // To get row count

    $stmtSelect = sqlsrv_query($conn, $sqlSelect, $paramsSelect, $options);

    if ($stmtSelect === false) {
        echo "Error executing SELECT query for province {$provCode}: \n";
        die(print_r(sqlsrv_errors(), true));
    }

    $rowCount = sqlsrv_num_rows($stmtSelect);
    if ($rowCount == 0) {
        echo "No records found for province {$provCode}. Skipping.\n\n";
        continue;
    }

    echo "Found {$rowCount} records. Starting decryption and update process...\n";

    $updatedCount = 0;
    $failedCount = 0;
    $processedCount = 0;
    
    // 4. Fetch each row and process it
    while ($row = sqlsrv_fetch_array($stmtSelect, SQLSRV_FETCH_ASSOC)) {
        $processedCount++;
        $iden = $row['IDEN'];
        $encryptedFirstName = $row['FirstName'];
        $encryptedLastName = $row['LastName'];
        
        // Decrypt data
        $decryptedFirstName = encrypt_decrypt('decrypt', $encryptedFirstName);
        $decryptedLastName = encrypt_decrypt('decrypt', $encryptedLastName);

        // 5. CRITICAL CHECK: Update only if both fields are successfully decrypted
        // The !empty() check handles false, null, and empty strings ('')
        if (!empty($decryptedFirstName) && !empty($decryptedLastName)) {
            // Both are valid, proceed with update
            $sqlUpdate = "UPDATE r_alldata SET FirstName_D = ?, LastName_D = ? WHERE IDEN = ?";
            $paramsUpdate = array($decryptedFirstName, $decryptedLastName, $iden);

            $stmtUpdate = sqlsrv_query($conn, $sqlUpdate, $paramsUpdate);
            if ($stmtUpdate === false) {
                echo "Error updating record IDEN {$iden}: \n";
                print_r(sqlsrv_errors(), true);
                // Continue to the next record
            } else {
                $updatedCount++;
                sqlsrv_free_stmt($stmtUpdate);
            }
        } else {
            // Decryption failed for one or both fields, skip this record.
            $failedCount++;
        }
        
        // Progress indicator
        if ($processedCount % 1000 == 0) {
            echo "Processed {$processedCount} / {$rowCount} records...\n";
        }
    }
    
    echo "\n--- Province {$provCode} Summary ---\n";
    echo "Total records processed: {$processedCount}\n";
    echo "Successfully updated:    {$updatedCount}\n";
    echo "Failed/Skipped records:  {$failedCount}\n";
    echo "-----------------------------------\n\n";

    // Free statement resource
    sqlsrv_free_stmt($stmtSelect);
}

echo "============================================================\n";
echo "All provinces have been processed. Script finished.\n";
echo "============================================================\n";

// 6. Close the connection
sqlsrv_close($conn);

?>
