# Getting VISTA Data
Write-Host "Getting VISTA Data"
if  (Test-Path -Path VISTA.zip)
{
    Write-Host "Vista data already downloaded."
}
else {
    Write-Host "Downloading Vista data"
    Invoke-WebRequest -o VISTA.zip 'https://vrpsaopendatastdlrs01.blob.core.windows.net/opendata/VISTA_data/VISTA%20online%20data%20CSV%202012-18.zip' 
}
if (Test-Path -Path './external_data') {
}
else {
    New-Item -type Directory -Path ./external_data
}


Expand-Archive -Force VISTA.zip -DestinationPath external_data

python -c @"
import pandas as pd
import os
wd = os.getcwd()
read_file = pd.read_excel(os.path.join(wd,'external_data','P_VISTA1218_V2.xlsx'))
read_file.to_csv(os.path.join(wd,'external_data','P_VISTA1218_V2.csv'), index = None, header = True)
"@

Remove-Item .\external_data\P_VISTA1218_V2.xlsx

Write-Host "Getting Vehicle Registration Snapshot Data"

if  (Test-Path -Path external_data/Whole_Fleet_Vehicle_Registration_Snapshot_by_Model_Q2_2023.csv)
{
    Write-Host "Rego data already downloaded."
}
else {
    Write-Host "Downloading Vehicle Registration Snapshot."
    Invoke-WebRequest -o external_data/Whole_Fleet_Vehicle_Registration_Snapshot_by_Model_Q2_2023.csv "https://vicroadsopendatastorehouse.vicroads.vic.gov.au/opendata/Registration_Licencing/2023/Whole_Fleet_Vehicle_Registration_Snapshot_by_Postcode_Q2_2023.csv" 
}
