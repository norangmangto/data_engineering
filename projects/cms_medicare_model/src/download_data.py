import os
import requests
import zipfile
import io

# URLs for DE-SynPUF Sample 1 Files (approximate direct links based on data.cms.gov structure)
# Note: Since CMS often uses dynamic links or redirectors, these are the common structure.
# If these fail, the user can manually download from:
# https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files/cms-2008-2010-data-entrepreneurs-synthetic-public-use-file-de-synpuf/de10-sample-1

FILES = {
    "DE1_0_2008_Beneficiary_Summary_File_Sample_1.zip": "https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_1.zip",
    "DE1_0_2009_Beneficiary_Summary_File_Sample_1.zip": "https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_1.zip",
    "DE1_0_2010_Beneficiary_Summary_File_Sample_1.zip": "https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_1.zip",
    "DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.zip": "https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.zip",
    "DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.zip": "https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.zip",
    "DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.zip": "https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.zip",
    "DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.zip": "https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.zip",
    "DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1.zip": "https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1.zip"
}

RAW_DIR = "data/raw"

def download_and_extract(filename, url):
    print(f"Downloading {filename}...")
    response = requests.get(url)
    if response.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(RAW_DIR)
            print(f"Extracted {filename}")
    else:
        print(f"Failed to download {filename} (Status {response.status_code})")
        print(f"Please download manually from: https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files/cms-2008-2010-data-entrepreneurs-synthetic-public-use-file-de-synpuf/de10-sample-1")

def main():
    os.makedirs(RAW_DIR, exist_ok=True)
    for filename, url in FILES.items():
        download_and_extract(filename, url)

if __name__ == "__main__":
    main()
