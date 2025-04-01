import os
import httpx
from datetime import datetime, date
from oaipmh_scythe import Scythe
import certifi
from fpdf import FPDF

# Configure SSL verification
USE_SSL_VERIFICATION = True  # Set to False if you want to bypass SSL verification (not recommended)

# Ensure the system uses certifi's certificates
if USE_SSL_VERIFICATION:
    os.environ["SSL_CERT_FILE"] = certifi.where()
    os.environ["SSL_CERT_DIR"] = os.path.dirname(certifi.where())


def main():
    config = readConfigFile()
    base_url = config["base_url"]
    metadataFormat = config["metadata_format"]
    last_run_date, today = readStateFile()

    # Create metadata folder if it doesn't exist
    if not os.path.exists("metadata"):
        os.makedirs("metadata")

    try:
        runScythe(base_url, metadataFormat, last_run_date, today)
    except Exception as e:
        print(f"Error running Scythe: {e}")
    updateStateFile(today)


def readConfigFile():
    config = {}
    with open("config.txt", "r") as configFile:
        for line in configFile:
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, value = line.split("=", 1)
            config[key] = value
    return config


def readStateFile():
    try:
        with open("state.txt", "r") as stateFile:
            last_run_str = stateFile.read().strip()
            last_run_date = datetime.strptime(last_run_str, "%Y-%m-%d").date()
    except (FileNotFoundError, ValueError):
        print("State file not found or invalid format. Defaulting to yesterday.")
        last_run_date = date.today()
    today = date.today()
    return last_run_date, today


def updateStateFile(new_date):
    with open("state.txt", "w") as stateFile:
        stateFile.write(new_date.strftime("%Y-%m-%d"))
    print(f"Updated state file with {new_date}")


def saveMetadataToPDF(record, index):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    pdf.multi_cell(0, 10, str(record))
    pdf_file = f"metadata/record_{index}.pdf"
    pdf.output(pdf_file)
    print(f"Saved metadata to {pdf_file}")


def downloadPDF(pdf_url, index):
    try:
        response = httpx.get(pdf_url)
        response.raise_for_status()
        pdf_file = f"metadata/record_{index}.pdf"
        with open(pdf_file, "wb") as file:
            file.write(response.content)
        print(f"Downloaded PDF to {pdf_file}")
    except Exception as e:
        print(f"Error downloading PDF from {pdf_url}: {e}")


def extractPDFUrl(record):
    try:
        for line in str(record).split("<dc:identifier>"):
            if line.startswith("http") and line.endswith(".pdf</dc:identifier>"):
                return line.split("</dc:identifier>")[0]
        return None
    except Exception as e:
        print(f"Error extracting PDF URL: {e}")
        return None


def runScythe(endpoint, metadataFormat, last_run_date, today):
    print(f"Querying endpoint: {endpoint} with format: {metadataFormat} from {last_run_date} to {today}")
    try:
        with Scythe(endpoint) as scythe:
            records = scythe.list_records(
                metadata_prefix=metadataFormat,
                from_=last_run_date.strftime("%Y-%m-%d"),
                until=today.strftime("%Y-%m-%d")
            )
            tempNumRecords = 0
            for index, record in enumerate(records):
                print(record)
                saveMetadataToPDF(record, index)
                pdf_url = extractPDFUrl(record)
                if pdf_url:
                    downloadPDF(pdf_url, index)
                tempNumRecords += 1
                if tempNumRecords == 100:
                    break  # Stop after 100 records for testing
    except Exception as e:
        print(f"No records found or error occurred: {e}")


if __name__ == "__main__":
    main()
