import xml.etree.ElementTree as ET
import datetime
import calendar
import csv

MAXMONTH = 12

def zero_pad(num):
    if int(num) < 10 and int(num) >= 0:
        return "0" + str(num)
    else:
        return str(num)

def extract_dates(root):
    dates = []
    
    # Дата принятия статьи
    try:
        accepted_date_elem = root.find(".//date[@date-type='accepted']")
        if accepted_date_elem is not None:
            accepted_date = "/".join([accepted_date_elem.find("year").text, accepted_date_elem.find("month").text, accepted_date_elem.find("day").text])
            dates.append(("Accepted", accepted_date))
    except Exception as e:
        print("Error extracting accepted date:", e)

    if not dates:
        return []

    return dates

def process_xml_files(infile, output_file):
    with open(output_file, 'w', newline='') as csvfile, open(infile) as f_in:
        fieldnames = ['File Name', 'Date']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for line in f_in:
            pmc_path = line.strip()
            xml_file = pmc_path
            try:
                root = ET.parse(xml_file).getroot()
                dates = extract_dates(root)
                for date_type, date_value in dates:
                    writer.writerow({'File Name': xml_file, 'Date': date_value})
            except Exception as e:
                print("Error processing", xml_file, ":", e)

if __name__ == "__main__":
    infile = "/home/nicolaedrabcinski/research/lab/new_reuse/data/pmc_paths.txt"
    output_file = "/home/nicolaedrabcinski/research/lab/new_reuse/data/pre_filter_dates.csv"
    process_xml_files(infile, output_file)