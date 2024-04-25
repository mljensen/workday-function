#Imports
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import csv
from io import StringIO


# API connection
API_USERNAME = "etommerup@xellia2.com"
API_PASSWORD = "vxg_hmw!ckr_UPR9wae"


#Defining variables
url = 'https://api.adaptiveinsights.com/api/v38'

header_global = {'Content-Type': 'application/xml'}

#Request to get the different versions for the budget.
def request_body_version():
        body = (f"<?xml version='1.0' encoding='UTF-8'?><call method='exportVersions' callerName='Basico'><credentials login='{API_USERNAME}' password='{API_PASSWORD}'/></call>")
        return body
    
    
# Request to get exportData
def request_body_data(version):
    body = (f"<?xml version='1.0' encoding='UTF-8'?><call method='exportData' callerName='Basico'><credentials login='{API_USERNAME}' password='{API_PASSWORD}'/><version name='{version}'/><format useInternalCodes='true' includeUnmappedItems='false' /><filters><timeSpan start='01/2024' end='12/2024'/></filters></call>")
    return body


def budget(data):
    version_response = requests.post(url, data=request_body_version(), headers=header_global)
    root = ET.fromstring(version_response.content)

    # List to hold the names of top-level parent versions
    data = []
    parent_map = {c: p for p in root.iter() for c in p}

    for version in root.iter('version'):
            name = version.get('name')
            parent = parent_map[version].get('name')
        #     if parent == 'Snowflake Dataload':
        #             data.append(name)
            if name == 'Rolling Forecast':
                    print(name)
                    data.append(name)

        #     if parent == 'Rolling Forecast' or 'Snowflake Dataload':
        #         print(parent)
        #         data.append(name)

    return data

def get_budget_data(version, data):
        response = requests.post(url=url, data=request_body_data(version=version), headers=header_global)
        #CSV file after CDATA[, which is why we have start and end index
        try:
                start_index = response.text.index("<![CDATA[") + len("<![CDATA[")
                end_index = response.text.index("]]>")

                csv_data = response.text[start_index:end_index]
                csv_rows = list(csv.reader(StringIO(csv_data)))
        except ValueError as e:
                print(e)
                return
        #Splitting the headers from the CSV
        header_line = [header.strip() for header in csv_data.split("\n")[0].split(",")]
        #Year/Month is columns, but in converted into rows here
        dates = header_line[3:]
        year_month = []
        for val in dates:
                year_month.append(val)
        for row in csv_rows[1:]:
            #Counter for getting the right element in the CSV row.
            i = 0
            for ym in year_month:
                    y = ym.split("/")[1]
                    m = ym.split("/")[0]

                    if row[3 + int(i)] != '0.0':
                                data.append([row[0], row[1], row[2], version, y, m, row[3 + int(i)], 'Standard sheet'])
                                i += 1
        return data
    

#Creating Data table
data = []
budget_versions = budget(data)

print(budget_versions)



# For Standard data  
# def get_standard_sheets():
#         for version in budget_versions:
#             print(version)
#             get_budget_data(version, data)
#             data3 = pd.DataFrame(data, columns=['Account_Name', 'Account_Code', 'Level_Code', 'Version', 'Year', 'Month', 'Amount', 'Sheet_type'])
#             file_name = 'standard_sheets.csv'
#             print("We made it!")
            
#             data3.to_csv(file_name, mode='a', index=False)


def get_standard_sheets():
        for version in budget_versions:
                print(version)
                get_budget_data(version, data)
                data3 = pd.DataFrame(data, columns=['Account_Name', 'Account_Code', 'Level_Code', 'Version', 'Year', 'Month', 'Amount', 'Sheet_type'])
                file_name = 'standard_sheets.csv'
                print("We made it!")


                data3.to_csv(file_name, mode='a', index=False)



