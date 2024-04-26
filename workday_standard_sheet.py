#Imports
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import csv
from io import StringIO

<<<<<<< HEAD
# Blob storage connection
connect_str = ""
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_name = "azure-webjobs-hosts"
=======
>>>>>>> parent of 87dd92f (Added func for get all)

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
def request_body_data(version, timespan_start, timespan_end):
    body = (f"<?xml version='1.0' encoding='UTF-8'?><call method='exportData' callerName='Basico'><credentials login='{API_USERNAME}' password='{API_PASSWORD}'/><version name='{version}'/><format useInternalCodes='true' includeUnmappedItems='false' /><filters><timeSpan start='{timespan_start}' end='{timespan_end}'/></filters></call>")
    return body


def get_budget_data(version, request_body):
        data = []
        response = requests.post(url=url, data=request_body, headers=header_global)
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
    
<<<<<<< HEAD
def get_standard_sheets(budget_versions, timespan_start, timespan_end):
        for version in budget_versions:
                request_body = request_body_data(version["name"], timespan_start, timespan_end)
                print(request_body)
                output_sheet = pd.DataFrame(get_budget_data(version["name"], request_body), columns=['Account_Name', 'Account_Code', 'Level_Code', 'Version', 'Year', 'Month', 'Amount', 'Sheet_type'])

                save_dataframe_to_blob(output_sheet, f'{version["name"]} - standard_sheet.csv')
                
def save_dataframe_to_blob(df, file_name):
=======

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

>>>>>>> parent of 87dd92f (Added func for get all)

                data3.to_csv(file_name, mode='a', index=False)



