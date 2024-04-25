#Imports
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import csv
from io import StringIO



#API connection
API_USERNAME = ""
API_PASSWORD = ""


#Defining variables
url = 'https://api.adaptiveinsights.com/api/v38'

header_global = {'Content-Type': 'application/xml'}

call_method = ['exportLevels', 'exportAccounts', 'exportAttributes', 'exportVersions', 'exportDimensions']

modelled_sheets =  ['Compensation'] #UPDATE to input parameter

#Request to get the different versions for the budget.
def request_body_version():
        body = (f"<?xml version='1.0' encoding='UTF-8'?><call method='exportVersions' callerName='Basico'><credentials login='{API_USERNAME}' password='{API_PASSWORD}'/></call>")
        return body
# Request to get exportData
def request_body_data(version):
    body = (f"<?xml version='1.0' encoding='UTF-8'?><call method='exportData' callerName='Basico'><credentials login='{API_USERNAME}' password='{API_PASSWORD}'/><version name='{version}'/><format useInternalCodes='true' includeUnmappedItems='false' /><filters><timeSpan start='01/2024' end='12/2024'/></filters></call>")
    return body

# Request to get the modelled data
def request_body_model_data(version, sheet):
    body = (f"<?xml version='1.0' encoding='UTF-8'?><call method='exportConfigurableModelData' callerName='Basico'><credentials login='{API_USERNAME}' password='{API_PASSWORD}'/><version name='{version}'/><modeled-sheet name='{sheet}' includeAllColumns='true' isGetAllRows='true'/></call>")
    return body

#Request to get the data for tables Levels, Account, Attributes, Versions and Dimensions
def request_body_general(method):
    body = (f"<?xml version='1.0' encoding='UTF-8'?><call method='{method}' callerName='Basico'><credentials login='{API_USERNAME}' password='{API_PASSWORD}'/></call>")
    return body

#Function for extraction data for Levels
def level(root, find_attribute, data):
    parent_map = {c: p for p in root.iter() for c in p}

    for level in root.findall(find_attribute):
        level_data = level.attrib.copy()  # Start with existing attributes

        # Get 'name' and 'currency' directly from attributes if present
        name = level.get('name')
        currency = level.get('currency')
        level_data['currency'] = currency

        # Handle parent or rollup
        parent = parent_map.get(level, None)
        rollup = parent.get('name', 'No Rollup') if parent else 'Top Level'
        level_data['rollup'] = rollup

        # Collecting attribute names and values within this level
        attributes = level.findall('.//attribute')
        attr_names = [attr.get('name') for attr in attributes]
        attr_values = {attr.get('name'): attr.get('value') for attr in attributes}

        # Setting attribute values using collected names
        for name in attr_names:
            if name:  # Check if name is not None or empty
                level_data[name] = attr_values.get(name)

        data.append(level_data)
    return data

#Function for extraction data for Accounts
def account(root, find_attribute, data):
        parent_map = {c: p for p in root.iter() for c in p}
        for account in root.iter('account'):
        # Extract all attributes dynamically
                account_data = account.attrib.copy()  # Use a copy to avoid modifying the original attributes directly
        # Add the parent 'name' attribute as 'rollup'
                parent = parent_map.get(account)
                if parent is not None:
                        account_data['rollup'] = parent.get('name', 'Top account')
                # Collecting attribute names and values
                attributes = account.findall('.//attribute')
                attr_names = [attr.get('name') for attr in attributes]
                attr_values = {attr.get('name'): attr.get('value') for attr in attributes}
                # Setting attribute values using collected names
                for name in attr_names:
                        if name:  # Check if name is not None or empty
                                account_data[name] = attr_values.get(name)
                # Add the account data to the list
                data.append(account_data)

                rollup = account_data.get('rollup')
                name = account_data.get('name', '')
                # rollup_cube_accounts = [
                # 'Patients', 'Monthly Fees', 'Accumulated Fees', 'Support Calculations'
                # ] # UPDATE: Must be input parameter
                # if rollup in rollup_cube_accounts:
                #         if len(name) < 2:
                #                 name = "No name"

        return data


def versions(root, find_element, data):
    parent_map = {c: p for p in root.iter() for c in p}
    for version in root.findall(find_element):
        # Create a dictionary to store version data dynamically
        version_data = version.attrib.copy()  # Start with existing attributes

        # If a version has a parent version, add the parent version details
        parent = parent_map.get(version, None)
        version_data['ParentID'] = parent.get('id', 'No Parent') if parent else 'No Parent'
        version_data['ParentName'] = parent.get('name', 'Top Level') if parent else 'Top Level'

        # Add the version data to the list
        data.append(version_data)

        # Handle nested elements such as 'versionDetail'
        for versionDetail in version.findall('versionDetail'):
            detail_data = versionDetail.attrib.copy()  # Start with existing attributes
            # Set the parent ID and name from the parent version
            detail_data['ParentID'] = version.get('id')
            detail_data['ParentName'] = version.get('name')

            # Add the version detail data
            data.append(detail_data)

    return data


def dimensions(root, find_element, data):
    for dimension in root.findall(find_element):
        # Create a dictionary to store dimension data dynamically
        dimension_data = dimension.attrib.copy()  # Start with existing attributes
        dimension_data['ParentID'] = 'No Parent'  # Default parent ID
        dimension_data['ParentName'] = 'Parent'  # Default parent name

        # Add the dimension data as the parent
        data.append(dimension_data)

        # Iterate over child dimension values
        for dimensionValue in dimension.findall('dimensionValue'):
            child_data = dimensionValue.attrib.copy()  # Start with existing attributes
            # Set the parent ID and name from the parent dimension
            child_data['ParentID'] = dimension.get('id')
            child_data['ParentName'] = dimension.get('name')

            # Add the child dimension data
            data.append(child_data)

    return data


def attributes(root, find_attribute, data):
    parent_map = {c: p for p in root.iter() for c in p}
    
    for attribute in root.findall(find_attribute):
        # Create a dictionary to store attribute data dynamically
        attribute_data = attribute.attrib.copy()  # Start with existing attributes
        parent = parent_map.get(attribute, None)

        # Set the parent ID and name
        attribute_data['ParentID'] = parent.get('id', 'No rollup') if parent else 'No rollup'
        attribute_data['ParentName'] = parent.get('name', 'Parent') if parent else 'Parent'

        # Add the attribute data as the parent
        data.append(attribute_data)

        # Iterate over child attribute values
        for attributeValue in attribute.findall('attributeValue'):
            child_data = attributeValue.attrib.copy()  # Start with existing attributes
            # Set the parent ID and name from the parent attribute
            child_data['ParentID'] = attribute.get('id')
            child_data['ParentName'] = attribute.get('name')

            # Add the child attribute data
            data.append(child_data)

    return data


# #Function for getting the name of the budgets
# UPDATE: Make this dynamic by adding a parameter to specify the parent versions - parse array
# --budget_version '{"Snowflake Dataload", "Rolling Forecast"}'

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

# #Getting the data from the budget version
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


def get_model_data(version, data, sheet):

        response = requests.post(url=url, data=request_body_model_data(version=version, sheet=sheet), headers=header_global)
        try:
                start_index = response.text.index("<![CDATA[") + len("<![CDATA[")
                end_index = response.text.index("]]>")
                csv_data = response.text[start_index:end_index]

                # Check if CSV data is not empty
                if not csv_data.strip():
                        print("CSV data is empty.")
                        return None

                # Load CSV data into a DataFrame
                data = pd.read_csv(StringIO(csv_data))
                return data

        except ValueError as e:
                print(f'Error parsing CSV data: {e}')
                return None
        except Exception as e:
                print(f'An error occurred: {e}')
                return None


def get_cube_data(version, data):
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
                                data.append([row[0], row[1], row[2], version, y, m, row[3 + int(i)], 'Cube sheet'])
                                i += 1
        return data

#Creating Levels, Accounts, and Attributes tabels
# for method in call_method:
#         response = requests.post(url=url, data=request_body_general(method), headers=header_global)
#         root = ET.fromstring(response.content)
#         data = []
#         headers = []
#         table_name = ''
        
        
#         match method:
#                 case 'exportLevels':
#                         level(root, './/level', data)
                            
                                               
#                         df = pd.DataFrame(data)
#                         # df.columns = df.columns.str.replace(r'[ &,;{}()\n\t=]+', '_', regex=True)
#                         df = df.fillna('')
                        
#                         levels_file_name = 'levels.csv'
                        
#                         #df.to_csv(levels_file_name, index=True)
#                         print("Levels printed to CSV file")

                      
                      
                        
#                 case 'exportAccounts':
#                         account(root, './/accounts', data)
                        
                        
#                         df = pd.DataFrame(data)
#                         #df.columns = df.columns.str.replace(r'[ &,;{}()\n\t=]+', '_', regex=True)
#                         df = df.fillna('')
                    
#                         accounts_file_name = 'accounts.csv'
#                         #df.to_csv(accounts_file_name, index=True)
#                         print("Accounts printed to CSV file")
                    
                                        
                      
#                 case 'exportAttributes':
#                         attributes(root, './/attribute', data)
                        
#                         #df = pd.DataFrame(data)
#                         #df.columns = df.columns.str.replace(r'[ &,;{}()\n\t=]+', '_', regex=True)
#                         #df = df.fillna('')
                        
#                         #attributes_file_name = 'attributes.csv'
                        
#                         #df.to_csv(attributes_file_name, index=True)
#                         #print("Attributes printed to CSV file")
                        
                                          
                      
              
                        
#                 case 'exportVersions':
#                         versions(root, './/version', data)
                                                
#                         df = pd.DataFrame(data)
#                         #df.columns = df.columns.str.replace(r'[ &,;{}()\n\t=]+', '_', regex=True)
#                         df = df.fillna('')
                        
#                         versions_file_name = 'versions.csv'
                        
#                         #df.to_csv(versions_file_name, index=True)
#                         print("Versions printed to CSV file")
                                            
                      
                        
                        
#                 case 'exportDimensions':
#                         dimensions(root, './/dimension', data)
                        
#                         df = pd.DataFrame(data)
#                         #df.columns = df.columns.str.replace(r'[ &,;{}()\n\t=]+', '_', regex=True)
#                         df = df.fillna('')
                        
#                         dimensions_file_name = 'dimensions.csv'
                        
#                         #df.to_csv(dimensions_file_name, index=True)
#                         print("Dimensions printed to CSV file")
                        
                                       
           
#Creating Data table
data = []
budget_versions = budget(data)

print(budget_versions)
        
def get_standard_sheets():
        pass

   
# For Standard data

for version in budget_versions:
        print(version)
        get_budget_data(version, data)
        data3 = pd.DataFrame(data, columns=['Account_Name', 'Account_Code', 'Level_Code', 'Version', 'Year', 'Month', 'Amount', 'Sheet_type'])
        file_name = 'standard_sheets.csv'
        print("We made it!")
        
        data3.to_csv(file_name, mode='a', index=False)
        
                




# for version in budget_versions:

#         print(version)
#         get_budget_data(version, data)
                

#         data3 = pd.DataFrame(data, columns=['Account_Name', 'Account_Code', 'Level_Code', 'Version', 'Year', 'Month', 'Amount', 'Sheet_type'])
        
#         file_name = 'standard_sheets.csv'

        
#         print('Data is not empty')
#         data3.to_csv(file_name, mode='a', index=True)
           
                    
  
   
    # #         # data3.to_parquet(f"abfss://837c7afd-52a0-47d3-ab3c-ab63ad5d5422@onelake.dfs.fabric.microsoft.com/f15c8131-864c-4978-8351-3f466c056ec8/Files/testFiles/{version}.parquet")
    # #         # data3 = spark.read.parquet(f"abfss://837c7afd-52a0-47d3-ab3c-ab63ad5d5422@onelake.dfs.fabric.microsoft.com/f15c8131-864c-4978-8351-3f466c056ec8/Files/testFiles/{version}.parquet")
    # #         # data3.write.mode("overwrite").format("delta").saveAsTable(version)


# For Modelled data 
# for sheet in modelled_sheets:
    
#         if sheet == 'Compensation':
#             data = []
#             budget_versions = budget(data)
#             for version in budget_versions:
#                         data = []
#                         print(version)
#                         data_df = get_model_data(version, data, sheet)
#                         print("Made it here")
#                         updated_data = []
#                         for i, row in enumerate(data):
                                
#                         # Append the sheet to the row
#                                 row.append(sheet)
                                
#                         # Update the row in data
#                                 data[i] = row
                        
                      
#                         data2 = pd.DataFrame(data, columns=['InternalID','Level','ID','Navidoc','Internal ID (Workday)','R&D Project','Activity','Area','Block','Category','Study Number',
#                                                            'Vendor','PO','RP','Commitment','Activity Status','Account','Account Number','Comment A','Comment B','Note to Amounts','Invoice Received',
#                                                            'Partner share','InputCurrency','Total Budget Amount','Cost [Project Currency]','Control [Project Currency]',
#                                                            'Year', 'Month', 'Version', 'Sheet type', 'Sheet Name'])
                        
                
                        
#                         if not data2.empty:
#                                 print(data2)
#                                 print(version)
#                                 print(data2)     
                            
                                


        