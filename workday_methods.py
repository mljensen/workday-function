#Imports
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import csv
from io import StringIO
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import io

# Blob storage connection
connect_str = ""
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_name = "azure-webjobs-hosts"



# API connection
API_USERNAME = "etommerup@xellia2.com"
API_PASSWORD = "vxg_hmw!ckr_UPR9wae"

#Defining variables
url = 'https://api.adaptiveinsights.com/api/v38'

header_global = {'Content-Type': 'application/xml'}

call_method = ['exportLevels', 'exportAccounts', 'exportAttributes', 'exportVersions', 'exportDimensions']

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


#Creating Levels, Accounts, and Attributes tabels
def get_methods():
        for method in call_method:
                response = requests.post(url=url, data=request_body_general(method), headers=header_global)
                root = ET.fromstring(response.content)
                data = []
                
                match method:
                        case 'exportLevels':
                                level(root, './/level', data)
                                            
                                df = pd.DataFrame(data)
                                df = df.fillna('')
                                levels_file_name = 'levels.csv'
                                df.to_csv(levels_file_name, index=True)
                                print("Levels printed to CSV file")


                        case 'exportAccounts':
                                account(root, './/accounts', data)
                                
                                df = pd.DataFrame(data)
                                df = df.fillna('')
                                accounts_file_name = 'accounts.csv'
                                df.to_csv(accounts_file_name, index=True)
                                print("Accounts printed to CSV file")
         
                        
                        case 'exportAttributes':
                                attributes(root, './/attribute', data)
                                
                                df = pd.DataFrame(data)
                                df = df.fillna('')
                                attributes_file_name = 'attributes.csv'
                                df.to_csv(attributes_file_name, index=True)
                                print("Attributes printed to CSV file")
      

                        case 'exportVersions':
                                versions(root, './/version', data)
                                                        
                                df = pd.DataFrame(data)
                                df = df.fillna('')
                                versions_file_name = 'versions.csv'
                                df.to_csv(versions_file_name, index=True)
                                print("Versions printed to CSV file")
                                                

                        case 'exportDimensions':
                                dimensions(root, './/dimension', data)
                                
                                df = pd.DataFrame(data)
                                df = df.fillna('')
                                dimensions_file_name = 'dimensions.csv'
                                df.to_csv(dimensions_file_name, index=True)
                                print("Dimensions printed to CSV file")
                        
                        
def save_dataframe_to_blob(df, file_name):

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name, )
    blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
    
    print(f"{file_name} uploaded to blob storage")
