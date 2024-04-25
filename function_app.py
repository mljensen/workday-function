import json
from datetime import datetime

import azure.functions as func
import logging
from workday_standard_sheet import get_standard_sheets
from workday_methods import get_methods

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
timespan_start = ''

def validate_body(body: json):
    try:
        delta = datetime.strptime(body["timespan_end"], '%m/%Y') - datetime.strptime(body["timespan_start"], '%m/%Y')
        if delta.days > 1085:
            raise Exception('The timespan cannot be greater than 3 years!')
        
        if not 'name' in body["versions"][0]:
            raise Exception('expected a list of dictionaries in versions')
        return True
    except Exception as e:
        logging.warning(e)
        return False


@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    status_code = None
    status_message = ''
    
    try:
        body = req.get_json()
        if validate_body(body):
            try:
                logging.info('Standard sheets was selected')
                get_standard_sheets(body["versions"], body["timespan_start"], body["timespan_end"])
                logging.info('Standard sheet executed successfully!')
        
                status_code = 201
                status_message = 'Data was extracted succesfully to Azure blob storage'
            except:
                pass
    except:
        status_code = 400
        status_message = 'Bad Request. Expected a body with the following values: "timespan_start", "timespan_end", "versions"'
    finally:
         return func.HttpResponse(
             status_message,
             status_code=status_code
        )

    
    
    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')
    
    if action == 'standard_sheets':
        try:
            logging.info('Standard sheets was selected')
            get_standard_sheets()
            logging.info('Standard sheet executed successfully!')
            
        except Exception as e:
            logging.error(f"Error executing get_standard_sheets: {str(e)}")
            
            
    elif action == 'methods':
        try:
            logging.info('Methods was selected')
            get_methods()
            logging.info('Methods executed successfully!')
            
            
        except Exception as e:
            logging.error(f"Error executing get_methods: {str(e)}")
            
    elif action == 'get_all':
        try:
            logging.info('get_all was selected')
            get_methods()
            get_standard_sheets()
            logging.info('Get all executed successfully!')
            
        except Exception as e:
            logging.error(f"Error executing get_all: {str(e)}")
            
   
    else:  
        logging.warning('Unknown parameter provided')
    
    
    
    
    
    # name = req.params.get('name')

    # if name:
    #     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")

    # else:
    #     return func.HttpResponse(
    #          "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
    #          status_code=200
    #     )
        
