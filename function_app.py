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
                get_methods()
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

    
    
    