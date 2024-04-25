import azure.functions as func
import logging
from workday_standard_sheet import get_standard_sheets
from workday_methods import get_methods

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    action = req.params.get('action')
    
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
        
