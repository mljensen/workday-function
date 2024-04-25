import azure.functions as func
import logging
from workday_utils import get_standard_sheets



app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')


    action = req.params.get('action')
    
    if action:
        try:
            print('Standard sheets were selected')
            logging.info('Run sheets was selected')
            get_standard_sheets()
            message = 'get_standard_sheets function was executed successfully'
            
        except Exception as e:
            logging.error(f"Error executing get_standard_sheets: {str(e)}")
            return func.HttpResponse(
                f"Failed to execute get_standard_sheets: {str(e)}",
                status_code=500
            )

    elif action:
        return func.HttpResponse(f"Action {action} is not recognized.", status_code=400)
    
    else:    
        return func.HttpResponse(
            message if action else "Pass an action in the query string for specific operations.",
            status_code=200
    )
    
    
    
    
    # name = req.params.get('name')
    # if not name:
    #     try:
    #         req_body = req.get_json()
    #     except ValueError:
    #         pass
    #     else:
    #         name = req_body.get('name')

    # if name:
    #     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")

    # else:
    #     return func.HttpResponse(
    #          "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
    #          status_code=200
    #     )
        
