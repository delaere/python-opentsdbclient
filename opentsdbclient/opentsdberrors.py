#Every request will be returned with a standard HTTP response code. Most responses will include content, particularly error codes that will include details in the body about what went wrong. 

otsdbErrors = {
# Successful codes returned from the API include:
200:"The request completed successfully",
204:"The server has completed the request successfully but is not returning content in the body. This is primarily used for storing data points as it is not necessary to return data to caller",
301:"This may be used in the event that an API call has migrated or should be forwarded to another server",
304:"The call did not provide any data to store.",

# Common error response codes include:
400:"Information provided by the API user, via a query string or content data, was in error or missing. This will usually include information in the error body about what parameter caused the issue. Correct the data and try again.",
404:"The requested endpoint or file was not found. This is usually related to the static file endpoint.",
405:"The requested verb or method was not allowed. Please see the documentation for the endpoint you are attempting to access",
406:"The request could not generate a response in the format specified. For example, if you ask for a PNG file of the logs endpoing, you will get a 406 response since log entries cannot be converted to a PNG image (easily)",
408:"The request has timed out. This may be due to a timeout fetching data from the underlying storage system or other issues",
413:"The results returned from a query may be too large for the server's buffers to handle. This can happen if you request a lot of raw data from OpenTSDB. In such cases break your query up into smaller queries and run each individually",
500:"An internal error occured within OpenTSDB. Make sure all of the systems OpenTSDB depends on are accessible and check the bug list for issues",
501:"The requested feature has not been implemented yet. This may appear with formatters or when calling methods that depend on plugins",
503:"A temporary overload has occurred. Check with other users/applications that are interacting with OpenTSDB and determine if you need to reduce requests or scale your system.",
}

#Field 	Name	Data Type	Always Present	Description
#code		Integer		Yes		The HTTP status code
#message	String		Yes		A descriptive error message about what went wrong
#details	String		Optional	Details about the error, often a stack trace
#trace		String		Optional	A JAVA stack trace describing the location where the error was generated.

class OpenTSDBError(Exception):
    def __init__(self, code, message, details, trace):
        self.code = code
        self.description = otsdbErrors[code]
        self.message = message
        self.details = details
        self.trace = trace

    def __str__(self):
        return "Error %d: %s"%(self.code, self.message)


def checkErrors(response, throw=False, allow=[200, 204, 301]):
    """Check for errors and either raise an error via the requests module or returns a dict representation of the error."""
    if response.status_code in allow:
        return None
    else:
        if throw:
            response.raise_for_status()
        else:
            try:
                content = response.json()
                error = content["error"]
            except (KeyError, ValueError):
                return {
                            "code": response.status_code, 
                            "message": otsdbErrors[response.status_code],
                            "details": "Error message not received."
                       }
            else:
                return error

