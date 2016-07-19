The REST API is defined in http://opentsdb.net/docs/build/html/api_http/index.html . Some extracts are copied below.

According to that documentation, the HTTP API is RESTful in nature but provides alternative access through various overrides since not all clients can adhere to a strict REST protocol. The default data exchange is via JSON though pluggable formatters may be accessed, via the request, to send or receive data in different formats. Standard HTTP response codes are used for all returned results and errors will be returned as content using the proper format.

As of yet, OpenTSDB lacks an authentication and access control system. Therefore no authentication is required when accessing the API. If you wish to limit access to OpenTSDB, use network ACLs or firewalls to block access. We do not recommend running OpenTSDB on a machine with a public IP Address.

OpenTSDB 2.0's API call calls are versioned so that users can upgrade with gauranteed backwards compatability. To access a specific API version, you craft a URL such as /api/v<version>/<endpoint> such as /api/v2/suggest. This will access version 2 of the suggest endpoint. Versioning starts at 1 for OpenTSDB 2.0.0. Requests for a version that does not exist will result in calls to the latest version. Also, if you do not supply an explicit version, such as /api/suggest, the latest version will be used.

The API can accept body content that has been compressed. Make sure to set the Content-Encoding header to gzip and pass the binary encoded data over the wire. This is particularly useful for posting data points to the /api/put endpoint. 

Supported API Endpoints:

/api/aggregators
/api/annotation
/api/config
/api/dropcaches
/api/put
/api/query
/api/search
/api/serializers
/api/stats
/api/suggest
/api/tree
/api/uid
/api/version


