from testtools import TestCase
from opentsdberrors import OpenTSDBError, otsdbErrors, checkErrors
from client import checkArg, checkArguments, process_response #TODO: check these three methods
from requests import Response
from requests.exceptions import HTTPError
import json

class FakeResponse:
    def __init__(self,status_code,content):
        self.status_code = status_code
        self.content = content

    def json(self):
        return json.loads(self.content)

    def raise_for_status(self):
        if self.status_code>=400:
            raise HTTPError()

class TestOpenTSDBError(TestCase):
    """test the Exception class"""

    def raiseError(self,code,message,details,trace):
        raise OpenTSDBError(code,message,details,trace)

    def test_exception(self):
        """direct test of the exception"""
        code = 413
        description = otsdbErrors[code]
        message = self.getUniqueString()
        details = self.getUniqueString()
        trace = self.getUniqueString()

        e = self.assertRaises(OpenTSDBError, self.raiseError,code,message,details,trace)

        self.assertEqual(description, e.description)
        self.assertEqual(code,e.code)
        self.assertEqual(message,e.message)
        self.assertEqual(details,e.details)
        self.assertEqual(trace,e.trace)

    def test_checkErrors_withError(self):
        """test the small method that processes the server response"""
        
        # example error from the opentsdb web page
        respContent = """{
            "error": {
                    "code": 400,
                    "message": "Missing parameter <code>type</code>",
                    "trace": "net.opentsdb.tsd.BadRequestException: Missing parameter <code>type</code>at net.opentsdb.tsd.BadRequestException.missingParameter(BadRequestException.java:78) ~[bin/:na]at net.opentsdb.tsd.HttpQuery.getRequiredQueryStringParam(HttpQuery.java:250) ~[bin/:na]at net.opentsdb.tsd.SuggestRpc.execute(SuggestRpc.java:63) ~[bin/:na]at net.opentsdb.tsd.RpcHandler.handleHttpQuery(RpcHandler.java:172) [bin/:na]at net.opentsdb.tsd.RpcHandler.messageReceived(RpcHandler.java:120) [bin/:na]at org.jboss.netty.channel.SimpleChannelUpstreamHandler.handleUpstream(SimpleChannelUpstreamHandler.java:75) [netty-3.5.9.Final.jar:na]at org.jboss.netty.channel.DefaultChannelPipeline.sendUpstream(DefaultChannelPipeline.java:565) [netty-3.5.9.Final.jar:na]....at java.lang.Thread.run(Unknown Source) [na:1.6.0_26]"
                    }}"""
        response = FakeResponse(400,respContent)

        self.assertEqual(response.json()["error"],checkErrors(response, False))
        self.assertRaises(HTTPError,checkErrors,response, True)

        response.content = ""
        expectedResult = { "code":400, "message":otsdbErrors[400], "details": "Error message not received." }

        self.assertEqual(expectedResult,checkErrors(response, False))
        self.assertRaises(HTTPError,checkErrors,response, True)

        response.content = """{ "noerror":"happy" }"""
        expectedResult = { "code":400, "message":otsdbErrors[400], "details": "Error message not received." }
        
        self.assertEqual(expectedResult,checkErrors(response, False))
        self.assertRaises(HTTPError,checkErrors,response, True)

    def test_checkErrors_noError(self):
        """test the small method that processes the server response"""
        
        # some proper response from the opentsdb web page
        respContent = """{
                          "timestamp": "1362712695",
                          "host": "localhost",
                          "repo": "/opt/opentsdb/build",
                          "full_revision": "11c5eefd79f0c800b703ebd29c10e7f924c01572",
                          "short_revision": "11c5eef",
                          "user": "localuser",
                          "repo_status": "MODIFIED",
                          "version": "2.0.0"
                      }"""
	response = FakeResponse(200,respContent)

	self.assertEqual(None,checkErrors(response, False))


