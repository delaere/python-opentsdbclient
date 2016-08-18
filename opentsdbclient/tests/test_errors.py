# Copyright 2016: C. Delaere
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.


from testtools import TestCase
from opentsdberrors import OpenTSDBError, otsdbErrors, checkErrors
from client import checkArg, checkArguments, process_response, RESTOpenTSDBClient
from requests import Response
from requests.exceptions import HTTPError
import json
import requests

class FakeResponse:
    def __init__(self,status_code,content):
        self.status_code = status_code
        self.content = content
        self.text = content

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
        expectedResult = { "code":400, "message":otsdbErrors[400], "details": respContent }

        self.assertEqual(expectedResult,checkErrors(response, False))
        self.assertRaises(HTTPError,checkErrors,response, True)

        response.content = """{ "noerror":"happy" }"""
        expectedResult = { "code":400, "message":otsdbErrors[400], "details": respContent }
        
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

    def test_process_response(self):
        """test the small method that processes the response for the client"""

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
        # no error status 200: should return the json content
        self.assertEqual(json.loads(respContent),process_response(response))

        # no error status 204: should return None
	response = FakeResponse(204,respContent)
        self.assertEqual(None,process_response(response))

        # error: should raise the error 
        respContent = """{
            "error": {
                    "code": 400,
                    "message": "Missing parameter <code>type</code>",
                    "details": "What the heck?",
                    "trace": "net.opentsdb.tsd.BadRequestException: Missing parameter <code>type</code>at net.opentsdb.tsd.BadRequestException.missingParameter(BadRequestException.java:78) ~[bin/:na]at net.opentsdb.tsd.HttpQuery.getRequiredQueryStringParam(HttpQuery.java:250) ~[bin/:na]at net.opentsdb.tsd.SuggestRpc.execute(SuggestRpc.java:63) ~[bin/:na]at net.opentsdb.tsd.RpcHandler.handleHttpQuery(RpcHandler.java:172) [bin/:na]at net.opentsdb.tsd.RpcHandler.messageReceived(RpcHandler.java:120) [bin/:na]at org.jboss.netty.channel.SimpleChannelUpstreamHandler.handleUpstream(SimpleChannelUpstreamHandler.java:75) [netty-3.5.9.Final.jar:na]at org.jboss.netty.channel.DefaultChannelPipeline.sendUpstream(DefaultChannelPipeline.java:565) [netty-3.5.9.Final.jar:na]....at java.lang.Thread.run(Unknown Source) [na:1.6.0_26]"
                    }}"""
        response = FakeResponse(400,respContent)
        e = self.assertRaises(OpenTSDBError,process_response,response)
        fullerror = json.loads(respContent)["error"]
        self.assertEqual(fullerror["code"],e.code)
        self.assertEqual(fullerror["message"],e.message)
        self.assertEqual(fullerror["details"],e.details)
        self.assertEqual(fullerror["trace"],e.trace)

    def test_checkArg(self):
        """Here we test the checkArg method used to check arguments of client methods"""

        # value None: should return True if NoneAllowed is True, raise ValueError otherwise
        # in that last case, the message should be valueErrorMessage

        self.assertEqual(True,checkArg(self.getUniqueInteger(),int,True))
        self.assertEqual(True,checkArg(None,int,True))
        message = self.getUniqueString()
        e = self.assertRaises(ValueError,checkArg,None,int,False,"Type mismatch",None,message)
        self.assertEqual(message,e.message)

        # type mismatch
        message = self.getUniqueString()
        e = self.assertRaises(TypeError,checkArg,self.getUniqueInteger(),float,False,message)
        self.assertEqual(message,e.message)

        # valueCheck error
        self.assertEqual(True,checkArg(abs(self.getUniqueInteger()),int,False,"Type mismatch",lambda x:x>0,message))
        message = self.getUniqueString()
        e = self.assertRaises(ValueError,checkArg,abs(self.getUniqueInteger()),int,False,"Type mismatch",lambda x:x<0,message)
        self.assertEqual(message,e.message)

    def test_checkArguments(self):
        """Test the top-level method to validate arguments of the client methods."""
        
        #the function uses the frame to get the context. It cannot be validated standalone.
        #we therefore use the client.delete_annotation method, disabling the requests.delete method.
        #for the method to work, it is enough for the delete replacement to return data as the FakeResponse content.

        def my_delete(url,data): return FakeResponse(200,data)
        self.patch(requests, 'delete', my_delete)
        client = RESTOpenTSDBClient("localhost",4242,"2.2.0")

        # this should pass
        client.delete_annotation(abs(self.getUniqueInteger()),abs(self.getUniqueInteger()),"000001000001000001")
        client.delete_annotation(abs(self.getUniqueInteger()),abs(self.getUniqueInteger()))

        # this should fail. We check the error template.
        e = self.assertRaises(ValueError,client.delete_annotation,-1)
        self.assertEqual("delete_annotation::startTime: got -1",e.message)
        e = self.assertRaises(ValueError,client.delete_annotation,"2016-01-01")
        self.assertEqual("delete_annotation::startTime: got 2016-01-01",e.message)
        e = self.assertRaises(TypeError,client.delete_annotation,3.4)
        self.assertEqual("delete_annotation::startTime: Type mismatch", e.message)

