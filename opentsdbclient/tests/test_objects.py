from opentsdbobjects import *
from testtools import TestCase
from client import RESTOpenTSDBClient
from requests.exceptions import HTTPError
import json
import requests
import time


class FakeResponse:
    def __init__(self,status_code,content):
        self.status_code = status_code
        self.content = content

    def json(self):
        return json.loads(self.content)

    def raise_for_status(self):
        if self.status_code>=400:
            raise HTTPError()


class TestOpenTSDBAnnotation(TestCase):

    def test_check(self):
        """test the constructor"""

        ts = int(time.time())
        # valid cases
        a = OpenTSDBAnnotation(ts)
        a = OpenTSDBAnnotation(1369141261,1369141262,"000001000001000001","Network Outage","Switch #5 died and was replaced",{"owner": "jdoe","dept": "ops"})

        # start NaN or negative
        self.assertRaises(ValueError,OpenTSDBAnnotation,self.getUniqueString())
        self.assertRaises(ValueError,OpenTSDBAnnotation,-ts)

        # tsuid not a string or not a base 16 representation
        self.assertRaises(ValueError,OpenTSDBAnnotation,1369141261,1369141262,000001000001000001)
        self.assertRaises(ValueError,OpenTSDBAnnotation,1369141261,1369141262,"00000100000100000Z")

        # description, note not a string
        self.assertRaises(ValueError,OpenTSDBAnnotation,1369141261,1369141262,"000001000001000001",self.getUniqueInteger())
        self.assertRaises(ValueError,OpenTSDBAnnotation,1369141261,1369141262,"000001000001000001","",self.getUniqueInteger())

        # custom not a dict of strings
        self.assertRaises(AttributeError,OpenTSDBAnnotation,ts,custom=False)
        self.assertRaises(ValueError,OpenTSDBAnnotation,ts,custom={"dept": 10})
        self.assertRaises(ValueError,OpenTSDBAnnotation,ts,custom={10: "jdoe"})


    def test_getMap(self):
        ts = int(time.time())
        # valid cases
        a = OpenTSDBAnnotation(ts)
        expected = {'startTime': ts}
        self.assertEqual(expected,a.getMap())

        a = OpenTSDBAnnotation(1369141261,1369141262,"000001000001000001","Network Outage","Switch #5 died and was replaced",{"owner": "jdoe","dept": "ops"})
        expected = {'tsuid': '000001000001000001', 'description': 'Network Outage', 'notes': 'Switch #5 died and was replaced', 
                    'custom': {'owner': 'jdoe', 'dept': 'ops'}, 'startTime': 1369141261, 'endTime': 1369141262}
        self.assertEqual(expected,a.getMap())

    def test_client(self):
        """test the interaction with the client with request emulation"""

        # load
        a = OpenTSDBAnnotation(1369141261,1369141262,"000001000001000001","Network Outage","Switch #5 died and was replaced",{"owner": "jdoe","dept": "ops"})
        b = OpenTSDBAnnotation(1369141261,1369141262,"000001000001000001")
        def my_get(url,data): return FakeResponse(200,json.dumps(a.getMap()))
        self.patch(requests, 'get', my_get)
	client = RESTOpenTSDBClient("localhost",4242)
        b.loadFrom(client)
        self.assertEqual(a.getMap(),b.getMap())

        # save
        a = OpenTSDBAnnotation(1369141261,1369141262,"000001000001000001","Network Outage","Switch #5 died and was replaced",{"owner": "jdoe","dept": "ops"})
        def my_post(url,data): return FakeResponse(200,data)
	self.patch(requests, 'post', my_post)
        a.saveTo(client)

        # delete
	def my_delete(url,data): return FakeResponse(200,data)
	self.patch(requests, 'delete', my_delete)
	client = RESTOpenTSDBClient("localhost",4242)
        a = OpenTSDBAnnotation(1369141261,1369141262,"000001000001000001","Network Outage","Switch #5 died and was replaced",{"owner": "jdoe","dept": "ops"})
        a.delete(client)


class TestOpenTSDBTimeSeries(TestCase):

    def test_check(self):
        # valid examples
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01", "dc": "lga"})
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01", "dc": "lga"},'0000150000070010D0')

        # metric is a proper string
        self.assertRaises(ValueError,OpenTSDBTimeSeries,"sys cpu nice",{"host":"web01", "dc": "lga"})
        self.assertRaises(TypeError,OpenTSDBTimeSeries,self.getUniqueInteger(),{"host":"web01", "dc": "lga"})

        # tags is a dict of strings with the proper format
        self.assertRaises(ValueError,OpenTSDBTimeSeries,"sys.cpu.nice",{"host ":"web01", "dc": "lga"})
        self.assertRaises(ValueError,OpenTSDBTimeSeries,"sys.cpu.nice",{"host":"web01", "dc": "lga "})

        # no tags
        self.assertRaises(ValueError,OpenTSDBTimeSeries,"sys.cpu.nice",{})

        # invalid tsuid
        self.assertRaises(ValueError,OpenTSDBTimeSeries,"sys.cpu.nice",{"host":"web01", "dc": "lga"},"Z")

    def test_getMap(self):
        # compare to the expected for a full example
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01", "dc": "lga"},'0000150000070010D0')
        expected = {'tsuid': '0000150000070010D0', 'metric': 'sys.cpu.nice', 'tags': {'host': 'web01', 'dc': 'lga'}}
        self.assertEqual(expected,ts.getMap())
        # same with full content including default meta
        expected = {'tagk_meta': {'host': {'displayName': '', 'description': '', 'created': 0, 'notes': '', 'custom': {}, 'uid': '', 'type': 'TAGK', 'name': 'host'}, 
                                  'dc': {'displayName': '', 'description': '', 'created': 0, 'notes': '', 'custom': {}, 'uid': '', 'type': 'TAGK', 'name': 'dc'}}, 
                    'tagv_meta': {'lga': {'displayName': '', 'description': '', 'created': 0, 'notes': '', 'custom': {}, 'uid': '', 'type': 'TAGV', 'name': 'lga'}, 
                                  'web01': {'displayName': '', 'description': '', 'created': 0, 'notes': '', 'custom': {}, 'uid': '', 'type': 'TAGV', 'name': 'web01'}}, 
                    'metric': 'sys.cpu.nice', 
                    'tags': {'host': 'web01', 'dc': 'lga'}, 
                    'metric_meta': {'displayName': '', 'description': '', 'created': 0, 'notes': '', 'custom': {}, 'uid': '', 'type': 'METRIC', 'name': 'sys.cpu.nice'}, 
                    'metadata': {'tsuid': '0000150000070010D0', 'displayName': '', 'lastReceived': 0, 'min': 'NaN', 'dataType': '', 'max': 'NaN', 
                                 'notes': '', 'created': 0, 'custom': {}, 'totalDatapoints': 0, 'units': '', 'retention': 0, 'description': ''}}
        self.assertEqual(expected,ts.getMap(True))

    def test_tsString(self):
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01", "dc": "lga"})
        expected = "sys.cpu.nice{host=web01,dc=lga}"
        self.assertEqual(expected,ts.tsString())

    def test_client(self):
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01", "dc": "lga"})
        response = {
                     "metric": { "sys.cpu.nice": "000042"},
                     "tagv": {
                         "web01": "00001A",
                         "lga": "00001B"
		     },
                     "tagk": {
                         "host": "000012",
                         "dc": "000013"
                     }
                 }
        def my_post(url,data): return FakeResponse(200,json.dumps(response))
	self.patch(requests, 'post', my_post)
	client = RESTOpenTSDBClient("localhost",4242)
        ts.assign_uid(client)
        self.assertEqual("000042",ts.metric_meta.uid)
        self.assertEqual("00001A",ts.tagv_meta["web01"].uid)
        self.assertEqual("00001B",ts.tagv_meta["lga"].uid)
        self.assertEqual("000012",ts.tagk_meta["host"].uid)
        self.assertEqual("000013",ts.tagk_meta["dc"].uid)

        # we now test what happens if some uids are already assigned. Here the metric.
        response = {
                     "metric": {},
                     "metric_errors": {"sys.cpu.nice": "Name already exists with UID: 000042"},
                     "tagv": {
                         "web01": "00001A",
                         "lga": "00001B"
		     },

                     "tagk": {
                         "host": "000012",
                         "dc": "000013"
                     }
                 }
        def my_post(url,data): return FakeResponse(400,json.dumps(response))
	self.patch(requests, 'post', my_post)
	client = RESTOpenTSDBClient("localhost",4242)
        ts.assign_uid(client)
        self.assertEqual("000042",ts.metric_meta.uid)
        self.assertEqual("00001A",ts.tagv_meta["web01"].uid)
        self.assertEqual("00001B",ts.tagv_meta["lga"].uid)
        self.assertEqual("000012",ts.tagk_meta["host"].uid)
        self.assertEqual("000013",ts.tagk_meta["dc"].uid)

        # we now test what happens if some uids are already assigned. Here one tag.
        response = {
                     "metric": { "sys.cpu.nice": "000042"},
                     "tagv": {
                         "web01": "00001A"
		     },
                     "tagv_errors": {
                         "lga": "00001B"
                         },
                     "tagk": {
                         "dc": "000013"
                     },
                     "tagk_errors": {
                         "host": "Name already exists with UID: 0007E5"
                         }
                 }
        def my_post(url,data): return FakeResponse(400,json.dumps(response))
	self.patch(requests, 'post', my_post)
	client = RESTOpenTSDBClient("localhost",4242)
        ts.assign_uid(client)
        self.assertEqual("000042",ts.metric_meta.uid)
        self.assertEqual("00001A",ts.tagv_meta["web01"].uid)
        self.assertEqual("00001B",ts.tagv_meta["lga"].uid)
        self.assertEqual("0007E5",ts.tagk_meta["host"].uid)
        self.assertEqual("000013",ts.tagk_meta["dc"].uid)

        # test the loadFrom functionality
        # case 1: tsuid set
        ts = OpenTSDBTimeSeries(tsuid = '000005000001000002000002000006')
        #    1a get_tsmeta works -> returns meta
        response = {
            "tsuid": "000005000001000002000002000006",
            "metric": {
                "uid": "00002A",
                "type": "METRIC",
                "name": "sys.cpu.nice",
                "description": "System Nice CPU Time",
                "notes": "",
                "created": 1350425579,
                "custom": None,
                "displayName": ""
            },
            "tags": [
                {
                    "uid": "000001",
                    "type": "TAGK",
                    "name": "host",
                    "description": "Server Hostname",
                    "notes": "",
                    "created": 1350425579,
                    "custom": None,
                    "displayName": "host"
                },
                {
                    "uid": "000001",
                    "type": "TAGV",
                    "name": "web01",
                    "description": "Website hosting server",
                    "notes": "",
                    "created": 1350425579,
                    "custom": None,
                    "displayName": "Web Server 01"
                }
            ],
            "description": "Measures CPU activity",
            "notes": "",
            "created": 1350425579,
            "units": "",
            "retention": 0,
            "max": "NaN",
            "min": "NaN",
            "custom": {
                "owner": "Jane Doe",
                "department": "Operations",
                "assetTag": "12345"
            },
            "displayName": "",
            "dataType": "absolute",
            "lastReceived": 1350425590,
            "totalDatapoints": 12532
        }
        reference = {'metadata': {'created': 1350425579,
                                  'custom': {u'assetTag': u'12345',
                                             u'department': u'Operations',
                                             u'owner': u'Jane Doe'},
                                  'dataType': u'absolute',
                                  'description': u'Measures CPU activity',
                                  'displayName': u'',
                                  'lastReceived': 1350425590,
                                  'max': u'NaN',
                                  'min': u'NaN',
                                  'notes': u'',
                                  'retention': 0,
                                  'totalDatapoints': 12532,
                                  'tsuid': u'000005000001000002000002000006',
                                  'units': u''},
                     'metric': 'sys.cpu.nice',
                     'metric_meta': {'created': 1350425579,
                                     'custom': {},
                                     'description': u'System Nice CPU Time',
                                     'displayName': u'',
                                     'name': u'sys.cpu.nice',
                                     'notes': u'',
                                     'type': u'METRIC',
                                     'uid': u'00002A'},
                     'tagk_meta': { 'host': {'created': 1350425579,
                                            'custom': {},
                                            'description': u'Server Hostname',
                                            'displayName': u'host',
                                            'name': u'host',
                                            'notes': u'',
                                            'type': u'TAGK',
                                            'uid': u'000001'}},
                     'tags': {'host': 'web01'},
                     'tagv_meta': {'web01': {'created': 1350425579,
                                             'custom': {},
                                             'description': u'Website hosting server',
                                             'displayName': u'Web Server 01',
                                             'name': u'web01',
                                             'notes': u'',
                                             'type': u'TAGV',
                                             'uid': u'000001'}}}
        search_response = {u'totalResults': 1, u'metric': u'*', 
                           u'results': [{u'tsuid': u'000005000001000002000002000006', u'metric': u'sys.cpu.nice', 
                                         u'tags': {u'host': u'web01'}}], 
                           u'startIndex': 0, u'limit': 25, u'time': 25491.0, u'query': u'', u'type': u'LOOKUP'}
        def my_get(url,params): 
            if "use_meta" in params: return FakeResponse(200,json.dumps(search_response))  
            else: return FakeResponse(200,json.dumps(response))
	self.patch(requests, 'get', my_get)
	client = RESTOpenTSDBClient("localhost",4242)
	ts.loadFrom(client)
        self.assertEqual(ts.metadata.created,1350425579)
        self.assertEqual(reference,ts.getMap(full=True))
        #    1b get_tsmeta fails -> exception -> set_tsmeta returns meta
        def my_get(url,params): return FakeResponse(404,"")
        def my_post(url,data,params): return FakeResponse(200,json.dumps(response))
	self.patch(requests, 'get', my_get)
	self.patch(requests, 'post', my_post)
	client = RESTOpenTSDBClient("localhost",4242)
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01"},'0000150000070010D0')
	ts.loadFrom(client)
        self.assertEqual(reference,ts.getMap(full=True))
        # case 2: metric and tags set
        #    2a get_ts_meta returns one entry -> meta
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01"})
        response = [ response ]
        def my_get(url,params): return FakeResponse(200,json.dumps(response))
	self.patch(requests, 'get', my_get)
	client = RESTOpenTSDBClient("localhost",4242)
	ts.loadFrom(client)
        self.assertEqual(ts.metadata.created,1350425579)
        self.assertEqual(reference,ts.getMap(full=True))
        #    2b get_ts_meta returns no entry -> set_tsmeta with query string -> meta
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01"})
        get_response = [ ]
        post_response = response[0]
        def my_get(url,params): return FakeResponse(200,json.dumps(get_response))
        def my_post(url,data,params): return FakeResponse(200,json.dumps(post_response))
	self.patch(requests, 'get', my_get)
	self.patch(requests, 'post', my_post)
	client = RESTOpenTSDBClient("localhost",4242)
	ts.loadFrom(client)
        self.assertEqual(ts.metadata.created,1350425579)
        self.assertEqual(reference,ts.getMap(full=True))
        #    2c get_ts_meta returns two entries -> exception
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01"})
        get_response = [post_response, post_response]
        def my_get(url,params): return FakeResponse(200,json.dumps(get_response))
        self.patch(requests, 'get', my_get)
        client = RESTOpenTSDBClient("localhost",4242)
        self.assertRaises(ValueError,ts.loadFrom,client)
        # saveTo - just check that it runs.
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01"},'000005000001000002000002000006')
        ts.metric_meta.uid = "00002A"
        ts.metric_meta.type = "metric"
        ts.tagk_meta["host"].uid = "000001"
        ts.tagk_meta["host"].type = "tagk"
        ts.tagv_meta["web01"].uid = "000001"
        ts.tagv_meta["web01"].type = "tagv"
        def my_post(url,data,params=None): return FakeResponse(200,json.dumps({"tsuid":'000005000001000002000002000006', "uid":"00002A", "type":"METRIC"}))
	self.patch(requests, 'post', my_post) # just enough to make it run, but meaningless
        client = RESTOpenTSDBClient("localhost",4242)
        ts.saveTo(client)
        # deleteMeta - just check that it runs.
        def my_delete(url,data): return FakeResponse(204,"")
        self.patch(requests, 'delete', my_delete)
        client = RESTOpenTSDBClient("localhost",4242)
        ts.deleteMeta(client,True)


class TestOpenTSDBMeasurement(TestCase):

    def test_check(self):
        # a valid case
        m = OpenTSDBMeasurement(OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01", "dc": "lga"},'0000150000070010D0'),int(time.time()),self.getUniqueInteger())

        # Timestamps must be integers and be no longer than 13 digits
        self.assertRaises(ValueError,OpenTSDBMeasurement,OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01", "dc": "lga"}),12.5,self.getUniqueInteger())
        self.assertRaises(ValueError,OpenTSDBMeasurement,OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01", "dc": "lga"}),"",self.getUniqueInteger())
        self.assertRaises(ValueError,OpenTSDBMeasurement,OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01", "dc": "lga"}),12345678901234,self.getUniqueInteger())

        # Data point can have a minimum value of -9,223,372,036,854,775,808 and a maximum value of 9,223,372,036,854,775,807 (inclusive)
        # Floats are also valid and stored on 32 bits (IEEE 754 floating-point "single format" with positive and negative value support)
        # on most platforms, this means int or float, excluding long. But this would not be portable.
        self.assertRaises(ValueError,OpenTSDBMeasurement,OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01", "dc": "lga"}),int(time.time()),9223372036854775808)
        self.assertRaises(ValueError,OpenTSDBMeasurement,OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01", "dc": "lga"}),int(time.time()),-9223372036854775809)

    def test_getMap(self):
        m = OpenTSDBMeasurement(OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01", "dc": "lga"},'0000150000070010D0'),1346846400,18)
        expected = {'timestamp': 1346846400, 'tsuid': '0000150000070010D0', 'metric': 'sys.cpu.nice', 'value': 18, 'tags': {'host': 'web01', 'dc': 'lga'}}
        self.assertEqual(expected,m.getMap())

    def test_client(self):
        m = OpenTSDBMeasurement(OpenTSDBTimeSeries("sys.cpu.nice",{"host":"web01", "dc": "lga"},'0000150000070010D0'),int(time.time()),self.getUniqueInteger())
        def my_post(url,data): return FakeResponse(204,"")
        self.patch(requests, 'post', my_post)
        client = RESTOpenTSDBClient("localhost",4242)
        m.saveTo(client)


class TestOpenTSDBRule(TestCase):

    def test_check(self):
        # valid case
        r = OpenTSDBRule(abs(self.getUniqueInteger()), level=1, order=0, type="METRIC", description="Split the metric on periods", separator= "\\.")

        # tree, level, order not positive ints
        self.assertRaises(ValueError,OpenTSDBRule,-abs(self.getUniqueInteger()),abs(self.getUniqueInteger()),abs(self.getUniqueInteger()),type="METRIC")
        self.assertRaises(ValueError,OpenTSDBRule,abs(self.getUniqueInteger()),-abs(self.getUniqueInteger()),abs(self.getUniqueInteger()),type="METRIC")
        self.assertRaises(ValueError,OpenTSDBRule,abs(self.getUniqueInteger()),abs(self.getUniqueInteger()),-abs(self.getUniqueInteger()),type="METRIC")
        self.assertRaises(ValueError,OpenTSDBRule,3.1416,abs(self.getUniqueInteger()),abs(self.getUniqueInteger()),type="METRIC")
        self.assertRaises(ValueError,OpenTSDBRule,abs(self.getUniqueInteger()),3.1416,abs(self.getUniqueInteger()),type="METRIC")
        self.assertRaises(ValueError,OpenTSDBRule,abs(self.getUniqueInteger()),abs(self.getUniqueInteger()),3.1416,type="METRIC")

        # type not in ["METRIC","METRIC_CUSTOM","TAGK","TAGK_CUSTOM","TAGV_CUSTOM"] or missing
        self.assertRaises(ValueError,OpenTSDBRule,abs(self.getUniqueInteger()),type="RING")
        self.assertRaises(ValueError,OpenTSDBRule,abs(self.getUniqueInteger()))

        # other fields not strings
        self.assertRaises(ValueError,OpenTSDBRule,abs(self.getUniqueInteger()),0,0,"METRIC",3)
        self.assertRaises(ValueError,OpenTSDBRule,abs(self.getUniqueInteger()),0,0,"METRIC","",3)
        self.assertRaises(ValueError,OpenTSDBRule,abs(self.getUniqueInteger()),0,0,"METRIC","","",3)
        self.assertRaises(ValueError,OpenTSDBRule,abs(self.getUniqueInteger()),0,0,"METRIC","","","",3)
        self.assertRaises(ValueError,OpenTSDBRule,abs(self.getUniqueInteger()),0,0,"METRIC","","","","",3)
        self.assertRaises(ValueError,OpenTSDBRule,abs(self.getUniqueInteger()),0,0,"METRIC","","","","","",3)
        self.assertRaises(ValueError,OpenTSDBRule,abs(self.getUniqueInteger()),0,0,"METRIC","","","","","","",3,3)

    def test_getMap(self):
        # check the map
        r = OpenTSDBRule(abs(self.getUniqueInteger()), level=1, order=0, type="METRIC", description="Split the metric on periods", separator= "\\.")
        expected = {'type': 'METRIC', 'description': 'Split the metric on periods', 'level': 1, 'regexGroupIdx': 0, 'treeId': 1, 'separator': '\\.', 'order': 0}
        self.assertEqual(expected,r.getMap())

    def test_client(self):
        # use the client... this should just work.
        r = OpenTSDBRule(abs(self.getUniqueInteger()), level=1, order=0, type="METRIC", description="Split the metric on periods", separator= "\\.")
        # saveTo
        def my_post(url,data): return FakeResponse(200,json.dumps(r.getMap()))
        self.patch(requests, 'post', my_post)
        client = RESTOpenTSDBClient("localhost",4242)
        r.saveTo(client)
        # delete
        def my_delete(url,data): return FakeResponse(204,"")
        self.patch(requests, 'delete', my_delete)
        client = RESTOpenTSDBClient("localhost",4242)
        r.delete(client)

class TestOpenTSDBTreeDefinition(TestCase):

    def test_check(self):
        # typical example
        td = OpenTSDBTreeDefinition(self.getUniqueString(), self.getUniqueString(), self.getUniqueString())
        # full example with rules
        rules = {
                    "0": {
                        "0": {
                            "type": "TAGK",
                            "field": "host",
                            "regex": "",
                            "separator": "",
                            "description": "Hostname rule",
                            "notes": "",
                            "level": 0,
                            "order": 0,
                            "treeId": 1,
                            "customField": "",
                            "regexGroupIdx": 0,
                            "displayFormat": ""
                        }
                    },
                    "1": {
                        "0": {
                            "type": "METRIC",
                            "field": "",
                            "regex": "",
                            "separator": "",
                            "description": "",
                            "notes": "Metric rule",
                            "level": 1,
                            "order": 0,
                            "treeId": 1,
                            "customField": "",
                            "regexGroupIdx": 0,
                            "displayFormat": ""
                        }
                    }
                }
        td = OpenTSDBTreeDefinition(self.getUniqueString(), self.getUniqueString(), self.getUniqueString(),rules=rules, created=int(time.time()), treeId=self.getUniqueInteger())

    def test_getMap(self):
        td = OpenTSDBTreeDefinition("name","description","notes",rules={}, created=1469546577, treeId=1)
        expected = {'name': 'name', 'created': 1469546577, 'rules': {}, 'notes': 'notes', 'enabled': False, 'treeId': 1, 'strictMatch': False, 'storeFailures': False, 'description': 'description'}
        self.assertEqual(expected,td.getMap())
        rules = {
                    "0": {
                        "0": {
                            "type": "TAGK",
                            "field": "host",
                            "regex": "",
                            "separator": "",
                            "description": "Hostname rule",
                            "notes": "",
                            "level": 0,
                            "order": 0,
                            "treeId": 1,
                            "customField": "",
                            "regexGroupIdx": 0,
                            "displayFormat": ""
                        }
                    },
                    "1": {
                        "0": {
                            "type": "METRIC",
                            "field": "",
                            "regex": "",
                            "separator": "",
                            "description": "",
                            "notes": "Metric rule",
                            "level": 1,
                            "order": 0,
                            "treeId": 1,
                            "customField": "",
                            "regexGroupIdx": 0,
                            "displayFormat": ""
                        }
                    }
                }
        name=self.getUniqueString()
        description=self.getUniqueString()
        notes=self.getUniqueString()
        treeId=self.getUniqueInteger()
        created=int(time.time())
        td = OpenTSDBTreeDefinition(name,description,notes,rules=rules, created=created, treeId=treeId)
        expected = {'name': name, 'created': created, 'rules': rules, 'notes': notes, 'enabled': False, 'treeId': treeId, 'strictMatch': False, 'storeFailures': False, 'description': description}
        self.assertEqual(expected,td.getMap())

    def test_client(self):
        # create
        td = OpenTSDBTreeDefinition("")
        client = RESTOpenTSDBClient("localhost",4242)
        self.assertRaises(ValueError,td.create,client)
        td = OpenTSDBTreeDefinition(self.getUniqueString(), self.getUniqueString(), self.getUniqueString())
        client = RESTOpenTSDBClient("localhost",4242)
        tdmap_before = td.getMap()
        response = td.getMap()
        response["created"]=int(time.time())
        response["treeId"]=self.getUniqueInteger()
        def my_post(url,data): return FakeResponse(200,json.dumps(response))
        self.patch(requests, 'post', my_post)
        td.create(client) # after this, the object must have a created value and a treeId.
        tdmap_after = td.getMap()
        for k in tdmap_before:
            self.assertEqual(tdmap_before[k],tdmap_after[k])
        self.assertIsNot(tdmap_after["created"],None)
        self.assertEqual(tdmap_after["created"],response["created"])
        self.assertEqual(tdmap_after["treeId"],response["treeId"])
        self.assertRaises(ValueError,td.create,client) # a second call to creates should fail
        # saveTo
        td.description = self.getUniqueString() # change the description
        response = td.getMap()
        def my_post(url,data): return FakeResponse(200,json.dumps(response))
        self.patch(requests, 'post', my_post)
        td.saveTo(client)
        # delete
        def my_delete(url,data): return FakeResponse(204,"")
        self.patch(requests, 'delete', my_delete)
        client = RESTOpenTSDBClient("localhost",4242)
        td.delete(client)
        self.assertEqual(None,td.treeId)
        self.assertEqual(None,td.created)
        self.assertEqual({},td.rules)


#do nothing here for the following two classes that are 100% interacting with the db.
#class OpenTSDBTreeBranch:
#class OpenTSDBTree(OpenTSDBTreeBranch):

