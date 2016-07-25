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
        self.assertEqual("000042",ts.metric_uid)
        self.assertEqual("00001A",ts.tagv_uids["web01"])
        self.assertEqual("00001B",ts.tagv_uids["lga"])
        self.assertEqual("000012",ts.tagk_uids["host"])
        self.assertEqual("000013",ts.tagk_uids["dc"])

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
        self.assertEqual("000042",ts.metric_uid)
        self.assertEqual("00001A",ts.tagv_uids["web01"])
        self.assertEqual("00001B",ts.tagv_uids["lga"])
        self.assertEqual("000012",ts.tagk_uids["host"])
        self.assertEqual("000013",ts.tagk_uids["dc"])

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
        self.assertEqual("000042",ts.metric_uid)
        self.assertEqual("00001A",ts.tagv_uids["web01"])
        self.assertEqual("00001B",ts.tagv_uids["lga"])
        self.assertEqual("0007E5",ts.tagk_uids["host"])
        self.assertEqual("000013",ts.tagk_uids["dc"])


#TODO: test all objects standalone.

#class OpenTSDBMeasurement:
#class OpenTSDBTreeDefinition:
#class OpenTSDBRule:

#do nothing here for the following two classes that are 100% interacting with the db.
#class OpenTSDBTreeBranch:
#class OpenTSDBTree(OpenTSDBTreeBranch):

