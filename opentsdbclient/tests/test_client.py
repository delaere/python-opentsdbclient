from testtools import TestCase
from client import RESTOpenTSDBClient
from opentsdbobjects import OpenTSDBMeasurement, OpenTSDBTimeSeries
import uuid
import time

# there should be two parts:
# 1. standalone test of the client. See if it makes sense.
# http://stackoverflow.com/questions/9559963/unit-testing-a-python-app-that-uses-the-requests-library
# or
# https://pypi.python.org/pypi/httmock/

# 2. full test with a test server - see how to do that practically, can only work for a precise sequence -> composite tests

class TestClientServer(TestCase):
    """Tests implying a running test server on localhost"""

    def __init__(self, *args, **kwargs):
        self.client = RESTOpenTSDBClient("localhost",4242)
        try:
            self.version = int(self.client.get_version()["version"].split('.')[0])
        except:
            self.version = None
        super(TestClientServer, self).__init__(*args, **kwargs)

    # simple info methods

    def test_get_statistics(self):
        if self.version is not 2: self.skipTest("No server running")
        stats= self.client.get_statistics()
        # should return a list of measurements.
        for s in stats:
            self.assertIsInstance(s,OpenTSDBMeasurement)

    def test_get_filters(self):
        if self.version is not 2: self.skipTest("No server running")
        filters = self.client.get_filters()
        # default filters should be there
        for std in ["not_iliteral_or","literal_or","wildcard","iliteral_or","regexp","iwildcard","not_literal_or"]:
            self.assertIn(std,filters)

    def test_get_configuration(self):
        if self.version is not 2: self.skipTest("No server running")
        conf = self.client.get_configuration()
        # all configuration items start with "tsd."
        for c in conf:
            self.assertEqual("tsd.",c[:4])

    def test_drop_caches(self):
        if self.version is not 2: self.skipTest("No server running")
        result = self.client.drop_caches()
        # this should always return this
        expected = {u'status': u'200', u'message': u'Caches dropped'}
        self.assertEqual(expected,result)

    def test_get_serializers(self):
        if self.version is not 2: self.skipTest("No server running")
        serializers = self.client.get_serializers()
        # This endpoint should always return data with the JSON serializer as the default.
        self.assertEqual(any(s["serializer"]=="json" for s in serializers),True)

    def test_get_version(self):
        if self.version is not 2: self.skipTest("No server running")
        v = self.client.get_version()
        self.assertEqual(2,int(v["version"].split('.')[0]))
        self.assertEqual([u'full_revision', u'repo_status', u'timestamp', u'short_revision', u'repo', u'host', u'version', u'user'],v.keys())

    def test_get_aggregators(self):
        if self.version is not 2: self.skipTest("No server running")
        a = self.client.get_aggregators()
        for std in ['sum', 'min', 'avg', 'dev', 'max', 'count']:
            self.assertIn(std,a)

    # create content in the db

    def test_assign_uid(self):
        if self.version is not 2: self.skipTest("No server running")
        # nb: this will assign a uid to a random tagk at least.
        # the first time, it may also create other uids
        # the next times, there should be existing uids.
        uid = str(uuid.uuid4())
        response = self.client.assign_uid(["sys.cpu.0","sys.cpu.1","illegal!character"],["host"],["web01","web02","web03",uid])
        self.assertIn('illegal!character', response["metric_errors"])
        self.assertIn(uid,response["tagv"])

    def test_put_measurements(self):
        if self.version is not 2: self.skipTest("No server running")
        # basic functional tests
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host": "web01","dc": "lga"})
        ts.assign_uid(self.client) # make sure that the time series is known
        meas = OpenTSDBMeasurement(ts,int(time.time()),18)
        response = self.client.put_measurements([meas], details=True) # this should work
        self.assertEqual(response["success"],1)
        wrong_meas = OpenTSDBMeasurement(OpenTSDBTimeSeries(str(uuid.uuid4()),{"host": "web01","dc": "lga"}),int(time.time()),18)
        response = self.client.put_measurements([wrong_meas], details=True) # this should fail with 'Unknown metric' error
        self.assertEqual('Unknown metric',response["errors"][0]["error"])
        #TODO
        # check options: summary, details, sync, compress

    # annotations (R/W)
    
    def test_annotation(self):
        if self.version is not 2: self.skipTest("No server running")
        pass

    # ts meta (R/W) _ includes define_retention

    def test_tsmeta(self):
        if self.version is not 2: self.skipTest("No server running")
        pass

    # uid meta (R/W)

    def test_uidmeta(self):
        if self.version is not 2: self.skipTest("No server running")
        pass

    # queries

    def test_suggest(self):
        if self.version is not 2: self.skipTest("No server running")
        pass

    def test_query(self):
        if self.version is not 2: self.skipTest("No server running")
        pass

    def test_search(self):
        if self.version is not 2: self.skipTest("No server running")
        pass

    # tree manipulation

    def test_tree(self):
        if self.version is not 2: self.skipTest("No server running")
        pass

