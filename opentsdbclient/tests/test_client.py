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
from client import RESTOpenTSDBClient
from opentsdbobjects import OpenTSDBMeasurement, OpenTSDBTimeSeries, OpenTSDBAnnotation
from opentsdbquery import OpenTSDBtsuidSubQuery, OpenTSDBMetricSubQuery, OpenTSDBQueryLast, OpenTSDBQuery, OpenTSDBFilter, OpenTSDBExpQuery
from opentsdberrors import OpenTSDBError
import uuid
import time
import random

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
        if self.version != 2: self.skipTest("No server running")
        stats= self.client.get_statistics()
        # should return a list of measurements.
        for s in stats:
            self.assertIsInstance(s,OpenTSDBMeasurement)

    def test_get_filters(self):
        if self.version != 2: self.skipTest("No server running")
        filters = self.client.get_filters()
        # default filters should be there
        for std in ["not_iliteral_or","literal_or","wildcard","iliteral_or","regexp","iwildcard","not_literal_or"]:
            self.assertIn(std,filters)

    def test_get_configuration(self):
        if self.version != 2: self.skipTest("No server running")
        conf = self.client.get_configuration()
        # all configuration items start with "tsd."
        for c in conf:
            self.assertEqual("tsd.",c[:4])

    def test_drop_caches(self):
        if self.version != 2: self.skipTest("No server running")
        result = self.client.drop_caches()
        # this should always return this
        expected = {'status': '200', 'message': 'Caches dropped'}
        self.assertEqual(expected,result)

    def test_get_serializers(self):
        if self.version != 2: self.skipTest("No server running")
        serializers = self.client.get_serializers()
        # This endpoint should always return data with the JSON serializer as the default.
        self.assertEqual(any(s["serializer"]=="json" for s in serializers),True)

    def test_get_version(self):
        if self.version != 2: self.skipTest("No server running")
        v = self.client.get_version()
        self.assertEqual(2,int(v["version"].split('.')[0]))
        self.assertEqual(['full_revision', 'repo_status', 'timestamp', 'short_revision', 'repo', 'host', 'version', 'user'],list(v.keys()))

    def test_get_aggregators(self):
        if self.version != 2: self.skipTest("No server running")
        a = self.client.get_aggregators()
        for std in ['sum', 'min', 'avg', 'dev', 'max', 'count']:
            self.assertIn(std,a)

    def test_assign_uid(self):
        if self.version != 2: self.skipTest("No server running")
        # nb: this will assign a uid to a random tagk at least.
        # the first time, it may also create other uids
        # the next times, there should be existing uids.
        uid = str(uuid.uuid4())
        response = self.client.assign_uid(["sys.cpu.0","sys.cpu.1","illegal!character"],["host"],["web01","web02","web03",uid])
        self.assertIn('illegal!character', response["metric_errors"])
        self.assertIn(uid,response["tagv"])

    def test_put_measurements(self):
        if self.version != 2: self.skipTest("No server running")
        # basic functional tests
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host": "web01","dc": "lga"})
        ts.assign_uid(self.client) # make sure that the time series is known
        meas = OpenTSDBMeasurement(ts,int(time.time()),18)
        response = self.client.put_measurements([meas], details=True) # this should work
        self.assertEqual(response["success"],1)
        wrong_meas = OpenTSDBMeasurement(OpenTSDBTimeSeries(str(uuid.uuid4()),{"host": "web01","dc": "lga"}),int(time.time()),18)
        response = self.client.put_measurements([wrong_meas], details=True) # this should fail with 'Unknown metric' error
        self.assertEqual('Unknown metric',response["errors"][0]["error"])
        # check options: summary, details, sync, compress
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host": "web01","dc": "lga"})
        meas = OpenTSDBMeasurement(ts,int(time.time()),15)
        response = self.client.put_measurements([meas])
        self.assertEqual(None,response)
        response = self.client.put_measurements([meas], summary=True)
        self.assertEqual(response["success"],1)
        for i in ["failed", "success"]:
                self.assertIn(i,response)
        response = self.client.put_measurements([meas], sync=True, sync_timeout=10000)
        meas = OpenTSDBMeasurement(ts,int(time.time()),10)
        response = self.client.put_measurements([meas], compress=True)

    def test_annotation(self):
        if self.version != 2: self.skipTest("No server running")
        now = int(time.time())
        myAnnotation = self.client.set_annotation(now,
                                                  description="Testing Annotations", 
                                                  notes="These would be details about the event, the description is just a summary", 
                                                  custom={"owner": "jdoe","dept": "ops"})
        expected = {'custom':{"owner": "jdoe","dept": "ops"},'description': 'Testing Annotations',
                    'notes': 'These would be details about the event, the description is just a summary',
                    'startTime': now}

        self.assertEqual(expected,myAnnotation.getMap())

        myAnnotation_readback = self.client.get_annotation(now)
        self.assertEqual(expected,myAnnotation_readback.getMap())

        myAnnotation_modified = self.client.set_annotation(now,description="Testing Annotations - modified")
        expected["description"]="Testing Annotations - modified"
        self.assertEqual(expected,myAnnotation_modified.getMap())

        myAnnotation_readback = self.client.get_annotation(now)
        self.assertEqual(expected,myAnnotation_readback.getMap())
        
        response = self.client.delete_annotation(now)
        self.assertEqual(None,response)

        error = self.assertRaises(OpenTSDBError,self.client.get_annotation,now)
        self.assertEqual(error.code,404)
        self.assertEqual(error.message,"Unable to locate annotation in storage")

        # do the same using the annotation object

        now = int(time.time())
        myAnnotation = OpenTSDBAnnotation(now,
                                          description="Testing Annotations", 
                                          notes="These would be details about the event, the description is just a summary", 
                                          custom={"owner": "jdoe","dept": "ops"})
        myAnnotation.saveTo(self.client)
        expected = {'custom':{"owner": "jdoe","dept": "ops"},'description': 'Testing Annotations',
                    'notes': 'These would be details about the event, the description is just a summary',
                    'startTime': now}
        self.assertEqual(expected,myAnnotation.getMap())
        
        myAnnotation_readback = OpenTSDBAnnotation(now).loadFrom(self.client)
        self.assertEqual(expected,myAnnotation_readback.getMap())

        myAnnotation.description = "Testing Annotations - modified"
        expected["description"]="Testing Annotations - modified"
        myAnnotation.saveTo(self.client)
        self.assertEqual(expected,myAnnotation.getMap())

        myAnnotation.delete(self.client)
        time.sleep(5)
        error = self.assertRaises(OpenTSDBError,myAnnotation.loadFrom,self.client)
        self.assertEqual(error.code,404)
        self.assertEqual(error.message,"Unable to locate annotation in storage")

    # ts meta (R/W) _ includes define_retention

    def test_tsmeta(self):
        if self.version != 2: self.skipTest("No server running")

        host = self.getUniqueString()
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host": host,"dc": "lga"})
        ts.assign_uid(self.client) # make sure that the time series is known
        try:
            tsuid = self.client.set_tsmeta(metric="sys.cpu.nice{host=%s,dc=lga}"%host)["tsuid"] #get the tsuid - could be done via the OpenTSDBTimeSeries class
        except OpenTSDBError: # this may happen if the TS meta already exists from a previous aborted test.
            tsuid = self.client.get_tsmeta(metric="sys.cpu.nice{host=%s,dc=lga}"%host)[0]["tsuid"]

        # set, get, check, delete, check
        description = self.getUniqueString()
        displayName = self.getUniqueString()
        notes = self.getUniqueString()
        custom = { "from":self.getUniqueString(), "to":self.getUniqueString() }
        units = self.getUniqueString()
        dataType = self.getUniqueString()
        retention = self.getUniqueInteger()
        minimum = float(self.getUniqueInteger())
        maximum = float(self.getUniqueInteger())
        r = self.client.set_tsmeta(tsuid,description=description, displayName=displayName, notes=notes, custom=custom, units=units, dataType=dataType, retention=retention, minimum=minimum, maximum=maximum)
        self.assertEqual(description, r["description"])
        r2 = self.client.get_tsmeta(tsuid)
        self.assertEqual(r,r2)
        self.client.define_retention(tsuid,14)
        self.assertEqual(None,self.client.delete_tsmeta(tsuid))
        time.sleep(5)
        e = self.assertRaises(OpenTSDBError,self.client.get_tsmeta,tsuid)
        self.assertEqual(e.code,404)
        self.assertEqual(e.message,"Could not find Timeseries meta data")

    # uid meta (R/W)

    def test_uidmeta(self):
        if self.version != 2: self.skipTest("No server running")

        host = self.getUniqueString()
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host": host,"dc": "lga"})
        ts.assign_uid(self.client) # make sure that the time series is known
        try:
            tsuid = self.client.set_tsmeta(metric="sys.cpu.nice{host=%s,dc=lga}"%host)["tsuid"] #get the tsuid - could be done via the OpenTSDBTimeSeries class
        except OpenTSDBError: # this may happen if the TS meta already exists from a previous aborted test.
            tsuid = self.client.get_tsmeta(metric="sys.cpu.nice{host=%s,dc=lga}"%host)[0]["tsuid"]

        # get one uid and its meta from the timeseries
        uidmeta = self.client.get_tsmeta(metric="sys.cpu.nice{host=%s,dc=lga}"%host)[0]["metric"]
        uid = uidmeta["uid"]

        # set, get, check, delete, check
        description = self.getUniqueString()
        displayName = self.getUniqueString()
        notes = self.getUniqueString()
        custom = { "from":self.getUniqueString(), "to":self.getUniqueString() }
        r = self.client.set_uidmeta(uid, "metric", description=description, displayName=displayName, notes=notes, custom=custom)
        self.assertEqual(r["description"],description)
        self.assertEqual(r["displayName"],displayName)
        self.assertEqual(r["notes"],notes)
        self.assertEqual(r["custom"],custom)
        r2 = self.client.get_uidmeta(uid, "metric")
        self.assertEqual(r2,r)
        self.client.delete_uidmeta(uid, "metric")
        time.sleep(5)
        r3 = self.client.get_uidmeta(uid, "metric")
        default={"uid":uid,"type":"METRIC","name":"sys.cpu.nice","description":"","notes":"","created":0,"custom":None,"displayName":""}
        self.assertEqual(default,r3)

    # same using the objects...

    def test_OpenTSDBTimeSeries_meta(self):
        # this combines functionalities from both tsmeta and uidmeta
        if self.version != 2: self.skipTest("No server running")
        host = self.getUniqueString()
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host": host,"dc": "lga"})
        ts.assign_uid(self.client) # make sure that the time series is known
        try:
            tsuid = self.client.set_tsmeta(metric="sys.cpu.nice{host=%s,dc=lga}"%host)["tsuid"] #get the tsuid - could be done via the OpenTSDBTimeSeries class
        except OpenTSDBError: # this may happen if the TS meta already exists from a previous aborted test.
            tsuid = self.client.get_tsmeta(metric="sys.cpu.nice{host=%s,dc=lga}"%host)[0]["tsuid"]

        # load full metadata, including tsuid
        ts.loadFrom(self.client)
        self.assertEqual(tsuid,ts.metadata.tsuid)

        # set, get, check, delete, check
        description = self.getUniqueString()
        displayName = self.getUniqueString()
        notes = self.getUniqueString()
        custom = { "from":self.getUniqueString(), "to":self.getUniqueString() }
        units = self.getUniqueString()
        dataType = self.getUniqueString()
        retention = self.getUniqueInteger()
        minimum = float(self.getUniqueInteger())
        maximum = float(self.getUniqueInteger())
        ts.metadata.set(description=description, displayName=displayName, notes=notes, custom=custom, units=units, dataType=dataType, retention=retention, minimum=minimum, maximum=maximum)
        ts.tagk_meta["host"].description="The host name"
        ts.tagk_meta["host"].displayName="hostname"
        ts.tagv_meta[host].description="A randomly generated hostname"
        ts.tagv_meta[host].notes="Just for the sake of testing"
        ts.saveTo(self.client)
        self.assertEqual(description,ts.metadata.description)
        ts.loadFrom(self.client)
        self.assertEqual(description,ts.metadata.description)
        ts.deleteMeta(self.client)
        time.sleep(5)
        e = self.assertRaises(OpenTSDBError,self.client.get_tsmeta,tsuid)
        self.assertEqual(e.code,404)
        self.assertEqual(e.message,"Could not find Timeseries meta data")

    # queries

    def test_suggest(self):
        if self.version != 2: self.skipTest("No server running")
        host = self.getUniqueString()
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host": host,"dc": "lga"})
        ts.assign_uid(self.client) # make sure that the time series is known

        allhosts = self.client.suggest("tagv",host[0])
        thishost= self.client.suggest("tagv",host)
        somehosts = self.client.suggest("tagv",host[:len(host)/2])

        self.assertIn(host,allhosts)
        self.assertIn(host,thishost)
        self.assertIn(host,somehosts)
        
        self.assertIn("host",self.client.suggest("tagk","h"))

        self.assertIn("sys.cpu.nice",self.client.suggest("metrics","sys"))

    def test_search(self):
        if self.version != 2: self.skipTest("No server running")
        #limited to /api/search/lookup since others rely on plugins...
        host = self.getUniqueString()
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host": host,"dc": "lga"})
        ts.assign_uid(self.client) # make sure that the time series is known
        try:
            tsuid = self.client.set_tsmeta(metric="sys.cpu.nice{host=%s,dc=lga}"%host)["tsuid"] #get the tsuid - could be done via the OpenTSDBTimeSeries class
        except OpenTSDBError: # this may happen if the TS meta already exists from a previous aborted test.
            tsuid = self.client.get_tsmeta(metric="sys.cpu.nice{host=%s,dc=lga}"%host)[0]["tsuid"]

        r = self.client.search("LOOKUP",metric="sys.cpu.nice",tags={"host": host,"dc": "lga"})
        reference = {'tsuid': tsuid, 'metric': 'sys.cpu.nice', 'tags': {'host': host, 'dc': 'lga'}}
        self.assertIn(reference,r["results"])

    def test_query(self):
        if self.version != 2: self.skipTest("No server running")
        # prepare some data
        host = self.getUniqueString()
        ts = OpenTSDBTimeSeries("sys.cpu.nice",{"host": host,"dc": "lga"})
        ts.assign_uid(self.client) # make sure that the time series is known
        try:
            tsuid = self.client.set_tsmeta(metric="sys.cpu.nice{host=%s,dc=lga}"%host)["tsuid"] #get the tsuid - could be done via the OpenTSDBTimeSeries class
        except OpenTSDBError: # this may happen if the TS meta already exists from a previous aborted test.
            tsuid = self.client.get_tsmeta(metric="sys.cpu.nice{host=%s,dc=lga}"%host)[0]["tsuid"]
        meas = [OpenTSDBMeasurement(ts,int(time.time())-200+i,random.random()) for i in range(0,100)]
        response = self.client.put_measurements(meas, compress=True) 
        time.sleep(5)

        # prepare a query
        subquery = OpenTSDBtsuidSubQuery("sum",[tsuid])
        theQuery = OpenTSDBQuery([subquery],'1h-ago',showTSUIDs=True, showSummary=True, showQuery=True)
        # run!
        r = self.client.query(theQuery)
        results = r[0]["dps"]
        # test
        for m in meas: 
            self.assertIn(str(m.timestamp),results)
            self.assertTrue(float(results.get(str(m.timestamp),0))-m.value<1e-6)

        # repeat with a metric query
        filters = [OpenTSDBFilter("literal_or","host",host),OpenTSDBFilter("literal_or","dc","lga")]
        subquery = OpenTSDBMetricSubQuery("sum","sys.cpu.nice",filters=filters)
        theQuery = OpenTSDBQuery([subquery],'1h-ago',showTSUIDs=True, showSummary=True, showQuery=True)
        r2 = self.client.query(theQuery)
        results2 = r[0]["dps"]
        self.assertEqual(results,results2)

        ## repeat with an exp query 
        timeSection = OpenTSDBExpQuery.timeSection("sum", "1h-ago")
        filters = [OpenTSDBExpQuery.filters("id0",[OpenTSDBFilter("literal_or","host",host),OpenTSDBFilter("literal_or","dc","lga")])]
        metrics = [OpenTSDBExpQuery.metric("cpunice","id0","sys.cpu.nice")]
        expressions = [OpenTSDBExpQuery.expression("e1","cpunice*2")]
        outputs = [OpenTSDBExpQuery.output("cpunice","CPU nice"),OpenTSDBExpQuery.output("e1","CPU nice twice")]
        theQuery = OpenTSDBExpQuery(timeSection, filters, metrics, expressions, outputs)
        self.assertRaises(RuntimeError,self.client.query,theQuery) # 2.3 only...
        ## decode and test
        ## NOTE I am experiencing problems with 2.3-RC1... server crash with some basic query. Too early?

        # last query
        querylast = OpenTSDBQueryLast(metrics=[],tsuids=[tsuid],backScan=1,resolveNames=True)
        r = self.client.query(querylast)
        self.assertEqual(r[0]["timestamp"],meas[-1].timestamp*1000)
        self.assertTrue(float(r[0]["value"])-meas[-1].value<1e-6)


#NOTE implement this later
class TestTreeManipulation(TestCase):
    """Tests implying a running test server on localhost
       tree manipulation
       separate class with preparation code to fill fake data and termination code to cleanup"""

    def __init__(self, *args, **kwargs):
        self.client = RESTOpenTSDBClient("localhost",4242)
        try:
            self.version = int(self.client.get_version()["version"].split('.')[0])
        except:
            self.version = None
        super(TestTreeManipulation, self).__init__(*args, **kwargs)


    def test_tree(self):
        if self.version != 2: self.skipTest("No server running")

        #lots of things to try...

