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
from opentsdbquery import OpenTSDBQuery, OpenTSDBMetricSubQuery, OpenTSDBtsuidSubQuery, OpenTSDBFilter, OpenTSDBExpQuery, OpenTSDBQueryLast

class TestOpenTSDBtsuidSubQuery(TestCase):
    """test the OpenTSDBtsuidSubQuery standalone"""

    def test_check(self):

        # this is a valid query
        q = OpenTSDBtsuidSubQuery("sum",["000001000002000042","000001000002000043"])

        # now we attack invalid cases

        testcases = [OpenTSDBtsuidSubQuery(self.getUniqueInteger(),["000001000002000042","000001000002000043"]),
                     OpenTSDBtsuidSubQuery("sum",self.getUniqueInteger()),
                     OpenTSDBtsuidSubQuery("sum",[self.getUniqueInteger()])]

        for q in testcases:
            self.assertRaises(TypeError,q.check)

        testcases = [OpenTSDBtsuidSubQuery("sum",[]),OpenTSDBtsuidSubQuery("sum",[self.getUniqueString()])]

        for q in testcases:
            self.assertRaises(ValueError,q.check)

    def test_map(self):

        q = OpenTSDBtsuidSubQuery("sum",["000001000002000042","000001000002000043"])
        expected = {"aggregator": "sum", "tsuids": ["000001000002000042","000001000002000043"]}
        self.assertEqual(expected,q.getMap())


class TestOpenTSDBMetricSubQuery(TestCase):
    """test the OpenTSDBMetricSubQuery standalone"""

    def test_check(self):
        """checks the constrains on the constructor"""

        # this is a valid query
        filters = [OpenTSDBFilter("wildcard","host","*",True),OpenTSDBFilter("literal_or","dc","lga|lga1|lga2")]
        q = OpenTSDBMetricSubQuery("sum", "sys.cpu.0", rate=True, filters=filters, counterMax=100, resetValue=1000, downsample="30m-avg-nan")
        q.check()

        # now we attack invalid cases

        testcases = [OpenTSDBMetricSubQuery(self.getUniqueInteger(),"sys.cpu.0"),
                     OpenTSDBMetricSubQuery("sum", self.getUniqueInteger()),
                     OpenTSDBMetricSubQuery("sum", "sys.cpu.0", rate=0.1),
                     OpenTSDBMetricSubQuery("sum", "sys.cpu.0", rate=True, counterMax=self.getUniqueString()),
                     OpenTSDBMetricSubQuery("sum", "sys.cpu.0", rate=True, downsample=self.getUniqueInteger()),
                     OpenTSDBMetricSubQuery("sum", "sys.cpu.*", explicitTags=self.getUniqueString())]

        for q in testcases:
            self.assertRaises(TypeError,q.check)

        q = OpenTSDBMetricSubQuery("sum", "sys.cpu.*")
        self.assertRaises(ValueError,q.check)


    def test_map(self):
        """Checks the resulting map"""

        # this is a query with everything
        filters = [OpenTSDBFilter("wildcard","host","*",True),OpenTSDBFilter("literal_or","dc","lga|lga1|lga2")]
        q = OpenTSDBMetricSubQuery("sum", "sys.cpu.0", rate=True, filters=filters, counterMax=100, resetValue=1000, downsample="30m-avg-nan")
        expected = {'aggregator': 'sum', 'metric': 'sys.cpu.0', 'rate': True, #'explicitTags': False,
                    'filters': [{'filter': '*', 'type': 'wildcard', 'groupBy': True, 'tagk': 'host', "groupBy":True}, 
                                {'filter': 'lga|lga1|lga2', 'type': 'literal_or', 'groupBy': False, 'tagk': 'dc'}], 
                    'rateOptions': {'counter': True, 'counterMax': 100, 'resetValue': 1000}}

        self.assertEqual(expected,q.getMap())
        
        # without rate

        filters = [OpenTSDBFilter("wildcard","host","*",True),OpenTSDBFilter("literal_or","dc","lga|lga1|lga2")]
        q = OpenTSDBMetricSubQuery("sum", "sys.cpu.0", rate=False, filters=filters, counterMax=100, resetValue=1000, downsample="30m-avg-nan")
        expected = {'aggregator': 'sum', 'metric': 'sys.cpu.0', #'explicitTags': False, 
                    'filters': [{'filter': '*', 'type': 'wildcard', 'groupBy': True, 'tagk': 'host', "groupBy":True}, 
                                {'filter': 'lga|lga1|lga2', 'type': 'literal_or', 'groupBy': False, 'tagk': 'dc'}]}
        
        self.assertEqual(expected,q.getMap())

        # minimal

        q = OpenTSDBMetricSubQuery("sum", "sys.cpu.0")
        expected = {'aggregator': 'sum', 'metric': 'sys.cpu.0', #'explicitTags': False
                   }

        self.assertEqual(expected,q.getMap())

class TestOpenTSDBQuery(TestCase):
    """test the OpenTSDBQuery class standalone"""

    def test_check(self):

        # these are valid queries
        mq = OpenTSDBMetricSubQuery("sum","sys.cpu.0",rate=True,filters=[OpenTSDBFilter("wildcard","host","*"),OpenTSDBFilter("literal_or","dc","lga")])
        tq = OpenTSDBtsuidSubQuery("sum",["000001000002000042","000001000002000043"])
        q = OpenTSDBQuery([mq,tq],start=1356998400,end=1356998460)
        q.check()
        q = OpenTSDBQuery([mq,tq],start=str(1356998400),end=1356998460)
        q = OpenTSDBQuery([mq,tq],start=1356998400,end=str(1356998460))

        # error cases
        q = OpenTSDBQuery(mq,self.getUniqueInteger())
        self.assertRaises(TypeError,q.check)
        q = OpenTSDBQuery([],self.getUniqueInteger())
        self.assertRaises(ValueError,q.check)
        q = OpenTSDBQuery([{"metric":"sys.cpu.0"},{"metric":"sys.cpu.1"}],start=1356998400,end=1356998460)
        self.assertRaises(TypeError,q.check)

    def test_map(self):
        mq = OpenTSDBMetricSubQuery("sum","sys.cpu.0",rate=True,filters=[OpenTSDBFilter("wildcard","host","*"),OpenTSDBFilter("literal_or","dc","lga")])
        tq = OpenTSDBtsuidSubQuery("sum",["000001000002000042","000001000002000043"])
        q = OpenTSDBQuery([mq,tq],start=1356998400,end=1356998460)

        q.getMap()
        expected={'start': 1356998400, 'end': 1356998460, 'showSummary': False, 
                  'queries': [{'aggregator': 'sum', 'metric': 'sys.cpu.0', 'rate': True, #'explicitTags': False,
                               'filters': [{'filter': '*', 'type': 'wildcard', 'groupBy': False, 'tagk': 'host'}, 
                                           {'filter': 'lga', 'type': 'literal_or', 'groupBy': False, 'tagk': 'dc'}], 'rateOptions': {'counter': True}}, 
                              {'tsuids': ['000001000002000042', '000001000002000043'], 'aggregator': 'sum'}], 
                  'noAnnotations': False, 'showQuery': False, 'delete': False, 'globalAnnotations': False, 'msResolution': False, 'showTSUIDs': False}
        self.assertEqual(expected,q.getMap())


class TestOpenTSDBExpQuery(TestCase):

    def test_check(self):
        #this class has many arguments... we will start by testing the helpers (static methods) and then will put everything together.

        # fillPolicy
        fp = OpenTSDBExpQuery.fillPolicy("nan")
        fp = OpenTSDBExpQuery.fillPolicy("null")
        fp = OpenTSDBExpQuery.fillPolicy("zero")
        fp = OpenTSDBExpQuery.fillPolicy("scalar",self.getUniqueInteger())
        fp = OpenTSDBExpQuery.fillPolicy("scalar",self.getUniqueInteger()+0.1)
        self.assertRaises(ValueError,OpenTSDBExpQuery.fillPolicy,self.getUniqueString())
        self.assertRaises(ValueError,OpenTSDBExpQuery.fillPolicy,"scalar")
        self.assertRaises(TypeError,OpenTSDBExpQuery.fillPolicy,"scalar",self.getUniqueString())

        # downsampler 
        ds = OpenTSDBExpQuery.downsampler("1d","avg")
        ds = OpenTSDBExpQuery.downsampler("1d","avg", fillPolicy=fp)
        self.assertRaises(TypeError,OpenTSDBExpQuery.downsampler,self.getUniqueInteger(),"avg", fp)
        self.assertRaises(TypeError,OpenTSDBExpQuery.downsampler,"1d",self.getUniqueInteger(),fp)
        self.assertRaises(TypeError,OpenTSDBExpQuery.downsampler,"1d","avg",self.getUniqueString())

        # timeSection
        ts = OpenTSDBExpQuery.timeSection("sum","1h-ago","2015/05/05-00:00:00",ds,True)
        ts = OpenTSDBExpQuery.timeSection("sum",10,11,ds,True)
        self.assertRaises(TypeError,OpenTSDBExpQuery.timeSection,"sum",[],"1h-ago")
        self.assertRaises(TypeError,OpenTSDBExpQuery.timeSection,"sum","1h-ago",[])
        self.assertRaises(TypeError,OpenTSDBExpQuery.timeSection,self.getUniqueInteger(),"1h-ago","2015/05/05-00:00:00")

        # filters do not require thorough test since filters are tested elsewhere. 
        filters = [OpenTSDBFilter("wildcard","host","*",True),OpenTSDBFilter("literal_or","dc","lga|lga1|lga2")]
        f = OpenTSDBExpQuery.filters("id", filters)
        self.assertRaises(TypeError,OpenTSDBExpQuery.filters,self.getUniqueInteger(),filters)
        self.assertRaises(ValueError,OpenTSDBExpQuery.filters,"id",[])
        self.assertRaises(TypeError,OpenTSDBExpQuery.filters,"id",[{},{}])

        # metrics
        m = OpenTSDBExpQuery.metric("cpunice","id","system.cpu.nice","count",fp)
        self.assertRaises(TypeError,OpenTSDBExpQuery.metric,self.getUniqueInteger(),"id","system.cpu.nice","count",fp)
        self.assertRaises(TypeError,OpenTSDBExpQuery.metric,"cpunice",self.getUniqueInteger(),"system.cpu.nice","count",fp)
        self.assertRaises(TypeError,OpenTSDBExpQuery.metric,"cpunice","id",self.getUniqueInteger(),"count",fp)
        self.assertRaises(TypeError,OpenTSDBExpQuery.metric,"cpunice","id","system.cpu.nice",self.getUniqueInteger(),fp)
        self.assertRaises(ValueError,OpenTSDBExpQuery.metric,"system.cpu.nice","id","system.cpu.nice","count",fp)

        # joins        
        j = OpenTSDBExpQuery.join("intersection", useQueryTags=True, includeAggTags=False)
        self.assertRaises(TypeError,OpenTSDBExpQuery.join,self.getUniqueInteger())
        self.assertRaises(TypeError,OpenTSDBExpQuery.join,"intersection",useQueryTags="")
        self.assertRaises(TypeError,OpenTSDBExpQuery.join,"intersection",includeAggTags="")

        # expressions
        e = OpenTSDBExpQuery.expression("cpubusy", "a + b / 1024", j, fp)
        self.assertRaises(TypeError,OpenTSDBExpQuery.expression,self.getUniqueInteger(), "a + b / 1024", j, fp)
        self.assertRaises(TypeError,OpenTSDBExpQuery.expression,"cpubusy",self.getUniqueInteger(),j, fp)
        self.assertRaises(TypeError,OpenTSDBExpQuery.expression,"cpubusy", "a + b / 1024",join=[])
        self.assertRaises(TypeError,OpenTSDBExpQuery.expression,"cpubusy", "a + b / 1024",fillPolicy=[])

        # outputs
        o = OpenTSDBExpQuery.output(self.getUniqueString())
        o = OpenTSDBExpQuery.output(self.getUniqueString(),self.getUniqueString())
        self.assertRaises(TypeError,OpenTSDBExpQuery.output,self.getUniqueInteger())
        self.assertRaises(TypeError,OpenTSDBExpQuery.output,"id",self.getUniqueInteger())

        # put it all together
        q = OpenTSDBExpQuery(ts, [f], [m,m], [e,e], [o])
        q.check()

        # mimics the example in the doc.

	ts = OpenTSDBExpQuery.timeSection("sum","1y-ago")
	f = OpenTSDBExpQuery.filters("f1",[OpenTSDBFilter("wildcard","host","web*",True)])
	m1 = OpenTSDBExpQuery.metric("a","f1","sys.cpu.user",fillPolicy=OpenTSDBExpQuery.fillPolicy("nan"))
	m2 = OpenTSDBExpQuery.metric("b","f1","sys.cpu.iowait",fillPolicy=OpenTSDBExpQuery.fillPolicy("nan"))
	e1 = OpenTSDBExpQuery.expression("e","a + b")
	e2 = OpenTSDBExpQuery.expression("e2","e * 2")
	e3 = OpenTSDBExpQuery.expression("e3","e2 * 2")
	e4 = OpenTSDBExpQuery.expression("e4","e3 * 2")
	e5 = OpenTSDBExpQuery.expression("e5","e4 + e2")
	o1 = OpenTSDBExpQuery.output("e5","Mega expression")
	o2 = OpenTSDBExpQuery.output("a","CPU User")

	q = OpenTSDBExpQuery(ts,[f],[m1,m2],[e1,e2,e3,e4,e5],[o1,o2])
	q.check()

    def test_map(self):
        # here we test one complete example. Valid (programatically) but nonsense (would fail submission)
        fp = OpenTSDBExpQuery.fillPolicy("scalar",1)
        ds = OpenTSDBExpQuery.downsampler("1d","avg", fillPolicy=fp)
        ts = OpenTSDBExpQuery.timeSection("sum","1h-ago","2015/05/05-00:00:00",ds,True)
        filters = [OpenTSDBFilter("wildcard","host","*",True),OpenTSDBFilter("literal_or","dc","lga|lga1|lga2")]
        f = OpenTSDBExpQuery.filters("id", filters)
        m = OpenTSDBExpQuery.metric("cpunice","id","system.cpu.nice","count",fp)
        j = OpenTSDBExpQuery.join("intersection", useQueryTags=True, includeAggTags=False)
        e = OpenTSDBExpQuery.expression("cpubusy", "a + b / 1024", j, fp)
        o = OpenTSDBExpQuery.output("e5","Mega expression")
        q = OpenTSDBExpQuery(ts, [f], [m,m], [e,e], [o])


        expected = {'metrics': [{'filter': 'id', 'aggregator': 'count', 'metric': 'system.cpu.nice', 
                                 'fillPolicy': {'policy': 'scalar', 'value': 1}, 'id': 'cpunice'}, 
                                {'filter': 'id', 'aggregator': 'count', 'metric': 'system.cpu.nice', 
                                 'fillPolicy': {'policy': 'scalar', 'value': 1}, 'id': 'cpunice'}], 
                    'outputs': [{'alias': 'Mega expression', 'id': 'e5'}], 
                    'expressions': [{'expr': 'a + b / 1024', 'fillPolicy': {'policy': 'scalar', 'value': 1}, 
                                     'join': {'operator': 'intersection', 'useQueryTags': True, 'includeAggTags': False}, 'id': 'cpubusy'}, 
                                    {'expr': 'a + b / 1024', 'fillPolicy': {'policy': 'scalar', 'value': 1}, 
                                     'join': {'operator': 'intersection', 'useQueryTags': True, 'includeAggTags': False}, 'id': 'cpubusy'}], 
                    'filters': [{'id': 'id', 'tags': [{'filter': '*', 'type': 'wildcard', 'groupBy': True, 'tagk': 'host'}, 
                                                      {'filter': 'lga|lga1|lga2', 'type': 'literal_or', 'groupBy': False, 'tagk': 'dc'}]}], 
                    'time': {'start': "1h-ago", 'rate': True, 'downsampler': {'aggregator': 'avg', 'interval': '1d', 'fillPolicy': {'policy': 'scalar', 'value': 1}}, 
                             'end': "2015/05/05-00:00:00", 'aggregator': 'sum'}
                    }

        self.assertEqual(expected,q.getMap())

        # mimics the example in the doc.

	ts = OpenTSDBExpQuery.timeSection("sum","1y-ago")
	f = OpenTSDBExpQuery.filters("f1",[OpenTSDBFilter("wildcard","host","web*",True)])
	m1 = OpenTSDBExpQuery.metric("a","f1","sys.cpu.user",fillPolicy=OpenTSDBExpQuery.fillPolicy("nan"))
	m2 = OpenTSDBExpQuery.metric("b","f1","sys.cpu.iowait",fillPolicy=OpenTSDBExpQuery.fillPolicy("nan"))
	e1 = OpenTSDBExpQuery.expression("e","a + b")
	e2 = OpenTSDBExpQuery.expression("e2","e * 2")
	e3 = OpenTSDBExpQuery.expression("e3","e2 * 2")
	e4 = OpenTSDBExpQuery.expression("e4","e3 * 2")
	e5 = OpenTSDBExpQuery.expression("e5","e4 + e2")
	o1 = OpenTSDBExpQuery.output("e5","Mega expression")
	o2 = OpenTSDBExpQuery.output("a","CPU User")

	q = OpenTSDBExpQuery(ts,[f],[m1,m2],[e1,e2,e3,e4,e5],[o1,o2])
	q.check()

        expected = {
                    "time": {
                        "start": "1y-ago",
                        "aggregator":"sum",
			'rate': False
                    },
                    "filters": [
                        {
                            "tags": [
                                {
                                    "type": "wildcard",
                                    "tagk": "host",
                                    "filter": "web*",
                                    "groupBy": True
                                }
                            ],
                            "id": "f1"
                        }
                    ],
                    "metrics": [
                        {
                            "id": "a",
                            "metric": "sys.cpu.user",
                            "filter": "f1",
                            "fillPolicy":{"policy":"nan"}
                        },
                        {
                            "id": "b",
                            "metric": "sys.cpu.iowait",
                            "filter": "f1",
                            "fillPolicy":{"policy":"nan"}
                        }
                    ],
                    "expressions": [
                        {
                            "id": "e",
                            "expr": "a + b"
                        },
                        {
                          "id":"e2",
                          "expr": "e * 2"
                        },
                        {
                          "id":"e3",
                          "expr": "e2 * 2"
                        },
                        {
                          "id":"e4",
                          "expr": "e3 * 2"
                        },
                        {
                          "id":"e5",
                          "expr": "e4 + e2"
                        }
                     ],
                     "outputs":[
                       {"id":"e5", "alias":"Mega expression"},
                       {"id":"a", "alias":"CPU User"}
                     ]
                  }

        self.assertEqual(expected,q.getMap())

class TestOpenTSDBQueryLast(TestCase):

    def test_check(self):
        q = OpenTSDBQueryLast([OpenTSDBQueryLast.metric("sys.cpu.user",{"host":"web01", "dc": "lga"})], ["000001000002000042","000001000002000043"],True,24)
        q.check()

        testcases = [OpenTSDBQueryLast(),
                     OpenTSDBQueryLast([]),
                     OpenTSDBQueryLast(None,[]),
                     OpenTSDBQueryLast([],[]),
                     OpenTSDBQueryLast([OpenTSDBQueryLast.metric("sys.cpu.user",{"host":"web01", "dc": "lga"})], ["000001000002000042","000001000002000043"],True,-10) ]

        for q in testcases:
            self.assertRaises(ValueError,q.check)

        testcases = [OpenTSDBQueryLast([OpenTSDBQueryLast.metric("sys.cpu.user",{"host":"web01", "dc": "lga"})], ["000001000002000042","000001000002000043"],True,24.2),
                     OpenTSDBQueryLast([OpenTSDBQueryLast.metric("sys.cpu.user",{"host":"web01", "dc": "lga"})], ["000001000002000042","000001000002000043"],"",24),
                     OpenTSDBQueryLast([OpenTSDBQueryLast.metric("sys.cpu.user",{"host":"web01", "dc": "lga"})], "000001000002000043",True,24),
                     OpenTSDBQueryLast(OpenTSDBQueryLast.metric("sys.cpu.user",{"host":"web01", "dc": "lga"}), ["000001000002000042","000001000002000043"],True,24)]

        for q in testcases:
            self.assertRaises(TypeError,q.check)

    def test_map(self):
        q = OpenTSDBQueryLast([OpenTSDBQueryLast.metric("sys.cpu.user",{"host":"web01", "dc": "lga"})], ["000001000002000042","000001000002000043"],True,24)
        expected = {'resolveNames': True, 'backScan': 24, 'queries': [{'metric': 'sys.cpu.user', 'tags': {'host': 'web01', 'dc': 'lga'}}, {'tsuids': ['000001000002000042', '000001000002000043']}]}
        self.assertEqual(expected,q.getMap())


