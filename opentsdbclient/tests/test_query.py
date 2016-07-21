from testtools import TestCase
from opentsdbquery import OpenTSDBQuery, OpenTSDBMetricSubQuery, OpenTSDBtsuidSubQuery, OpenTSDBFilter, OpenTSDBFilterSet, OpenTSDBExpQuery, OpenTSDBQueryLast

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
        expected = {'aggregator': 'sum', 'metric': 'sys.cpu.0', 'explicitTags': False, 'rate': True, 
                    'filters': [{'filter': '*', 'type': 'wildcard', 'groupBy': True, 'tagk': 'host', "groupBy":True}, 
                                {'filter': 'lga|lga1|lga2', 'type': 'literal_or', 'groupBy': False, 'tagk': 'dc'}], 
                    'rateOptions': {'counter': True, 'counterMax': 100, 'resetValue': 1000}}

        self.assertEqual(expected,q.getMap())
        
        # without rate

        filters = [OpenTSDBFilter("wildcard","host","*",True),OpenTSDBFilter("literal_or","dc","lga|lga1|lga2")]
        q = OpenTSDBMetricSubQuery("sum", "sys.cpu.0", rate=False, filters=filters, counterMax=100, resetValue=1000, downsample="30m-avg-nan")
        expected = {'aggregator': 'sum', 'metric': 'sys.cpu.0', 'explicitTags': False, 
                    'filters': [{'filter': '*', 'type': 'wildcard', 'groupBy': True, 'tagk': 'host', "groupBy":True}, 
                                {'filter': 'lga|lga1|lga2', 'type': 'literal_or', 'groupBy': False, 'tagk': 'dc'}]}
        
        self.assertEqual(expected,q.getMap())

        # minimal

        q = OpenTSDBMetricSubQuery("sum", "sys.cpu.0")
        expected = {'aggregator': 'sum', 'metric': 'sys.cpu.0', 'explicitTags': False}

        self.assertEqual(expected,q.getMap())


class TestOpenTSDBQuery(TestCase):
    """test the OpenTSDBQuery class standalone"""

    # basically construct one complex query, run check, run getMap 

    pass

