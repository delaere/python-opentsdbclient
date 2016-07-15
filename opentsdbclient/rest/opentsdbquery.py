
class OpenTSDBQuery:
    """Enables extracting data from the storage system in various formats determined by the serializer selected.
       An OpenTSDB query requires at least one sub query, a means of selecting which time series should be included in the result set. 
       
       There are two types:
           * Metric Query - The full name of a metric is supplied along with an optional list of tags. 
             This is optimized for aggregating multiple time series into one result.
           * TSUID Query - A list of one or more TSUIDs that share a common metric. 
             This is optimized for fetching individual time series where aggregation is not required.

       A query can include more than one sub query and any mixture of the two types. 
       Each sub query can retrieve individual or groups of timeseries data, 
       performing aggregation or grouping calculations on each set.
       
       See http://opentsdb.net/docs/build/html/api_http/query/index.html"""

    def __init__(self, subqueries, start, end=None, 
                 noAnnotations=False, globalAnnotations=False, msResolution=False, 
                 showTSUIDs=False, showSummary=False, showQuery=False, delete=False):
        self.subqueries = subqueries
        self.start = start
        self.end = end
        self.noAnnotations = noAnnotations
        self.globalAnnotations = globalAnnotations
        self.msResolution = msResolution
        self.showTSUIDs = showTSUIDs
        self.showSummary = showSummary
        self.showQuery = showQuery
        self.delete = delete

    def getMap(self):
        myself = { "start": self.start,
                   "queries": map(lambda q:q.getMap(),self.subqueries),
                   "noAnnotations": self.noAnnotations,
                   "globalAnnotations": self.globalAnnotations,
                   "msResolution": self.msResolution,
                   "showTSUIDs": self.showTSUIDs,
                   "showSummary": self.showSummary,
                   "showQuery": self.showQuery,
                   "delete": self.delete
                }
        if end is not None:
            myself["end"] = self.end

    def check(self):
        if (not isinstance(self.subqueries,list) or
            (not isinstance(self.start,int) and not isinstance(self.start,basestring)) or
            (self.end is not None and not isinstance(self.start,int) and not isinstance(self.start,basestring)) or
            not isinstance(self.noAnnotations,bool) or
            not isinstance(self.globalAnnotations,bool) or
            not isinstance(self.msResolution,bool) or
            not isinstance(self.showTSUIDs,bool) or
            not isinstance(self.showQuery,bool) or
            not isinstance(self.delete,bool)):
            raise TypeError("OpenTSDBQuery type mismatch")
        for q in self.subqueries:
            if not isinstance(q,OpenTSDBMetricSubQuery) and not isinstance(q,OpenTSDBtsuidSubQuery):
                raise TypeError("OpenTSDBQuery type mismatch")
            else:
                q.check()


class OpenTSDBMetricSubQuery:
    """ Metric Query - The full name of a metric is supplied along with an optional list of tags. 
        This is optimized for aggregating multiple time series into one result."""

    def __init__(self, aggregator, metric, rate=False, counterMax=None, resetValue=None, downsample=None, filters=None, explicitTags=False):
        self.aggregator = aggregator
        self.metric = metric
        self.rate = rate
        self.counterMax = counterMax
        self.resetValue = resetValue
        self.downsample = downsample
        self.filters = filters
        self.explicitTags = explicitTags

    def getMap(self):
        myself = { "aggregator": self.aggregator,
                   "metric": self.metric,
                   "explicitTags": self.explicitTags
                 }
        if self.rate:
            myself["rate"] = self.rate
            rateOptions = { "counter": True }
            if self.counterMax is not None: rateOptions["counterMax"] = self.counterMax
            if self.resetValue is not None: rateOptions["resetValue"] = self.resetValue
            myself["rateOptions"] = rateOptions
        if self.filters is not None:
            myself["filters"] = map(lambda f:f.getMap(),self.filters)
        return myself
    
    def check(self):
        if (not isinstance(self.aggregator,basestring) or 
            not isinstance(self.metric,basestring) or 
            not isinstance(self.rate, bool) or 
            (self.counterMax is not None and not isinstance(self.counterMax,int)) or
            (self.resetValue is not None and not isinstance(self.resetValue,int)) or
            (self.downsample is not None and not isinstance(self.downsample,basestring)) or
            not isinstance(self.explicitTags,bool)):
               raise TypeError("OpenTSDBMetricSubQuery type mismatch")
        if self.filters is not None:
            for f in self.filters:
                if isinstance(i,OpenTSDBFilter):
                    f.check()
                else:
                    raise TypeError("OpenTSDBMetricSubQuery type mismatch")


class OpenTSDBtsuidSubQuery:
    """TSUID Query - A list of one or more TSUIDs that share a common metric. 
       This is optimized for fetching individual time series where aggregation is not required."""

    def __init__(self, aggregator, tsuids):
        self.aggregator = aggregator
        self.tsuids = tsuids

    def getMap(self):
        return { "aggregator": self.aggregator, "tsuids": self.tsuids }

    def check(self):
        if (not isinstance(self.aggregator,basestring) or
            not (isinstance(self.tsuids, list) and not isinstance(self.tsuids, basestring))):
                raise TypeError("OpenTSDBtsuidSubQuery type mismatch")
        for i in self.tsuids:
            if not isinstance(i,basestring):
               raise TypeError("OpenTSDBtsuidSubQuery type mismatch")


class OpenTSDBFilter:
    """ OpenTSDB includes expanded and plugable filters across tag key and value combinations. 
        Multiple filters on the same tag key are allowed and when processed, they are ANDed together 
        e.g. if we have two filters host=literal_or(web01) and host=literal_or(web02) the query will always return empty. 
        If two or more filters are included for the same tag key and one has group by enabled 
        but another does not, then group by will effectively be true for all filters on that tag key."""

    def __init__(self, filterType, tagKey, filterExpression, groupBy=False):
        self.filterType = filterType
        self.tagKey = tagKey
        self.filterExpression = filterExpression
        self.groupBy = groupBy

    def getMap(self):
        return {"type": self.filterType, 
                "tagk": self.tagKey,
                "filter": self.filterExpression,
                "groupBy": self.groupBy}

    def check(self):
        if (not isinstance(self.filterType,basestring) or
            not isinstance(self.tagKey,basestring) or
            not isinstance(self.filterExpression,basestring) or
            not isinstance(self.groupBy,bool)):
               raise TypeError("OpenTSDBFilter type mismatch")


class OpenTSDBExpQuery:
    """Allows for querying data using expressions. The query is broken up into different sections.
    Two set operations (or Joins) are allowed. The union of all time series or the intersection.
    
    For example we can compute "a + b" with a group by on the host field. 
    Both metrics queried alone would emit a time series per host, e.g. maybe one for "web01", "web02" and "web03". 
    Lets say metric "a" has values for all 3 hosts but metric "b" is missing "web03".

    With the intersection operator, the expression will effectively add "a.web01 + b.web01" and 
    "a.web02 + b.web02" but will skip emitting anything for "web03". 
    Be aware of this if you see fewer outputs that you expected or you see errors about no series available after intersection.

    With the union operator the expression will add the web01 and web02 series but for metric "b", 
    it will substitute the metric's fill policy value for the results.

    See http://opentsdb.net/docs/build/html/api_http/query/exp.html
    """

    #TODO implement the methods
    def __init__(self, timeSection, downsampler, fillPolicies, filters, metrics, expressions, joins, outputs):
        pass

    def getMap(self):
        pass

    def check(self):
        pass

    # methods below are there to help in the constructor.

    @staticmethod
    def timeSection(aggregator, start, end=None, downsampler=None, rate=False):
        return {}

    @staticmethod
    def downsampler(interval, aggregator, fillPolicy=None):
        return {}

    @staticmethod
    def fillPolicy(policy, value=None):
        return {}

    @staticmethod
    def metric(metricId, filterId, metricName, aggregator=None, fillPolicy=None):
        return {}

    @staticmethod
    def expression(exprId, expr, join=None, fillPolicy=None):
        return {}

    @staticmethod
    def join(operator, useQueryTags=False, includeAggTags=True):
        return {}

    @staticmethod
    def output(outputId, alias=None):
        return {}
