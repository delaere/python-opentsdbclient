from opentsdbobjects import OpenTSDBTimeSeries
import string

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
        if self.end is not None:
            myself["end"] = self.end
        return myself

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
        if len(self.subqueries)<1:
            raise ValueError("Requires at least one subquery")
        for q in self.subqueries:
            if not isinstance(q,OpenTSDBMetricSubQuery) and not isinstance(q,OpenTSDBtsuidSubQuery):
                raise TypeError("Subqueries must be either OpenTSDBMetricSubQuery or OpenTSDBtsuidSubQuery")
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
            not isinstance(self.rate, bool) or 
            (self.counterMax is not None and not isinstance(self.counterMax,int)) or
            (self.resetValue is not None and not isinstance(self.resetValue,int)) or
            (self.downsample is not None and not isinstance(self.downsample,basestring)) or
            not isinstance(self.explicitTags,bool)):
               raise TypeError("OpenTSDBMetricSubQuery type mismatch")
        if self.filters is not None:
            for f in self.filters:
                if isinstance(f,OpenTSDBFilter):
                    f.check()
                else:
                    raise TypeError("OpenTSDBMetricSubQuery type mismatch")
        if not OpenTSDBTimeSeries.checkString(self.metric):
            raise ValueError("Invalid metric name")


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
        if len(self.tsuids)<1:
            raise ValueError("OpenTSDBtsuidSubQuery tsuid list cannot be empty")
        for i in self.tsuids:
            if not isinstance(i,basestring):
               raise TypeError("OpenTSDBtsuidSubQuery type mismatch")
            int(i,16)



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

#TODO convert to a static method as for other fields of OpenTSDBExpQuery
class OpenTSDBFilterSet:
    """ A list of OpenTSDBFilters associated to an id, for the expression query."""

    def __init__(self, theId, filters):
        self.theId = theId
        self.filters = filters

    def getMap(self):
        return {"id": self.theId,
                "tags": map(lambda f: f.getMap(), self.filters)}

    def check(self):
        if (not isinstance(self.theId,basestring)):
            raise TypeError("OpenTSDBFilterSet arg type mismatch")
        if len(self.filters)<1:
            raise ValueError("There must be at least one OpenTSDBFilter in a OpenTSDBFilterSet.")
        for f in self.filters:
            if not isinstance(f,OpenTSDBFilter):
                raise TypeError("OpenTSDBFilterSet arg type mismatch")
            f.check()

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

    def __init__(self, timeSection, filters, metrics, expressions, outputs=None):
        self.timeSection = timeSection # from the static method below
        self.filters = filters # list of OpenTSDBFilterSet
        self.metrics = metrics # a list of metrics form the method below
        self.expressions = expressions# a list of expressions from the method below
        self.outputs = outputs
        
    def getMap(self):
        myself = { "time": self.timeSection,
                   "filters": map(lambda f:f.getMap(),self.filters),
                   "metrics": self.metrics,
                   "expressions": self.expressions
                   }
        if self.outputs is not None: 
            myself["outputs"] = self.outputs
        return myself

    def check(self):
        if (not isinstance(self.timeSection,dict) or
            not isinstance(self.filters,list) or
            not isinstance(self.metrics,list) or
            not isinstance(self.expressions,list)):
            raise TypeError("OpenTSDBExpQuery arg type mismatch.")
        if len(self.filters)<1:
            raise ValueError("At least one filter must be specified.")
        for f in self.filters:
            if not isinstance(f,OpenTSDBFilterSet):
                raise TypeError("OpenTSDBExpQuery filter type mismatch.")
            else:
                f.check()
        if len(self.metrics)<1:
            raise ValueError("There must be at least one metric.")
        for m in self.metrics:
            if not isinstance(m,dict):
                raise TypeError("OpenTSDBExpQuery metric type mismatch.")
        if len(self.expressions)<1:
            raise ValueError("At least one expression must be specified.")
        for e in self.expressions:
            if not isinstance(e,dict):
                raise TypeError("OpenTSDBExpQuery expression type mismatch.")
        if self.outputs is not None:
            for o in self.outputs:
                if not isinstance(o,dict):
                    raise TypeError("OpenTSDBExpQuery output  type mismatch.")

    # Methods below are there to help in the constructor.
    # They contains additional checks.

    @staticmethod
    def timeSection(aggregator, start, end=None, downsampler=None, rate=False):
        """The time section is required. It affects the time range and optional reductions for all metrics requested."""
        #TODO int and strings are both valid for start and end. Will have to test with the db since the doc is self-contradictory.
        if not isinstance(start,(int,basestring)) or not isinstance(aggregator, basestring) or not isinstance(rate, bool):
            raise TypeError("timeSection args type mismatch.")
        timesection = { "start": start, 
                        "aggregator": aggregator,
                        "rate": rate}
        if end is not None: 
            if not isinstance(end,(int,basestring)):
                raise TypeError("end must be integer or string")
            timesection["end"]=end
        if downsampler is not None: 
            if not isinstance(downsampler, dict):
                raise TypeError("DownSampler should be a dict")
            timesection["downsampler"]=downsampler
        
        return timesection

    @staticmethod
    def downsampler(interval, aggregator, fillPolicy=None):
        """Reduces the number of data points returned. Part of the time section."""
        downSampler = { "interval": interval,
                        "aggregator": aggregator}
        if not isinstance(interval,basestring) or not isinstance(aggregator,basestring):
            raise TypeError("downsampler args type mismatch.")
        if fillPolicy is not None:
            if not isinstance(fillPolicy, dict):
                raise TypeError("Fill Policy should be a dict")
            downSampler["fillPolicy"]=fillPolicy
        return downSampler

    @staticmethod
    def fillPolicy(policy, value=None):
        """These are used to replace "missing" values, i.e. when a data point was expected but couldn't be found in storage."""
        if policy not in ["nan", "null", "zero", "scalar"]:
            raise ValueError("Unkonwn policy. Must be one of nan,null,zero,scalar.")
        if policy is "scalar":
            if value is None:
                raise ValueError("Must specify a value for scalar fill policy")
            elif not isinstance(value,(int,float)):
                raise TypeError("Scalar fill policy value must be an integer or floating point.")
            return {"policy":"scalar", "value": value}
        return {"policy":policy}

    @staticmethod
    def metric(metricId, filterId, metricName, aggregator=None, fillPolicy=None):
        """The metrics list determines which metrics are included in the expression. 
           There must be at least one metric."""
        if not isinstance(metricId,basestring) or not isinstance(filterId,basestring) or not isinstance(metricName,basestring):
            raise TypeError("metric args type mismatch.")
        if any(char not in string.ascii_letters+string.digits for char in metricId):
            raise ValueError("unique ID for the metric MUST be a simple string, no punctuation or spaces")
        theMetric = {"id": metricId, "filter": filterId, "metric": metricName}
        if aggregator is not None:
            if not isinstance(aggregator,basestring):
                raise TypeError("metric args type mismatch.")
            theMetric["aggregator"] = aggregator
        if fillPolicy is not None:
            if not isinstance(fillPolicy,dict):
                raise TypeError("Fill Policy should be a dict")
            theMetric["fillPolicy"] = fillPolicy
        return theMetric

    @staticmethod
    def expression(exprId, expr, join=None, fillPolicy=None):
        """The variables in an expression MUST refer to either a metric ID field or an expression ID field. 
           Nested expressions are supported but exceptions will be thrown if a self reference 
           or circular dependency is detected. 
           So far only basic operations are supported such as addition, subtraction, multiplication, division, modulo"""

        theExpr = { "id": exprId, "expr": expr }
        if not isinstance(exprId,basestring) or not isinstance(expr,basestring):
            raise TypeError("expression args type mismatch.")
        if join is not None:
            if not isinstance(join, dict):
                raise TypeError("join should be a dict")
            theExpr["join"] = join
        if fillPolicy is not None:
            if not isinstance(fillPolicy,dict):
                raise TypeError("Fill Policy should be a dict")
            theExpr["fillPolicy"] = fillPolicy
        return theExpr

    @staticmethod
    def join(operator, useQueryTags=False, includeAggTags=True):
        """The join object controls how the various time series for a given metric are merged within an expression. 
           The two basic operations supported at this time are the union and intersection operators. 
           Additional flags control join behavior."""
        if not isinstance(operator,basestring) or not isinstance(useQueryTags,bool) or not isinstance(includeAggTags,bool):
            raise TypeError("join args type mismatch.")
        return {"operator": operator, 
                "useQueryTags": useQueryTags,
                "includeAggTags": includeAggTags}

    @staticmethod
    def output(outputId, alias=None):
        """These determine the output behavior and allow you to eliminate some expressions 
           from the results or include the raw metrics. 
           By default, if this section is missing, all expressions and only the expressions will be serialized. 
           The field is a list of one or more output objects. 
           More fields will be added later with flags to affect the output."""
        if not isinstance(outputId,basestring) or (alias is not None and not isinstance(alias,basestring)):
            raise TypeError("output args type mismatch.")
        if alias is None:
            return {"id": outputId }
        else:
            return {"id": outputId, "alias": alias}

class OpenTSDBQueryLast:
    """Provides support for accessing the latest value of individual time series. 
       It provides an optimization over a regular query when only the last data point is required. 
       Locating the last point can be done with the timestamp of the meta data counter 
       or by scanning backwards from the current system time."""

    def __init__(self, metrics=None, tsuids=None, resolveNames=False, backScan=0):
        self.resolveNames = resolveNames
        self.backScan = backScan
        self.metrics = metrics
        self.tsuids = tsuids

    def getMap(self):
        queries = []

        if self.metrics is not None:
            for m in self.metrics:
               queries.append(m)
        if self.tsuids is not None:
            queries.append({"tsuids":self.tsuids})

        return { "queries": queries, "resolveNames": self.resolveNames, "backScan": self.backScan }

    def check(self):
        if ((self.metrics is None and self.tsuids is None) or
            (self.metrics is None and len(self.tsuids)==0) or
            (self.tsuids is None and len(self.metrics)==0) or
            self.metrics is not None and self.tsuids is not None and len(self.metrics)==0 and len(self.tsuids)==0):
            raise ValueError("OpenTSDBQueryLast requires at least one metric or one tsuid.")

        if not isinstance(self.metrics, list) or not isinstance(self.tsuids,list) or not isinstance(self.resolveNames,bool) or not isinstance(self.backScan,int):
            raise TypeError("OpenTSDBQueryLast arg type mismatch.")

        if self.backScan<0:
            raise ValueError("backScan must be a positive integer")

    @staticmethod
    def metric(metric, tags):
        if not isinstance(metric,basestring) or not isinstance(tags,dict):
            raise TypeError("metric args type mismatch.")
        return { "metric":metric, "tags":tags }

