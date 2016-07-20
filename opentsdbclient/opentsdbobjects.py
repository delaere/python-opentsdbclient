import json
import string

class OpenTSDBAnnotation:
    def __init__(self,startTime, endTime=None, tsuid=None, description=None, notes=None, custom=None):
        self.startTime = startTime
        self.endTime = endTime
        self.tsuid = tsuid
        self.description = description
        self.notes = notes
        self.custom = custom
        if not self.check():
            raise ValueError("Invalid OpenTSDBAnnotation: \n%s"%str(self))

    def check(self):
        if not isinstance(self.startTime,int) or self.startTime<0: return False
        if not (self.endTime is None or (isinstance(self.endTime,int) and self.endTime>=self.startTime)): return False
        if not self.tsuid is None:
            if not isinstance(self.tsuid,basestring) : return False
            try:
                int(self.tsuid,16)
            except:
                return False
        if not (self.description is None or isinstance(self.description,basestring)): return False
        if not (self.notes is None or isinstance(self.notes,basestring)): return False
        if not self.custom is None:
            for k,v in self.custom.iteritems():
                if not isinstance(k,basestring) and isinstance(v,basestring): return False
        return True

    def getMap(self):
        myself = self.__dict__
        return { k:v for k,v in myself.iteritems() if v is not None  }

    def json(self):
        return json.dumps(self.getMap())

    def __str__(self):
        return self.getMap().__str__()


class OpenTSDBTimeSeries:
    """A time series is made of a metric and a set of (at least one) tags."""
    #basically a metric + tags
    def __init__(self, metric, tags):
        self.metric = metric
        self.tags= tags
        if not self.check():
            raise ValueError("Invalid OpenTSDBTimeSeries: \n%s"%str(self))

    def check(self):
        if not OpenTSDBTimeSeries.checkString(self.metric): 
            return False
        for t,v in self.tags.iteritems():
            if not (OpenTSDBTimeSeries.checkString(t) and OpenTSDBTimeSeries.checkString(v)):
                return False
        if len(self.tags)<1 : return False
        return True

    @staticmethod
    def checkString(thestring):
       """The following rules apply to metric and tag values:
          - Strings are case sensitive, i.e. "Sys.Cpu.User" will be stored separately from "sys.cpu.user"
          - Spaces are not allowed
          - Only the following characters are allowed: a to z, A to Z, 0 to 9, -, _, ., / or Unicode letters (as per the specification)"""
       asciichars = string.ascii_letters + "0123456789-_./"
       for c in thestring:
           if not c in asciichars and not ud.category(c) in ['Ll', 'Lu']:
               return False
       return True

    def getMap(self):
        return self.__dict__

    def json(self):
        return json.dumps(self.getMap())

    def __str__(self):
        return self.getMap().__str__()


class OpenTSDBMeasurement:
    """A measurement is made of a Timeseries + timestamp and value."""
    def __init__(self,timeseries, timestamp, value):
        self.ts = timeseries
        self.timestamp = timestamp
        if isinstance(value,basestring):
            if '.' in self.value:
                self.value = float(value)
            else:
                self.value = int(value)
        else:
            self.value = value
        if not self.check():
            raise ValueError("Invalid OpenTSDBMeasurement: \n%s"%str(self))
        
    def check(self):
        # timeseries must be valid
        if not self.ts.check(): return False
        #  Timestamps must be integers and be no longer than 13 digits
        if not (isinstance(self.timestamp,int) and self.timestamp>=0 and len(str(self.timestamp))<13) : return False
        # Data point can have a minimum value of -9,223,372,036,854,775,808 and a maximum value of 9,223,372,036,854,775,807 (inclusive)
        # Floats are also valid and stored on 32 bits (IEEE 754 floating-point "single format" with positive and negative value support)
        # on most platforms, this means int or float, excluding long. But this would not be portable.
        if isinstance(self.value,int):
            if self.value < -9223372036854775808 or self.value > 9223372036854775807: return False
        elif not isinstance(self.value,(float,basestring)) : return False

        return True

    def getMap(self):
        myself = self.ts.getMap()
        myself["timestamp"] = self.timestamp
        myself["value"] = self.value
        return myself

    def json(self):
        return json.dumps(self.getMap())

    def __str__(self):
        return self.getMap().__str__()


class OpenTSDBTreeDefinition:
    def __init__(self,name=None, description=None, notes=None, rules=None, created=None, treeId=None, strictMatch=False, storeFailures=False, enabled=False):
        self.name = name
        self.description = description
        self.notes = notes
        self.rules = {}
        for level,orders in rules.iteritems():
            ordersdict = {}
            for order,therule in orders.iteritems():
                ordersdict[int(order)] = OpenTSDBRule(**therule)
            self.rules[int(level)] = ordersdict
        self.created = created
        self.treeId = treeId
        self.strictMatch = strictMatch
        self.storeFailures = storeFailures
        self.enabled = enabled
        if not self.check():
            raise ValueError("Invalid OpenTSDBTreeDefinition: \n%s"%str(self))

    def check(self):
        # not all the fields are mandatory.
        # we must have at least name (at creation time) or treeId (later on).
        if self.created is not None and self.treeId is None: return False
        if self.created is None and self.name is None: return False
        if not (self.name is None or isinstance(self.name,basestring)): return False
        if not (self.description is None or isinstance(self.description,basestring)): return False
        if not (self.notes is None or isinstance(self.note,basestring)): return False
        if not (self.created is None or isinstance(self.created,int)): return False
        if not (self.treeId is None or isinstance(self.treeId,int)): return False
        if not isinstance(self.strictMatch,bool) : return False
        if not isinstance(self.storeFailures,bool) : return False
        if not isinstance(self.enabled,bool) : return False
        return True

    def rule(self, level, order):
        return self.rules[level][order]

    def getMap(self):
        myself = self.__dict__
        for level,orders in self.rules.iteritems():
            for order,therule in orders.iteritems():
                orders[order]=therule.getMap()
        return { k:v for k,v in myself.iteritems() if v is not None  }

    def json(self):
        return json.dumps(self.getMap())

    def __str__(self):
        return self.getMap().__str__()

class OpenTSDBRule:

    def __init__(self, treeId, level=0, order=0, ruleType=None, description=None, notes=None, field=None, customField=None, regex=None, separator=None, regexGroupIdx=0, displayFormat=None):
        self.treeId = treeId
        self.level = level
        self.order = order
        self.ruleType = ruleType
        self.description = description
        self.notes = notes
        self.field = field
        self.customField = customField
        self.regex = regex
        self.separator = separator
        self.regexGroupIdx = regexGroupIdx
        self.displayFormat = displayFormat
        if not self.check():
            raise ValueError("Invalid OpenTSDBRule: \n%s"%str(self))

    def check(self):
        if not (isinstance(self.treeId,int) and self.treeId>=0): return False
        if not (isinstance(self.level,int) and self.level >=0): return False
        if not (isinstance(self.order,int) and self.order >=0): return False
        if not (isinstance(self.regexGroupIdx,int) and self.regexGroupIdx>=0): return False
        if not self.ruleType is None or not self.ruleType in ["METRIC","METRIC_CUSTOM","TAGK","TAGK_CUSTOM","TAGV_CUSTOM"] : return False
        if not self.description is None or isinstance(self.description,basestring) : return False
        if not self.notes is None or isinstance(self.notes,basestring) : return False
        if not self.field is None or isinstance(self.field,basestring) : return False
        if not self.customField is None or isinstance(self.customField,basestring) : return False
        if not self.regex is None or isinstance(self.regex,basestring) : return False
        if not self.separator is None or isinstance(self.separator,basestring) : return False
        if not self.displayFormat is None or isinstance(self.displayFormat,basestring) : return False
        return True

    def getMap(self):
        myself = self.__dict__
        return { k:v for k,v in myself.iteritems() if v is not None  }

    def json(self):
        return json.dumps(self.getMap())

    def __str__(self):
        return self.getMap().__str__()

#TODO
#these objects are for more advanced use.
#not really needed by the client, but would ease the navigation.
#I think of instantiating the tree from the treeId + client, and let it do the discovery itelf.
#It would somehow be the top-level object.

class OpenTSDBTree:
    pass

class OpenTSDBTreeBranch:
    pass