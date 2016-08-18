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

import copy
import json
import string
import unicodedata as ud
from opentsdberrors import OpenTSDBError

class OpenTSDBAnnotation:
    def __init__(self,startTime, endTime=None, tsuid=None, description=None, notes=None, custom=None):
        self.startTime = startTime
        self.endTime = endTime
        if self.endTime is 0: self.endTime = None
        self.tsuid = tsuid
        if self.tsuid is u'': self.tsuid = None
        self.description = description
        self.notes = notes
        self.custom = custom
        if not self.check():
            raise ValueError("Invalid OpenTSDBAnnotation: \n%s"%str(self))

    def check(self):
        if not isinstance(self.startTime,int) or self.startTime<0: return False
        if not (self.endTime is None or (isinstance(self.endTime,int) and self.endTime>=self.startTime)): return False
        if not self.tsuid is None and not self.tsuid is u'':
            if not isinstance(self.tsuid,basestring) : return False
            try:
                int(self.tsuid,16)
            except:
                return False
        if not (self.description is None or isinstance(self.description,basestring)): return False
        if not (self.notes is None or isinstance(self.notes,basestring)): return False
        if not self.custom is None:
            for k,v in self.custom.iteritems():
                if not (isinstance(k,basestring) and isinstance(v,basestring)): return False
        return True

    def getMap(self):
        myself = self.__dict__
        return { k:v for k,v in myself.iteritems() if v is not None  }

    def json(self):
        return json.dumps(self.getMap())

    def __str__(self):
        return self.getMap().__str__()

    def loadFrom(self,client):
        self.__init__(**client.get_annotation(self.startTime, self.endTime, self.tsuid).getMap())
        return self

    def saveTo(self,client):
        self.__init__(**client.set_annotation(**self.getMap()).getMap())
        return self

    def delete(self,client):
        client.delete_annotation(self.startTime, self.endTime, self.tsuid)
        return self


class OpenTSDBTSMeta:
    """Meta data for a time series, identified by its tsuid"""
    def __init__(self, **kwargs):
	self.created = kwargs.get("created",0)
        self.dataType = kwargs.get("dataType",'')
        self.description = kwargs.get("description",'')
        self.displayName = kwargs.get("displayName",'')
	self.lastReceived = kwargs.get("lastReceived",0)
        self.min = kwargs.get("min",'NaN')
        self.max = kwargs.get("max",'NaN')
        self.notes = kwargs.get("notes",'')
        self.retention = kwargs.get("retention",0)
	self.totalDatapoints = kwargs.get("totalDatapoints",0)
	self.tsuid = kwargs.get("tsuid",'')
        self.units = kwargs.get("units",'')
        self.custom = kwargs["custom"] if kwargs.get("custom",None) is not None else {}

    def set(self, **kwargs):
	self.created = kwargs.get("created",self.created)
        self.dataType = kwargs.get("dataType",self.dataType)
        self.description = kwargs.get("description",self.description)
        self.displayName = kwargs.get("displayName",self.displayName)
	self.lastReceived = kwargs.get("lastReceived",self.lastReceived)
        self.min = kwargs.get("min",self.min)
        self.max = kwargs.get("max",self.max)
        self.notes = kwargs.get("notes",self.notes)
        self.retention = kwargs.get("retention",self.retention)
	self.totalDatapoints = kwargs.get("totalDatapoints",self.totalDatapoints)
	self.tsuid = kwargs.get("tsuid",self.tsuid)
        self.units = kwargs.get("units",self.units)
        self.custom = kwargs.get("custom",self.custom) if kwargs.get("custom",self.custom) is not None else {}

    def getMap(self):
        return self.__dict__

    def loadFrom(self, client):
        r = client.get_tsmeta(self, self.tsuid)
        self.set(**r)
        return self

    def saveTo(self, client):
        self.set(**client.set_tsmeta(self.tsuid, None, self.description, self.displayName, self.notes,
                                     self.custom, self.units, self.dataType, self.retention, self.max, self.min))
        return self

    def delete(self, client):
        client.delete_tsmeta(self.tsuid)
        return self


class OpenTSDBUIDMeta:
    """Meta data for a UID (metric, tagk or tagv)"""
    def __init__(self, **kwargs):
	self.name = kwargs.get("name",'')
	self.uid = kwargs.get("uid",'')
	self.created = kwargs.get("created",0)
        self.type = kwargs.get("type",'')
        self.description = kwargs.get("description",'')
        self.displayName = kwargs.get("displayName",'')
        self.notes = kwargs.get("notes",'')
        self.custom = kwargs["custom"] if kwargs.get("custom",None) is not None else {}

    def set(self, **kwargs):
	self.name = kwargs.get("name",self.name)
	self.uid = kwargs.get("uid",self.uid)
	self.created = kwargs.get("created",self.created)
        self.type = kwargs.get("type",self.type)
        self.description = kwargs.get("description",self.description)
        self.displayName = kwargs.get("displayName",self.displayName)
        self.notes = kwargs.get("notes",self.notes)
        self.custom = kwargs.get("custom",self.custom) if kwargs.get("custom",self.custom) is not None else {}
        
    def getMap(self):
        return self.__dict__

    def loadFrom(self, client):
        r = client.get_uidmeta(self.uid, self.type)
        self.set(**r)
        return self

    def saveTo(self, client):
        self.set(**client.set_uidmeta(self.uid, self.type, self.description, self.displayName, self.notes, self.custom))
        return self

    def delete(self, client):
        client.delete_uidmeta(self.uid, self.type)
        return self


class OpenTSDBTimeSeries:
    """A time series is made of a metric and a set of (at least one) tags.
       It also contains associated meta data for the TS and for the UID components"""
    #basically a metric + tags
    def __init__(self, metric=None, tags=None, tsuid=None):
        self.metric = metric
        self.tags= tags
        # TS meta
        self.metadata = OpenTSDBTSMeta()
        self.metadata.tsuid = tsuid
        # Metric meta
        self.metric_meta = OpenTSDBUIDMeta(type="METRIC", name=metric)
        # tags meta
        self.tagk_meta = {}
        self.tagv_meta = {}
        if self.tags is not None:
            for k,v in self.tags.iteritems():
                self.tagk_meta[k] = OpenTSDBUIDMeta(type="TAGK", name=k)
                self.tagv_meta[v] = OpenTSDBUIDMeta(type="TAGV", name=v)
        # check
        if not self.check():
            raise ValueError("Invalid OpenTSDBTimeSeries: \n%s"%str(self))

    def check(self):
        if self.metric is None and self.metadata.tsuid is None:
            return False
        if self.metric is not None:
            if not OpenTSDBTimeSeries.checkString(self.metric): 
                return False
            for t,v in self.tags.iteritems():
                if not ((OpenTSDBTimeSeries.checkString(t) and OpenTSDBTimeSeries.checkString(v))):
                    return False
            if len(self.tags)<1 : return False
        if not self.metadata.tsuid is None:
            if not isinstance(self.metadata.tsuid,basestring) : return False
            try:
                int(self.metadata.tsuid,16)
            except:
                return False
        return True

    @staticmethod
    def checkString(thestring):
       """The following rules apply to metric and tag values:
          - Strings are case sensitive, i.e. "Sys.Cpu.User" will be stored separately from "sys.cpu.user"
          - Spaces are not allowed
          - Only the following characters are allowed: a to z, A to Z, 0 to 9, -, _, ., / or Unicode letters (as per the specification)"""
       asciichars = string.ascii_letters + "0123456789-_./"
       for c in thestring:
           if not c in asciichars and not ud.category(unicode(c)) in ['Ll', 'Lu']:
               return False
       return True

    def getMap(self, full=False):
        if full:
            myself = copy.deepcopy(self.__dict__)
            for k,v in myself.iteritems():
                if isinstance(v,(OpenTSDBUIDMeta,OpenTSDBTSMeta)):
                    myself[k] = v.getMap()
                if isinstance(v,dict):
                    for kk,vv in v.iteritems():
                        if isinstance(vv,(OpenTSDBUIDMeta,OpenTSDBTSMeta)):
                            myself[k][kk] = vv.getMap()

        else:
            myself = { "metric":self.metric, "tags":self.tags }
            if self.metadata.tsuid is not None: myself["tsuid"]=self.metadata.tsuid
        return myself

    def json(self, full=False):
        return json.dumps(self.getMap(full))

    def __str__(self):
        return self.getMap(full=True).__str__()

    def tsString(self):
        mystring = []
        mystring.append(self.metric)
        mystring.append("{")
        for k,v in self.tags.iteritems():
            mystring.append("%s=%s"%(k,v))
            mystring.append(",")
        mystring[-1] = "}"
        return "".join(mystring)

    def assign_uid(self,client):
        if self.metadata.tsuid is not None:
            raise ValueError("UID already assigned.")
        if self.metric is None or len(self.tags)==0:
            raise ValueError("Cannot assign uid if metric and tags are not set.")
        if not(self.metadata.tsuid is None and self.metric is not None and len(self.tags)>0): return
        try:
            r = client.assign_uid([self.metric], list(self.tags.keys()), list(self.tags.values()))
        except OpenTSDBError as e:
            if e.code==400:
                r = json.loads(e.details)
            else:
                raise
        if self.metric in r["metric"]:
            self.metric_meta.uid = r["metric"][self.metric]
        elif self.metric in r["metric_errors"]:
            self.metric_meta.uid = r["metric_errors"][self.metric].split()[-1]
        else:
            raise OpenTSDBError(400,"assign_uid: unexpected error",json.dumps(r),"")
        for k,v in self.tags.iteritems():
            try:
                if k in r["tagk"]:
                    self.tagk_meta[k].uid = r["tagk"][k]
                else:
                    self.tagk_meta[k].uid = r["tagk_errors"][k].split()[-1]
                if v in r["tagv"]:
                    self.tagv_meta[v].uid = r["tagv"][v]
                else:
                    self.tagv_meta[v].uid = r["tagv_errors"][v].split()[-1]
            except:
                raise OpenTSDBError(400,"assign_uid: unexpected error",json.dumps(r),"")


    def loadFrom(self, client):
        # call ts_meta to get meta.
        if self.metadata.tsuid is not None: 
            try:
                # if the TS meta is not yet set, it will raise an exception
                meta = client.get_tsmeta(tsuid=self.metadata.tsuid)
            except OpenTSDBError:
                # in that case, create a new meta record from the metric string.
                meta = client.set_tsmeta(metric=self.tsString())
        else:
            r = client.get_tsmeta(metric=self.tsString())
            # check the response. 
            # if the TS meta is not yet set, the response will be empty.
            if len(r)==0:
                # no meta was set for this TS, yet. Do it.
                meta = client.set_tsmeta(metric=self.tsString())
            elif len(r)>1:
                raise ValueError("Attempt to load meta for an ambiguous TS. Please specify all the tags.",r)
            else:
                meta = r[0]
        self.metric_meta.set(**meta["metric"])
        if self.metric is None or self.tags is None:
            timeseries = client.search("LOOKUP", metric=self.metric_meta.name)["results"]
            for ts in timeseries:
                if ts["tsuid"]==self.metadata.tsuid:
                    self.metric= ts["metric"]
                    self.tags = ts["tags"]
                    break
        self.metadata.set(**meta)
        for tags_meta in meta["tags"]:
            if tags_meta["type"]=="TAGK":
                if not tags_meta["name"] in self.tagk_meta:
                    self.tagk_meta[tags_meta["name"]] = OpenTSDBUIDMeta()
                self.tagk_meta[tags_meta["name"]].set(**tags_meta)
            else:
                if not tags_meta["name"] in self.tagk_meta:
                    self.tagv_meta[tags_meta["name"]] = OpenTSDBUIDMeta()
                self.tagv_meta[tags_meta["name"]].set(**tags_meta)
        return self

    def saveTo(self,client):
        self.metadata.saveTo(client)
        self.metric_meta.saveTo(client)
        for _,meta in self.tagk_meta.iteritems():
            meta.saveTo(client)
        for _,meta in self.tagv_meta.iteritems():
            meta.saveTo(client)
        return self

    def deleteMeta(self, client, recursive=False):
        self.metadata.delete(client)
        if recursive:
            self.metric_meta.delete(client)
            for k,v in self.tagk_meta.iteritems():
                v.delete(client)
                v = OpenTSDBUIDMeta(type="TAGV", name=k)
            for k,v in self.tagv_meta.iteritems():
                v.delete(client)
                v = OpenTSDBUIDMeta(type="TAGK", name=k)
        return self


class OpenTSDBMeasurement:
    """A measurement is made of a Timeseries + timestamp and value."""
    def __init__(self,timeseries, timestamp, value):
        self.ts = timeseries
        self.timestamp = timestamp
        if isinstance(value,basestring):
            if '.' in value:
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

    def saveTo(self,client):
        return client.put_measurements([self])


class OpenTSDBTreeDefinition:
    def __init__(self,name=None, description=None, notes=None, rules=None, created=None, treeId=None, strictMatch=False, storeFailures=False, enabled=False):
        self.name = name
        self.description = description
        self.notes = notes
        self.rules = {}
        if rules is not None:
            for level,orders in rules.iteritems():
                ordersdict = {}
                for order,therule in orders.iteritems():
                    ordersdict[order] = OpenTSDBRule(**therule)
                self.rules[level] = ordersdict
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
        if not (self.notes is None or isinstance(self.notes,basestring)): return False
        if not (self.created is None or isinstance(self.created,int)): return False
        if not (self.treeId is None or isinstance(self.treeId,int)): return False
        if not isinstance(self.strictMatch,bool) : return False
        if not isinstance(self.storeFailures,bool) : return False
        if not isinstance(self.enabled,bool) : return False
        return True

    def rule(self, level, order):
        return self.rules[str(level)][str(order)]

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

    def create(self,client):
        if self.name is None or len(self.name)==0:
            raise ValueError("Tree name should be defined to allow its creation. Is None.\n"+str(self))
        if self.created is not None or self.treeId is not None: 
            raise ValueError("Tree seems to be created already.\n"+str(self))
        myself = self.getMap()
        data = { key:myself.get(key,None) for key in ['name','description','notes','strictMatch','enabled','storeFailures'] }
        myself = client.create_tree(**data)
        self.__init__(**myself)

    def saveTo(self,client):
        myself = client.edit_tree(self.treeId, self.description, self.notes, self.strictMatch, self.enabled, self.storeFailures)
        self.__init__(**myself)

    def delete(self,client):
        client.delete_tree(self.treeId,True)
        self.treeId = None
        self.created = None
        self.rules = {}
        self.enabled = False


class OpenTSDBRule:

    def __init__(self, treeId, level=0, order=0, type=None, description=None, notes=None, field=None, customField=None, regex=None, separator=None, regexGroupIdx=0, displayFormat=None):
        self.treeId = treeId
        self.level = level
        self.order = order
        self.type = type
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
        if self.type is None or self.type not in ["METRIC","METRIC_CUSTOM","TAGK","TAGK_CUSTOM","TAGV_CUSTOM"] : return False
        if not (self.description is None or isinstance(self.description,basestring)) : return False
        if not (self.notes is None or isinstance(self.notes,basestring)) : return False
        if not (self.field is None or isinstance(self.field,basestring)) : return False
        if not (self.customField is None or isinstance(self.customField,basestring)) : return False
        if not (self.regex is None or isinstance(self.regex,basestring)) : return False
        if not (self.separator is None or isinstance(self.separator,basestring)) : return False
        if not (self.displayFormat is None or isinstance(self.displayFormat,basestring)) : return False
        return True

    def getMap(self):
        myself = self.__dict__
        return { k:v for k,v in myself.iteritems() if v is not None  }

    def json(self):
        return json.dumps(self.getMap())

    def __str__(self):
        return self.getMap().__str__()

    def saveTo(self,client):
        client.set_tree_rule(**self.getMap())

    def delete(self,client):
        client.delete_tree_rule(self.treeId, self.level, self.order)

class OpenTSDBTreeBranch:

    def __init__(self, branchId=None, treeId=None, path=None, displayName=None, depth=None, leaves=None, branches=None, client=None, recursive=False ):
        self.treeId = treeId
        self.path = path
        self.displayName = displayName
        self.branchId = branchId
        self.depth = depth
        self.leaves = leaves
        self.branches = branches

        if client is not None:
            # in that case, load from the client. If recursive is set, this will recursively load all the tree.
            # TODO: check if this is needed. I think so reading the doc: leaves and branches are null for sub-branches.
            if branchId is not None:
                data = client.get_tree_branch(branch=self.branchId)
            elif treeId is not None:
                data = client.get_tree_branch(branch=self.treeId)
            else:
                raise ValueError("Need treeId or branchId to load a branch.")
            self.treeId = data["treeId"]
            self.path = data["path"]
            self.displayName = data["displayName"]
            self.branchId = data["branchId"]
            self.depth = data["depth"]
            self.leaves = map(lambda l: (OpenTSDBTimeSeries(l["metric"], l["tags"], l["tsuid"]), l["displayName"]), data["leaves"])
            if recursive:
                self.branches = []
                for b in data["branches"]:
                    b["client"]=client
                    b["recursive"]=True
                    self.branches.append(OpenTSDBTreeBranch(**b))
            else:
                self.branches = map(lambda b:OpenTSDBTreeBranch(**b),data["branches"])


class OpenTSDBTree(OpenTSDBTreeBranch):

    def __init__(self,treeId,client):
        OpenTSDBTreeBranch.__init__(treeId=treeId, client=client, recursive=True)


