# inspired by a code released with 
# Copyright 2014: Mirantis Inc.
# All Rights Reserved.
# Licensed under the Apache License, Version 2.0 (the "License")

import json
import requests
import inspect
import cStringIO
import gzip

import opentsdbquery
import templates
from opentsdberrors import checkErrors, OpenTSDBError
from opentsdbobjects import OpenTSDBAnnotation, OpenTSDBTSMeta, OpenTSDBTimeSeries, OpenTSDBMeasurement, OpenTSDBTreeDefinition, OpenTSDBRule

# TODO: introduce a Response class
# it should give access to the (error) code, the response content and the error content.
# right now, we have the supposedly interesting part but much is hidden.

# TODO: implement /api/annotation/bulk

def checkArg(value, thetype, NoneAllowed=False, typeErrorMessage="Type mismatch", valueCheck=None, valueErrorMessage="Value error"):
    """check a single argument."""

    if value is None:
        if not NoneAllowed:
            raise ValueError(valueErrorMessage)
        else:
            return True
    else:
        if not isinstance(value,thetype):
            raise TypeError(typeErrorMessage)
        if valueCheck is not None:
            if not valueCheck(value):
                raise ValueError(valueErrorMessage)
            else:
                return True
    return True

def checkArguments(frame, argTypes, valueChecks = {}, 
                   typeErrorMessageTemplate="%(functionName)s::%(argName)s: Type mismatch", 
                   valueErrorMessageTemplate = "%(functionName)s::%(argName)s: got %(argValue)s" ):
    """Checks the arguments of the function.
        - frame must come from inspect.currentframe() within the function
        - argTypes is a dict relating args to their expected type
        - valueChecks is a dict relating args to functions with additional constrains (like being strictly positive) 
        - typeErrorMessageTemplate and valueErrorMessageTemplate are templates for exception messages. It may use functionName, argName and argValue for substitution."""

    args, _, _, values = inspect.getargvalues(frame)
    functionName = inspect.getframeinfo(frame)[2]
    _, _, _, defaults  = inspect.getargspec(getattr(values['self'],functionName))
    if defaults is not None:
        defaults = dict(zip(args[::-1],defaults[::-1]))
    else: 
        defaults = {}
    for arg in args[1:]:
        value = values[arg]
        theType = argTypes[arg]
        valueCheck = valueChecks.get(arg,None)
        noneAllowed = (defaults.get(arg,0) is None)
        typeErrorMessage=typeErrorMessageTemplate%{'functionName':functionName, 'argName':arg, 'argValue':value}
        valueErrorMessage=valueErrorMessageTemplate%{'functionName':functionName, 'argName':arg, 'argValue':value}
        checkArg(value, theType, noneAllowed, typeErrorMessage, valueCheck, valueErrorMessage)
    return True

def process_response(response, allow=[200,204,301]):
    """Processes the response and raise an error if needed."""
    err = checkErrors(response,allow=allow)
    if err is not None:
        code = err["code"]
        message = err["message"]
        details = err.get("details","")
        trace = err.get("trace","")
        raise OpenTSDBError(code, message, details, trace)
    if response.status_code != 204:
        return response.json()
    else:
        return None

class RESTOpenTSDBClient:

    def __init__(self,host,port):
        self.host = host
        self.port = port

    def get_statistics(self):
        """Get info about what metrics are registered and with what stats."""

        req = requests.get(templates.STATS_TEMPL % {'host': self.host, 'port': self.port})
        stats = process_response(req)
        # build a vector of OpenTSDBMeasurements
        output = []
        for s in stats:
            output.append(OpenTSDBMeasurement(OpenTSDBTimeSeries(s["metric"],s["tags"]),s["timestamp"],s["value"]))
        return output

    def put_measurements(self, measurements, summary=False, details=False, sync=False, sync_timeout=0, compress=False):
        """Post new meter(s) to the database.
           Measurements is a vector of valid OpenTSDBMeasurement.
           Other flags affect the response object. """

        # prepare options. Requests doesn't handle empty options by itself.
        if details: options = "?details"
        elif summary: options = "?summary"
        else: options = ""
        
        if sync:
            if options is "":
                options +="?"
            else:
                options +="&"
            options +="sync&sync_timeout=%d"%sync_timeout
        rawData = json.dumps(map(lambda x:x.getMap(),measurements))
        if compress:
            fgz = cStringIO.StringIO()
            with gzip.GzipFile(filename='myfile.json.gz', mode='wb', fileobj=fgz)  as gzip_obj:
                gzip_obj.write(rawData)
            compressedData = fgz.getvalue()
            req = requests.post(templates.PUT_TEMPL % {'host': self.host,'port': self.port,'options': options},
                                data=compressedData,
                                headers={'Content-Encoding':'gzip'} )
        else:
            req = requests.post(templates.PUT_TEMPL % {'host': self.host,'port': self.port,'options': options },
                                data=rawData )
        #handle the response
        return process_response(req, allow=[200,204,301,400])

    def get_aggregators(self):
        """Used to get the list of default aggregation functions."""
        req = requests.get(templates.AGGR_TEMPL % {'host': self.host,'port': self.port})
        return process_response(req)

    def get_annotation(self, startTime, endTime=None, tsuid=None):
        """Used to get an annotation.
           All annotations are identified by the startTime field and optionally the tsuid field. 
           Each note can be global, meaning it is associated with all timeseries, or it can be local, 
           meaning it's associated with a specific tsuid. 
           If the tsuid is not supplied or has an empty value, the annotation is considered to be a global note."""

        checkArguments(inspect.currentframe(), {'startTime':int, 'endTime':int, 'tsuid':basestring}, 
                                               {'startTime':lambda t:t>0, 'endTime':lambda t:t>0,'tsuid':lambda x: int(x,16)})
        params = { "startTime":startTime, "endTime":endTime, "tsuid":tsuid}
        params = { k:v for k,v in params.iteritems() if v is not None  }
        req = requests.get(templates.ANNOT_TEMPL % {'host': self.host,'port': self.port},
                           data = json.dumps(params))
        return OpenTSDBAnnotation(**process_response(req))

    def set_annotation(self, startTime, endTime=None, tsuid=None, description=None, notes=None, custom=None):
        """Used to set an annotation.
           Annotations are very basic objects used to record a note of an arbitrary event at some point, 
           optionally associated with a timeseries. 
           Annotations are not meant to be used as a tracking or event based system, 
           rather they are useful for providing links to such systems by displaying a notice on graphs or via API query calls."""

        checkArguments(inspect.currentframe(), {'startTime':int, 'endTime':int, 'tsuid':basestring, 
                                                'description': basestring, 'notes':basestring, 'custom':dict}, 
                                               {'startTime':lambda t:t>0, 'endTime':lambda t:t>0, 'tsuid':lambda x: int(x,16)} )
        params = { "startTime":startTime, "endTime":endTime, "tsuid":tsuid, "description":description, "notes":notes, "custom":custom}
        params = { k:v for k,v in params.iteritems() if v is not None  }
        req = requests.post(templates.ANNOT_TEMPL % {'host': self.host,'port': self.port},
                            data = json.dumps(params))
        return OpenTSDBAnnotation(**process_response(req))

    def delete_annotation(self, startTime, endTime=None, tsuid=None):
        """Used to delete an annotation."""

        checkArguments(inspect.currentframe(), {'startTime':int, 'endTime':int, 'tsuid':basestring}, 
                                               {'startTime':lambda t:t>0, 'endTime':lambda t:t>0,'tsuid':lambda x: int(x,16)})

        params = { "startTime":startTime, "endTime":endTime, "tsuid":tsuid }
        params = { k:v for k,v in params.iteritems() if v is not None }
        req = requests.delete(templates.ANNOT_TEMPL % {'host': self.host,'port': self.port},
                              data = json.dumps(params))
        return process_response(req)

    def get_configuration(self):
        """This endpoint returns information about the running configuration of the TSD. 
           It is read only and cannot be used to set configuration options.
           This endpoint does not require any parameters via query string or body.
           The response is a hash map of configuration properties and values."""

        req = requests.get(templates.CONF_TEMPL % {'host': self.host,'port': self.port})
        return process_response(req)

    def get_filters(self):
        """This endpoint lists the various filters loaded by the TSD and some information about how to use them."""

        req = requests.get(templates.FILT_TEMPL % {'host': self.host,'port': self.port})
        return process_response(req)

    def drop_caches(self):
        """This endpoint purges the in-memory data cached in OpenTSDB. 
        This includes all UID to name and name to UID maps for metrics, tag names and tag values."""

        req = requests.get(templates.DCACH_TEMPL % {'host': self.host,'port': self.port})
        return process_response(req)

    def get_serializers(self):
        """Used to get the list of serializer plugins loaded by the running TSD. 
        Information given includes the name, implemented methods, content types and methods.."""

        req = requests.get(templates.SERIAL_TEMPL % {'host': self.host,'port': self.port})
        return process_response(req)

    def suggest(self, datatype, query=None, maxResults=None):
        """This endpoint provides a means of implementing an "auto-complete" call that can 
           be accessed repeatedly as a user types a request in a GUI. 
           It does not offer full text searching or wildcards, rather it simply matches 
           the entire string passed in the query on the first characters of the stored data. """

        checkArguments(inspect.currentframe(), {'datatype':basestring, 'query':basestring, 'maxResults':int}, 
                                               {'maxResults':lambda m:m>0, 'datatype':lambda d: d in ['metrics', 'tagk' , 'tagv']} )

        params = { "type":datatype }
        if query is not None: params["q"]=query
        if maxResults is not None and maxResults>0: params["max"]=maxResults
        req = requests.post(templates.SUGGEST_TEMPL % {'host': self.host,'port': self.port},
                           data = json.dumps(params))
        return process_response(req)

    def query(self, openTSDBQuery):
        """enables extracting data from the storage system in various formats determined by the serializer selected"""

        openTSDBQuery.check()
        params = openTSDBQuery.getMap()
        if isinstance(openTSDBQuery,opentsdbquery.OpenTSDBQuery):
            endpoint = templates.QUERY_TEMPL
        elif isinstance(openTSDBQuery,opentsdbquery.OpenTSDBExpQuery):
            endpoint = templates.EXPQUERY_TEMPL
        elif isinstance(openTSDBQuery,opentsdbquery.OpenTSDBQueryLast):
            endpoint = templates.QUERYLST_TEMPL
        else:
            raise TypeError("Not a known query type. Should be OpenTSDBQuery or OpenTSDBExpQuery.")
        req = requests.post(endpoint % {'host': self.host,'port': self.port},
                            data = json.dumps(params))
        return process_response(req)

    def search(self, mode, query="", metric="*", tags={}, limit=25, startindex=0, useMeta=False):
        """This endpoint provides a basic means of searching OpenTSDB meta data. 
           Lookups can be performed against the tsdb-meta table when enabled. 
           Optionally, a search plugin can be installed to send and retreive information from 
           an external search indexing service such as Elastic Search. 
           It is up to each search plugin to implement various parts of this endpoint and return data in a consistent format. 
           The type of object searched and returned depends on the endpoint chosen."""

        endpoint = { "TSMETA": "/api/search/tsmeta", 
                     "TSMETA_SUMMARY":"/api/search/tsmeta_summary",
                     "TSUIDS":"/api/search/tsuids",
                     "UIDMETA":"/api/search/uidmeta",
                     "ANNOTATION":"/api/search/annotation",
                     "LOOKUP":"/api/search/lookup" }

        checkArguments(inspect.currentframe(), {'mode':basestring, 'query':basestring, 'metric':basestring, 'tags':dict, 'limit':int, 'startindex':int, 'useMeta':bool}, 
                                               {'limit':lambda x:x>0, 'startindex':lambda x:x>=0, 'mode':lambda m:m.upper() in endpoint} )

        if mode.upper()=="LOOKUP":
            tagslist =[]
            for k,v in tags.iteritems():
                tagslist.append({ "key":k, "value":v })
            theData = { "metric":metric, "tags": tagslist,  "useMeta": useMeta }
	    # BEGIN PATCH
            # there seems to be a bug with OpenTSDB here... use the query instead.
            def tsString(metric, tags):
                mystring = []
                mystring.append(metric)
                mystring.append("{")
                for k,v in tags.iteritems():
                    mystring.append("%s=%s"%(k,v))
                    mystring.append(",")
                mystring[-1] = "}"
                return "".join(mystring)
            params = { "m":tsString(metric,tags), "use_meta":useMeta }
            req = requests.get(templates.SEARCH_TEMPL % { 'endpoint': endpoint[mode.upper()], 'host': self.host,'port': self.port }, params = params)
            return process_response(req)
	    # END PATCH
        else:
            theData = { "query":query, "limit":limit, "startindex":startindex }
        req = requests.post(templates.SEARCH_TEMPL % { 'endpoint': endpoint[mode.upper()], 'host': self.host,'port': self.port},
                            data = json.dumps(theData))
        return process_response(req)

    def get_version(self):
        """Used to check OpenTSDB version.
        That might be needed in case of unknown bugs - this code is written
        only for the 2.x REST API version, so some of the failures might refer
        to the wrong OpenTSDB version installed."""

        req = requests.get(templates.VERSION_TEMPL % {'host': self.host,'port': self.port})
        return process_response(req)

    def assign_uid(self, metric_list=None, tagk_list=None, tagv_list=None):
        """This endpoint enables assigning UIDs to new metrics, tag names and tag values. 
           Multiple types and names can be provided in a single call and the API will process each name individually, 
           reporting which names were assigned UIDs successfully, along with the UID assigned, and which failed due to invalid characters or had already been assigned."""

        checkArguments(inspect.currentframe(), {'metric_list':list, 'tagk_list':list, 'tagv_list':list},
                                               {'metric_list':lambda l: all(map(lambda x:isinstance(x,basestring),l)),
                                                'tagk_list':lambda l: all(map(lambda x:isinstance(x,basestring),l)),
                                                'tagv_list':lambda l: all(map(lambda x:isinstance(x,basestring),l))})

        if metric_list is None and tagk_list is None and tagv_list is None: 
            return None
        theData = { "metric":metric_list, "tagk":tagk_list, "tagv":tagv_list }
        req = requests.post(templates.ASSIGNUID_TEMPL % {'host': self.host,'port': self.port},
                            data = json.dumps(theData))
        return process_response(req, allow=[200,400])

    def get_tsmeta(self, tsuid=None, metric=None):
        """This endpoint enables searching timeseries meta data information, that is meta data associated with 
           a specific timeseries associated with a metric and one or more tag name/value pairs. 
           Some fields are set by the TSD but others can be set by the user."""

        if metric is None and tsuid is None:
            raise ValueError("Either metric or tsuid must be set.")
        if metric is not None and tsuid is not None:
            raise ValueError("Only one of metric or tsuid must be set.")
        checkArguments(inspect.currentframe(), {'tsuid':basestring, 'metric':basestring}, {'metric': lambda x: OpenTSDBTimeSeries.checkString, 'tsuid':lambda x: int(x,16)})
        if tsuid is None:
            params = { 'm':metric }
        else:
            params = {'tsuid':tsuid}
        req = requests.get(templates.TSMETA_TEMPL % {'host': self.host,'port': self.port},params = params)
        return process_response(req)

    def set_tsmeta(self, tsuid=None, metric=None, description=None, displayName=None, notes=None, custom=None, 
                                                  units=None, dataType=None, retention=None, maximum=None, minimum=None):
        """This endpoint enables editing timeseries meta data information, that is meta data associated with 
           a specific timeseries associated with a metric and one or more tag name/value pairs. 
           Some fields are set by the TSD but others can be set by the user. 
           Only the fields supplied with the request will be stored. Existing fields that are not included will be left alone."""

        checkArguments(inspect.currentframe(), {'tsuid':basestring, 'metric':basestring, 'description':basestring, 'displayName':basestring, 
                                                'notes':basestring, 'custom':dict, 'units':basestring, 'dataType':basestring, 
                                                'retention':int, 'maximum':(float,basestring), 'minimum':(float,basestring)}, 
                                               {'tsuid':lambda x: int(x,16), 'retention': lambda x:x>=0} )

        if tsuid is not None:
            # in that case perform a standard query
            theData = { "tsuid":tsuid, "description":description, "displayName":displayName, "notes":notes, 
                        "custom":custom, "units":units, "dataType":dataType, "retention":retention, "max":maximum, "min":minimum}
            theData = { k:v for k,v in theData.iteritems() if v is not None }
            req = requests.post(templates.TSMETA_TEMPL % {'host': self.host,'port': self.port},
                                data = json.dumps(theData))
            return process_response(req)
        elif metric is not None:
            # perform a 2.1 metric style query
            theData = { "description":description, "displayName":displayName, "notes":notes, 
                        "custom":custom, "units":units, "dataType":dataType, "retention":retention, "max":maximum, "min":minimum}
            theData = { k:v for k,v in theData.iteritems() if v is not None }
            params = {'m':metric, 'create':'true'}
            req = requests.post(templates.TSMETA_TEMPL % {'host': self.host,'port': self.port},
                                data = json.dumps(theData),
                                params = params)
            return process_response(req)
        else:
            raise ValueError("Either the TSUID or a metric query must be set.")

    def delete_tsmeta(self, tsuid):
        """This endpoint enables deleting timeseries meta data information.
           Please note that deleting a meta data entry will not delete the data points stored for the timeseries. 
           Neither will it remove the UID assignments or associated UID meta objects."""

        checkArguments(inspect.currentframe(), {'tsuid':basestring}, {'tsuid':lambda x: int(x,16)})

        req = requests.delete(templates.TSMETA_TEMPL % {'host': self.host,'port': self.port},
                              data = json.dumps({ "tsuid": tsuid }))
        return process_response(req)

    def define_retention(self, tsuid, retention_days):
        """Set retention days for the defined by ID timeseries.

        ##########################################################
        NOTE: currently not working directly through the REST API.
              that should be done directly on the HBase level.
        ##########################################################

        :param tsuid: hexadecimal representation of the timeseries UID
        :param retention_days: number of days of data points to retain for the
                               given timeseries. When set to 0, the default,
                               data is retained indefinitely."""

        return self.set_tsmeta(tsuid, retention=retention_days)

    def get_uidmeta(self, uid, uidtype):
        """This endpoint enables getting UID meta data information, that is meta data associated with metrics, 
           tag names and tag values. Some fields are set by the TSD but others can be set by the user. """

        checkArguments(inspect.currentframe(), {'uid':basestring, 'uidtype':basestring}, 
                                               {'uid':lambda x: int(x,16), 'uidtype':lambda x: x.upper() in ["METRIC", "TAGK", "TAGV"]})

        theData = {"uid":uid, "type":uidtype}
        req = requests.get(templates.UIDMETA_TEMPL % {'host': self.host,'port': self.port},
                           params = theData)
        return process_response(req)

    def set_uidmeta(self, uid, uidtype, description=None, displayName=None, notes=None, custom=None):
        """This endpoint enables editing  UID meta data information, that is meta data associated with metrics, 
           tag names and tag values. Some fields are set by the TSD but others can be set by the user.
           Only the fields supplied with the request will be stored. Existing fields that are not included will be left alone."""

        checkArguments(inspect.currentframe(), {'uid':basestring, 'uidtype':basestring, 'description':basestring, 'displayName':basestring, 'notes':basestring, 'custom':dict}, 
                                               {'uid':lambda x: int(x,16), 'uidtype':lambda x: x.upper() in ["METRIC", "TAGK", "TAGV"]})

        theData = { "uid":uid, "type":uidtype, "description":description, "displayName":displayName, "notes":notes, "custom":custom}
        theData = { k:v for k,v in theData.iteritems() if v is not None }
        req = requests.post(templates.UIDMETA_TEMPL % {'host': self.host,'port': self.port},
                            data = json.dumps(theData))
        return process_response(req)

    def delete_uidmeta(self, uid, uidtype):
        """This endpoint enables deleting UID meta data information, that is meta data associated with metrics, tag names and tag values."""

        checkArguments(inspect.currentframe(), {'uid':basestring, 'uidtype':basestring}, 
                                               {'uid':lambda x: int(x,16), 'uidtype':lambda x: x.upper() in ["METRIC", "TAGK", "TAGV"]})

        theData = {"uid":uid, "type":uidtype}
        req = requests.delete(templates.UIDMETA_TEMPL % {'host': self.host,'port': self.port},
                              data = json.dumps(theData))
        return process_response(req)

    def create_tree(self, name, description=None, notes=None, strictMatch=False, enabled=False, storeFailures=False):
        """Trees are meta data used to organize time series in a heirarchical structure for browsing similar to a typical file system. 
           This allows for creating a tree definition. Tree definitions include configuration and meta data accessible via this endpoint, 
           as well as the rule set defined with other methods.
           When creating a tree it will have the enabled field set to false by default. 
           After creating a tree you should add rules then use the tree/test endpoint with a few TSUIDs to make sure the resulting tree will be what you expected. 
           After you have verified the results, you can set the enabled field to true and new TSMeta objects or a tree synchronization will start to populate branches."""

        checkArguments(inspect.currentframe(), {'name':basestring, 'description':basestring, 'notes':basestring, 'strictMatch':bool, 'enabled':bool, 'storeFailures':bool})

        theData = {"name":name, "strictMatch":strictMatch, "enabled":enabled, "storeFailures":storeFailures, "description":description, "notes":notes }
        theData = { k:v for k,v in theData.iteritems() if v is not None }
        req = requests.post(templates.TREE_TEMPL % {'host': self.host,'port': self.port},
                            data = json.dumps(theData))
        return process_response(req)

    def delete_tree(self, treeId, definition=False):
        """Using this method will remove only collisions, not matched entries and branches for the given tree from storage. 
           Because the delete can take some time, the endpoint will return a successful 204 response without data if the delete completed. 
           If the tree was not found, it will return a 404. 
           If you want to delete the tree definition itself, you can supply the defintion flag in the query string 
           with a value of true and the tree and rule definitions will be removed as well."""

        checkArguments(inspect.currentframe(), {'treeId':int, 'definition':bool})

        theData = { "treeId":treeId, "definition":definition }
        req = requests.delete(templates.TREE_TEMPL % {'host': self.host,'port': self.port},
                              data = json.dumps(theData))
        return process_response(req)

    def edit_tree(self, treeId, description=None, notes=None, strictMatch=False, enabled=False, storeFailures=False):
        """Using this method, you can edit most of the fields for an existing tree. A successful request will return the modified tree object."""

        checkArguments(inspect.currentframe(), {'treeId':int, 'description':basestring, 'notes':basestring, 'strictMatch':bool, 'enabled':bool, 'storeFailures':bool})

        theData = {"treeId":treeId, "strictMatch":strictMatch, "enabled":enabled, "storeFailures":storeFailures, "description":description, "notes":notes }
        theData = { k:v for k,v in theData.iteritems() if v is not None }
        req = requests.post(templates.TREE_TEMPL % {'host': self.host,'port': self.port},
                            data = json.dumps(theData))
        return process_response(req)

    def get_tree(self, treeId=None):
        """This returns the tree with the given id."""

        checkArguments(inspect.currentframe(), {'treeId':int})

        req = requests.get(templates.TREE_TEMPL % {'host': self.host,'port': self.port},
                           data = json.dumps({"treeId":treeId}))
        resp = process_response(req)
        if isinstance(resp,list):
            return map(lambda t: OpenTSDBTreeDefinition(**t),resp)
        else:
            return OpenTSDBTreeDefinition(**resp)

    def get_tree_branch(self, treeId=None, branch=None):
        """A branch represents a level in the tree heirarchy and contains information about child branches and/or leaves.
           A branch is identified by a branchId, a hexadecimal encoded string that represents the ID of the tree it belongs to 
           as well as the IDs of each parent the branch stems from. 
           All branches stem from the ROOT branch of a tree and this is usually the starting place when browsing. 
           To fetch the ROOT just call this endpoingt with a valid treeId. The root branch ID is also a 4 character encoding of the tree ID."""

        checkArguments(inspect.currentframe(), {'treeId':int, 'branch':basestring},{'branch':lambda x:int(x,16)})

        if branch is not None:
            theData = { "branch":branch }
        elif treeId is not None:
            theData = { "treeId":treeId }
        else:
            raise ValueError("get_tree_branch requires at least one of treeId or branch.")
        req = requests.get(templates.TREEBRANCH_TEMPL % {'host': self.host,'port': self.port},
                           data = json.dumps(theData))
        return process_response(req)

    def get_tree_collisions(self, treeId, tsuids):
        """When processing a TSMeta, if the resulting leaf would overwrite an existing leaf with a different TSUID, a collision will be recorded. 
           This endpoint allows retreiving a list of the TSUIDs that were not included in a tree due to collisions. 
           It is useful for debugging in that if you find a TSUID in this list, you can pass it through the /tree/test endpoint to get details on why the collision occurred.
           Calling this endpoint without a list of one or more TSUIDs will return all collisions in the tree. 
           If you have a large number of timeseries in your system, the response can potentially be very large. Thus it is best to use this endpoint with specific TSUIDs.
           If storeFailures is diabled for the tree, this endpoint will not return any data. Collisions will still appear in the TSD's logs."""

        checkArguments(inspect.currentframe(), {'treeId':int, 'tsuids':list})

        theData = { "treeId":treeId }
        thetsuids = ""
        for tsuid in tsuids:
            thetsuids += tsuid+","
        if len(tsuids)>0:
            thetsuids = thetsuids[:-1]
        theData["tsuids"]=thetsuids
        req = requests.get(templates.TREECOLL_TEMPL % {'host': self.host,'port': self.port},
                           data = json.dumps(theData))
        return process_response(req)

    def get_tree_notmatched(self, treeId, tsuids):
        """When processing a TSMeta, if the tree has strictMatch enabled and the meta fails to match on a rule in any level of the set, a not matched entry will be recorded. 
           This endpoint allows for retrieving the list of TSUIDs that failed to match a rule set. 
           It is useful for debugging in that if you find a TSUID in this list, you can pass it through the /tree/test endpoint to get details on why the meta failed to match.
           Calling this endpoint without a list of one or more TSUIDs will return all non-matched TSUIDs in the tree. 
           If you have a large number of timeseries in your system, the response can potentially be very large. Thus it is best to use this endpoint with specific TSUIDs.
           If storeFailures is diabled for the tree, this endpoint will not return any data. Not Matched entries will still appear in the TSD's logs."""
           
        checkArguments(inspect.currentframe(), {'treeId':int, 'tsuids':list})

        theData = { "treeId":treeId }
        thetsuids = ""
        for tsuid in tsuids:
            thetsuids += tsuid+","
        if len(tsuids)>0:
            thetsuids = thetsuids[:-1]
        theData["tsuids"]=thetsuids
        req = requests.get(templates.TREEMATCH_TEMPL % {'host': self.host,'port': self.port},
                           data = json.dumps(theData))
        return process_response(req)
        
    def test_tree(self, treeId, tsuids):
        """For debugging a rule set, the test endpoint can be used to run a TSMeta object through a tree's rules and determine where 
           in the heirarchy the leaf would appear. Or find out why a timeseries failed to match on a rule set or collided with an existing timeseries. 
           The only method supported is GET and no changes will be made to the actual tree in storage when using this endpoint."""

        checkArguments(inspect.currentframe(), {'treeId':int, 'tsuids':list})

        theData = { "treeId":treeId }
        thetsuids = ""
        for tsuid in tsuids:
            thetsuids += tsuid+","
        if len(tsuids)>0:
            thetsuids = thetsuids[:-1]
        theData["tsuids"]=thetsuids
        req = requests.get(templates.TREETEST_TEMPL % {'host': self.host,'port': self.port},
                           data = json.dumps(theData))
        return process_response(req)

    def get_tree_rule(self, treeId, level=0, order=0):
        """Access to an individual tree rule. 
           Rules are addressed by their tree ID, level and order and all requests require these three parameters."""

        checkArguments(inspect.currentframe(), {'treeId':int, 'level':int, 'order':int})

        theData = { "treeId":treeId, "level":level, "order":order }
        req = requests.get(templates.TREERULE_TEMPL % {'host': self.host,'port': self.port},
                           data = json.dumps(theData))
        return OpenTSDBRule(**process_response(req))

    def set_tree_rule(self, treeId, level=0, order=0, type=None, description=None, notes=None, field=None, customField=None, regex=None, separator=None, regexGroupIdx=0, displayFormat=None):
        """allows for easy modification of a single rule in the set.
           You can create a new rule or edit an existing rule. New rules require a type value. 
           Existing trees require a valid treeId ID and any fields that require modification. 
           A successful request will return the modified rule object. 
           Note that if a rule exists at the given level and order, any changes will be merged with or overwrite the existing rule."""

        checkArguments(inspect.currentframe(), {'treeId':int, 'level':int, 'order':int, 'type':basestring, 'description':basestring, 
                                                'notes':basestring, 'field':basestring, 'customField':basestring, 'regex':basestring, 
                                                'separator':basestring, 'regexGroupIdx':int, 'displayFormat':basestring},
                                               {'type':lambda x:x in ["METRIC","METRIC_CUSTOM","TAGK","TAGK_CUSTOM","TAGV_CUSTOM"], 'regexGroupIdx':lambda x: x>=0})

        theData = { "treeId":treeId, "level":level, "order":order, "regexGroupIdx":regexGroupIdx, "type":type, "description":description, 
                    "notes":notes, "field":field, "customField":customField, "regex":regex, "separator":separator, "displayFormat":displayFormat }
        theData = { k:v for k,v in theData.iteritems() if v is not None }
        req = requests.post(templates.TREERULE_TEMPL % {'host': self.host,'port': self.port},
                            data = json.dumps(theData))
        return OpenTSDBRule(**process_response(req,allow=[200,204,301,304]))

    def delete_tree_rule(self, treeId, level=0, order=0, deleteAll=False):
        """Using the DELETE method will remove a rule from a tree.
           If deleteAll is true, all rules from the tree will be deleted."""

        checkArguments(inspect.currentframe(), {'treeId':int, 'level':int, 'order':int, 'deleteAll':bool})

        if deleteAll:
            theData = { "treeId":treeId }
            req = requests.delete(templates.TREERULES_TEMPL % {'host': self.host,'port': self.port},
                                  data = json.dumps(theData))
        else:
            theData = { "treeId":treeId, "level":level, "order":order }
            req = requests.delete(templates.TREERULE_TEMPL % {'host': self.host,'port': self.port},
                                  data = json.dumps(theData))
	return process_response(req, allow=[204])

