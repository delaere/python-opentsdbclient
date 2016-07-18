# Copyright 2014: Mirantis Inc.
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

import json

import requests

from opentsdbmetric import metric as opentsdbmetric
import opentsdbquery
import opentsdbclient
#TODO: drop the non-REST part of the client.
from opentsdbclient import base
from opentsdbclient.rest import utils
from opentsdberrors import checkErrors

#TODO: add support for compression of the json content, at least in put.

#TODO: make it more OO:
# objects for tree and rules
# objects for annotations, tsmeta, uidmeta

class RESTOpenTSDBClient(base.BaseOpenTSDBClient):

    def get_statistics(self):
        """Get info about what metrics are registered and with what stats."""
        req = requests.get(utils.STATS_TEMPL % {'host': self.hosts[0][0],
                                                'port': self.hosts[0][1]})
        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    #TODO do the following in the base class + telnet api
    #def put_meter(self, metrics, summary=False, details=False, sync=False, sync_timeout=0):
    #for m in metrics:
    #    if not isinstance(m,opentsdbmetric):
    #        raise TypeError("Please use opentsdbmetric to define metrics.")
    #    else:
    #        m.check()
    def put_meter(self, meters, summary=False, details=False, sync=False, sync_timeout=0):
        """Post new meter(s) to the database.

        Meters is a vector dictionnaries.
        Meter dictionary *should* contain the following four required fields:
          - metric: the name of the metric you are storing
          - timestamp: a Unix epoch style timestamp in seconds or milliseconds.
                       The timestamp must not contain non-numeric characters.
          - value: the value to record for this data point. It may be quoted or
                   not quoted and must conform to the OpenTSDB value rules.
          - tags: a map of tag name/tag value pairs. At least one pair must be
                  supplied.
        """

        if details: options = "?details"
        elif summary: options = "?summary"
        else: options = ""
        
        if sync:
            if options is "":
                options +="?"
            else:
                options +="&"
            options +="sync&sync_timeout=%d"%sync_timeout

        req = requests.post(utils.PUT_TEMPL %
                            {'host': self.hosts[0][0],
                             'port': self.hosts[0][1],
                             'options': options },
                            data=json.dumps(meters)
                            )
        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def get_aggregators(self):
        """Used to get the list of default aggregation functions."""
        req = requests.get(utils.AGGR_TEMPL % {'host': self.hosts[0][0],
                                               'port': self.hosts[0][1]})

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def get_annotation(self, startTime, endTime=None, tsuid=None):
        """Used to get an annotation.
        
           All annotations are identified by the startTime field and optionally the tsuid field. 
           Each note can be global, meaning it is associated with all timeseries, or it can be local, 
           meaning it's associated with a specific tsuid. 
           If the tsuid is not supplied or has an empty value, the annotation is considered to be a global note."""

        params = { "startTime":startTime }
        if endTime is not None: params["endTime"]=endTime
        if tsuid is not None: params["tsuid"]=tsuid
        req = requests.get(utils.ANNOT_TEMPL % {'host': self.hosts[0][0],
                                                'port': self.hosts[0][1]},
                           data = json.dumps(params))
        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def set_annotation(self, startTime, endTime=None, tsuid=None, description=None, notes=None, custom=None):
        """Used to set an annotation.
        
           Annotations are very basic objects used to record a note of an arbitrary event at some point, 
           optionally associated with a timeseries. 
           Annotations are not meant to be used as a tracking or event based system, 
           rather they are useful for providing links to such systems by displaying a notice on graphs or via API query calls."""

        params = { "startTime":startTime }
        if endTime is not None: params["endTime"]=endTime
        if tsuid is not None: params["tsuid"]=tsuid
        if description is not None: params["description"]=description
        if notes is not None: params["notes"]=notes
        if custom is not None: params[custom]=custom
        req = requests.post(utils.ANNOT_TEMPL % {'host': self.hosts[0][0],
                                                 'port': self.hosts[0][1]},
                            data = json.dumps(params))
        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def delete_annotation(self, startTime, endTime=None, tsuid=None):
        """Used to delete an annotation."""
        params = { "startTime":startTime }
        if endTime is not None: params["endTime"]=endTime
        if tsuid is not None: params["tsuid"]=tsuid
        req = requests.delete(utils.ANNOT_TEMPL % {'host': self.hosts[0][0],
                                                'port': self.hosts[0][1]},
                           data = json.dumps(params))
        err = checkErrors(req)
        return err

    def get_configuration(self):
        """This endpoint returns information about the running configuration of the TSD. 

           It is read only and cannot be used to set configuration options.
           This endpoint does not require any parameters via query string or body.
           The response is a hash map of configuration properties and values."""
        req = requests.get(utils.CONF_TEMPL % {'host': self.hosts[0][0],
                                               'port': self.hosts[0][1]})
        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def get_filters(self):
        """This endpoint lists the various filters loaded by the TSD and some information about how to use them."""
        req = requests.get(utils.FILT_TEMPL % {'host': self.hosts[0][0],
                                               'port': self.hosts[0][1]})
        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err


    def drop_caches(self):
        """This endpoint purges the in-memory data cached in OpenTSDB. 
        This includes all UID to name and name to UID maps for metrics, tag names and tag values."""
        req = requests.get(utils.DCACH_TEMPL % {'host': self.hosts[0][0],
                                                'port': self.hosts[0][1]})
        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def get_serializers(self):
        """Used to get the list of serializer plugins loaded by the running TSD. 
        Information given includes the name, implemented methods, content types and methods.."""
        req = requests.get(utils.SERIAL_TEMPL % {'host': self.hosts[0][0],
                                                 'port': self.hosts[0][1]})

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def suggest(self, datatype, query=None, maxResults=None):
        """This endpoint provides a means of implementing an "auto-complete" call that can 
           be accessed repeatedly as a user types a request in a GUI. 
           It does not offer full text searching or wildcards, rather it simply matches 
           the entire string passed in the query on the first characters of the stored data. """
        params = { "type":datatype }
        if query is not None: params["q"]=query
        if maxResults is not None and maxResults>0: params["max"]=maxResults
        req = requests.post(utils.SUGGEST_TEMPL % {'host': self.hosts[0][0],
                                                 'port': self.hosts[0][1]},
                           data = json.dumps(params))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def query(self, openTSDBQuery):
        """enables extracting data from the storage system in various formats determined by the serializer selected"""
        openTSDBQuery.check()
        params = openTSDBQuery.getMap()
        if isinstance(openTSDBQuery,opentsdbquery.OpenTSDBQuery):
            endpoint = utils.QUERY_TEMPL
        elif isinstance(openTSDBQuery,opentsdbquery.OpenTSDBExpQuery):
            endpoint = utils.EXPQUERY_TEMPL
        elif isinstance(openTSDBQuery,opentsdbquery.OpenTSDBQueryLast):
            endpoint = utils.QUERYLST_TEMPL
        else:
            raise TypeError("Not a known query type. Should be OpenTSDBQuery or OpenTSDBExpQuery.")
        req = requests.post(endpoint % {'host': self.hosts[0][0],
                                        'port': self.hosts[0][1]},
                            data = json.dumps(params))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

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
        assert mode.upper() in endpoint

        if mode.upper() is "LOOKUP":
            tagslist =[]
            for k,v in tags.iteritems():
                tagslist.append({ "key":k, "value":v })
            theData = { "metric":metric, "tags": tagslist,  "useMeta": useMeta }
        else:
            theData = { "query":query, "limit":limit, "startindex":startindex }

        req = requests.post(endpoint[mode.upper()] % {'host': self.hosts[0][0],
                                                      'port': self.hosts[0][1]},
                            data = json.dumps(theData))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err


    def get_version(self):
        """Used to check OpenTSDB version.

        That might be needed in case of unknown bugs - this code is written
        only for the 2.x REST API version, so some of the failures might refer
        to the wrong OpenTSDB version installed.
        """
        req = requests.get(utils.VERSION_TEMPL % {'host': self.hosts[0][0],
                                                  'port': self.hosts[0][1]})
        return req

    def assign_uid(self, metric_list=None, tagk_list=None, tagv_list=None):
        """This endpoint enables assigning UIDs to new metrics, tag names and tag values. 
           Multiple types and names can be provided in a single call and the API will process each name individually, 
           reporting which names were assigned UIDs successfully, along with the UID assigned, and which failed due to invalid characters or had already been assigned."""

        if metric_list is None and tagk_list is None and tagv_list is None: 
            return None

        for metric in metric_list:
            if not isinstance(metric, basestring):
                raise TypeError("assign_uid arg type mismatch.")
        
        for tagk in tagk_list: 
            if not isinstance(tagk, basestring):
                raise TypeError("assign_uid arg type mismatch.")

        for tagv in tagv_list: 
            if not isinstance(tagv, basestring):
                raise TypeError("assign_uid arg type mismatch.")

        theData = { "metric":metric_list, "tagk":tagk_list, "tagv":tagv_list }

        req = requests.post(utils.ASSIGNUID_TEMPL % {'host': self.hosts[0][0],
                                                     'port': self.hosts[0][1]},
                            data = json.dumps(theData))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def get_tsmeta(self, tsuid):
        """This endpoint enables searching timeseries meta data information, that is meta data associated with 
           a specific timeseries associated with a metric and one or more tag name/value pairs. 
           Some fields are set by the TSD but others can be set by the user."""

        req = requests.get(utils.TSMETA_TEMPL % {'host': self.hosts[0][0],
                                                 'port': self.hosts[0][1]},
                           data = json.dumps({ "tsuid": tsuid }))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def set_tsmeta(self, tsuid, description=None, displayName=None, notes=None, custom=None, 
                                units=None, dataType=None, retention=None, maximum=None, minimum=None):
        """This endpoint enables editing timeseries meta data information, that is meta data associated with 
           a specific timeseries associated with a metric and one or more tag name/value pairs. 
           Some fields are set by the TSD but others can be set by the user. 
           Only the fields supplied with the request will be stored. Existing fields that are not included will be left alone."""
        if not isinstance(tsuid,basestring):
            raise TypeError("set_tsmeta arg type mismatch")
        theData = { "tsuid":tsuid }
        if description is not None: 
            if not isinstance(description, basestring):
                raise TypeError("set_tsmeta arg type mismatch")
            theData["description"]=description
        if displayName is not None: 
            if not isinstance(displayName, basestring):
                raise TypeError("set_tsmeta arg type mismatch")
            theData["displayName"]=displayName
        if notes is not None: 
            if not isinstance(notes, basestring):
                raise TypeError("set_tsmeta arg type mismatch")
            theData["notes"]=notes
        if custom is not None:
            if not isinstance(custom, dict):
                raise TypeError("set_tsmeta arg type mismatch")
            theData["custom"]=custom
        if units is not None: 
            if not isinstance(units, basestring):
                raise TypeError("set_tsmeta arg type mismatch")
            theData["units"]=units
        if dataType is not None: 
            if not isinstance(dataType, basestring):
                raise TypeError("set_tsmeta arg type mismatch")
            theData["dataType"]=dataType
        if retention is not Note: 
            if not isinstance(retention, int) or retention<0:
                raise TypeError("set_tsmeta arg type mismatch")
            theData["retention"]=retention
        if maximum is not None: 
            if not isinstance(maximum, float):
                raise TypeError("set_tsmeta arg type mismatch")
            theData["max"]=maximum
        if minimum is not None: 
            if not isinstance(minimum, float):
                raise TypeError("set_tsmeta arg type mismatch")
            theData["min"]=minimum

        req = requests.post(utils.TSMETA_TEMPL % {'host': self.hosts[0][0],
                                                  'port': self.hosts[0][1]},
                            data = json.dumps(theData))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err


    def delete_tsmeta(self, tsuid):
        """This endpoint enables deleting timeseries meta data information.
           Please note that deleting a meta data entry will not delete the data points stored for the timeseries. 
           Neither will it remove the UID assignments or associated UID meta objects."""
        req = requests.delete(utils.TSMETA_TEMPL % {'host': self.hosts[0][0],
                                                    'port': self.hosts[0][1]},
                              data = json.dumps({ "tsuid": tsuid }))

        err = checkErrors(req)
        return err

    def define_retention(self, tsuid, retention_days):
        """Set retention days for the defined by ID timeseries.

        ##########################################################
        NOTE: currently not working directly through the REST API.
              that should be done directly on the HBase level.
        ##########################################################

        :param tsuid: hexadecimal representation of the timeseries UID
        :param retention_days: number of days of data points to retain for the
                               given timeseries. When set to 0, the default,
                               data is retained indefinitely.
        """
        return self.set_tsmeta(tsuid, retention=retention_days)

    def get_uidmeta(self, uid, uidtype):
        """This endpoint enables getting UID meta data information, that is meta data associated with metrics, 
           tag names and tag values. Some fields are set by the TSD but others can be set by the user. """
        assert uidtype in ["metric", "tagk", "tagv"]
        theData = {"uid":uid, "type":uidtype}

        req = requests.get(utils.UIDMETA_TEMPL % {'host': self.hosts[0][0],
                                                  'port': self.hosts[0][1]},
                           data = json.dumps(theData))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err


    def set_uidmeta(self, uid, uidtype, description=None, displayName=None, notes=None, custom=None):
        """This endpoint enables editing  UID meta data information, that is meta data associated with metrics, 
           tag names and tag values. Some fields are set by the TSD but others can be set by the user.
           Only the fields supplied with the request will be stored. Existing fields that are not included will be left alone."""
        assert uidtype in ["metric", "tagk", "tagv"]
        if not isinstance(uid,basestring):
            raise TypeError("set_uidmeta arg type mismatch")
        theData = { "uid":uid }
        if description is not None: 
            if not isinstance(description, basestring):
                raise TypeError("set_uidmeta arg type mismatch")
            theData["description"]=description
        if displayName is not None: 
            if not isinstance(displayName, basestring):
                raise TypeError("set_uidmeta arg type mismatch")
            theData["displayName"]=displayName
        if notes is not None: 
            if not isinstance(notes, basestring):
                raise TypeError("set_uidmeta arg type mismatch")
            theData["notes"]=notes
        if custom is not None:
            if not isinstance(custom, dict):
                raise TypeError("set_uidmeta arg type mismatch")
            theData["custom"]=custom

        req = requests.post(utils.UIDMETA_TEMPL % {'host': self.hosts[0][0],
                                                   'port': self.hosts[0][1]},
                            data = json.dumps(theData))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def delete_uidmeta(self, uid, uidtype):
        """This endpoint enables deleting UID meta data information, that is meta data associated with metrics, tag names and tag values."""
        assert uidtype in ["metric", "tagk", "tagv"]
        theData = {"uid":uid, "type":uidtype}

        req = requests.delete(utils.TSMETA_TEMPL % {'host': self.hosts[0][0],
                                                    'port': self.hosts[0][1]},
                              data = json.dumps(theData))

        err = checkErrors(req)
        return err

    def create_tree(self, name, description=None, notes=None, strictMatch=False, enabled=False, storeFailures=False):
        """Trees are meta data used to organize time series in a heirarchical structure for browsing similar to a typical file system. 
           This allows for creating a tree definition. Tree definitions include configuration and meta data accessible via this endpoint, 
           as well as the rule set defined with other methods.
           When creating a tree it will have the enabled field set to false by default. 
           After creating a tree you should add rules then use the tree/test endpoint with a few TSUIDs to make sure the resulting tree will be what you expected. 
           After you have verified the results, you can set the enabled field to true and new TSMeta objects or a tree synchronization will start to populate branches."""
        if not isinstance(name, basestring):
            raise TypeError("create_tree arg type mismatch.")
        theData = {"name":name, "strictMatch":strictMatch, "enabled":enabled, "storeFailures":storeFailures }
        if description is not None :
            if not isinstance(description, basestring):
                raise TypeError("create_tree arg type mismatch.")
            theData["description"]=description
        if notes is not None:
            if not isinstance(notes, basestring):
                raise TypeError("create_tree arg type mismatch.")
            theData["notes"]=notes
        
        req = requests.post(utils.TREE_TEMPL % {'host': self.hosts[0][0],
                                                'port': self.hosts[0][1]},
                            data = json.dumps(theData))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err


    def delete_tree(self, treeId, definition=False):
        """Using this method will remove only collisions, not matched entries and branches for the given tree from storage. 
           Because the delete can take some time, the endpoint will return a successful 204 response without data if the delete completed. 
           If the tree was not found, it will return a 404. 
           If you want to delete the tree definition itself, you can supply the defintion flag in the query string 
           with a value of true and the tree and rule definitions will be removed as well."""
        theData = { "treeId":treeId, "definition":definition }
        
        req = requests.delete(utils.TREE_TEMPL % {'host': self.hosts[0][0],
                                                  'port': self.hosts[0][1]},
                              data = json.dumps(theData))

        err = checkErrors(req)
        return err

    def edit_tree(self, treeId, description=None, notes=None, strictMatch=False, enabled=False, storeFailures=False):
        """Using this method, you can edit most of the fields for an existing tree. A successful request will return the modified tree object."""
        if not isinstance(treeId, int):
            raise TypeError("create_tree arg type mismatch.")
        theData = {"treeId":treeId, "strictMatch":strictMatch, "enabled":enabled, "storeFailures":storeFailures }
        if description is not None :
            if not isinstance(description, basestring):
                raise TypeError("create_tree arg type mismatch.")
            theData["description"]=description
        if notes is not None:
            if not isinstance(notes, basestring):
                raise TypeError("create_tree arg type mismatch.")
            theData["notes"]=notes
        
        req = requests.post(utils.TREE_TEMPL % {'host': self.hosts[0][0],
                                                'port': self.hosts[0][1]},
                            data = json.dumps(theData))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def get_tree(self, treeId=None):
        """This returns the tree with the given id."""

        req = requests.get(utils.TREE_TEMPL % {'host': self.hosts[0][0],
                                               'port': self.hosts[0][1]},
                           data = json.dumps({"treeId":treeId}))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def get_tree_branch(self, treeId=None, branch=None):
        """A branch represents a level in the tree heirarchy and contains information about child branches and/or leaves.
           A branch is identified by a branchId, a hexadecimal encoded string that represents the ID of the tree it belongs to 
           as well as the IDs of each parent the branch stems from. 
           All branches stem from the ROOT branch of a tree and this is usually the starting place when browsing. 
           To fetch the ROOT just call this endpoingt with a valid treeId. The root branch ID is also a 4 character encoding of the tree ID."""

        if branch is not None:
            theData = { "branch":branch }
        elif treeId is not None:
            theData = { "treeId":treeId }
        else:
            raise ValueError("get_tree_branch requires at least one of treeId or branch.")

        req = requests.get(utils.TREEBRANCH_TEMPL % {'host': self.hosts[0][0],
                                                     'port': self.hosts[0][1]},
                           data = json.dumps(theData))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def get_tree_collisions(self, treeId, tsuids):
        """When processing a TSMeta, if the resulting leaf would overwrite an existing leaf with a different TSUID, a collision will be recorded. 
           This endpoint allows retreiving a list of the TSUIDs that were not included in a tree due to collisions. 
           It is useful for debugging in that if you find a TSUID in this list, you can pass it through the /tree/test endpoint to get details on why the collision occurred.
           Calling this endpoint without a list of one or more TSUIDs will return all collisions in the tree. 
           If you have a large number of timeseries in your system, the response can potentially be very large. Thus it is best to use this endpoint with specific TSUIDs.
           If storeFailures is diabled for the tree, this endpoint will not return any data. Collisions will still appear in the TSD's logs."""

        theData = { "treeId":treeId }
        thetsuids = ""
        for tsuid in tsuids:
            thetsuids += tsuid+","
        if len(tsuids)>0:
            thetsuids = thetsuids[:-1]
        theData["tsuids"]=thetsuids

        req = requests.get(utils.TREECOLL_TEMPL % {'host': self.hosts[0][0],
                                                   'port': self.hosts[0][1]},
                           data = json.dumps(theData))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def get_tree_notmatched(self, treeId, tsuids):
        """When processing a TSMeta, if the tree has strictMatch enabled and the meta fails to match on a rule in any level of the set, a not matched entry will be recorded. 
           This endpoint allows for retrieving the list of TSUIDs that failed to match a rule set. 
           It is useful for debugging in that if you find a TSUID in this list, you can pass it through the /tree/test endpoint to get details on why the meta failed to match.
           Calling this endpoint without a list of one or more TSUIDs will return all non-matched TSUIDs in the tree. 
           If you have a large number of timeseries in your system, the response can potentially be very large. Thus it is best to use this endpoint with specific TSUIDs.
           If storeFailures is diabled for the tree, this endpoint will not return any data. Not Matched entries will still appear in the TSD's logs."""
           
        theData = { "treeId":treeId }
        thetsuids = ""
        for tsuid in tsuids:
            thetsuids += tsuid+","
        if len(tsuids)>0:
            thetsuids = thetsuids[:-1]
        theData["tsuids"]=thetsuids

        req = requests.get(utils.TREEMATCH_TEMPL % {'host': self.hosts[0][0],
                                                    'port': self.hosts[0][1]},
                           data = json.dumps(theData))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err
        
    def test_tree(self, treeId, tsuids):
        """For debugging a rule set, the test endpoint can be used to run a TSMeta object through a tree's rules and determine where 
           in the heirarchy the leaf would appear. Or find out why a timeseries failed to match on a rule set or collided with an existing timeseries. 
           The only method supported is GET and no changes will be made to the actual tree in storage when using this endpoint."""

        theData = { "treeId":treeId }
        thetsuids = ""
        for tsuid in tsuids:
            thetsuids += tsuid+","
        if len(tsuids)>0:
            thetsuids = thetsuids[:-1]
        theData["tsuids"]=thetsuids

        req = requests.get(utils.TREETEST_TEMPL % {'host': self.hosts[0][0],
                                                   'port': self.hosts[0][1]},
                           data = json.dumps(theData))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def get_tree_rule(self, treeId, level=0, order=0):
        """Access to an individual tree rule. 
           Rules are addressed by their tree ID, level and order and all requests require these three parameters."""
        theData = { "treeId":treeId, "level":level, "order":order }

        req = requests.get(utils.TREERULE_TEMPL % {'host': self.hosts[0][0],
                                                   'port': self.hosts[0][1]},
                           data = json.dumps(theData))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def set_tree_rule(self, treeId, level=0, order=0, ruleType=None, description=None, notes=None, field=None, customField=None, regex=None, separator=None, regexGroupIdx=0, displayFormat=None):
        """allows for easy modification of a single rule in the set.
           You can create a new rule or edit an existing rule. New rules require a type value. 
           Existing trees require a valid treeId ID and any fields that require modification. 
           A successful request will return the modified rule object. 
           Note that if a rule exists at the given level and order, any changes will be merged with or overwrite the existing rule."""

        theData = { "treeId":treeId, "level":level, "order":order, "regexGroupIdx":regexGroupIdx }
        if ruleType is not None:
            if ruleType not in ["METRIC","METRIC_CUSTOM","TAGK","TAGK_CUSTOM","TAGV_CUSTOM"]:
                raise ValueError("unknown rule type")
            theData["type"]=ruleType
        if description is not None:
            if not isinstance(description, basestring):
                raise TypeError("set_tree_rule arg type mismatch")
            theData["description"]=description
        if notes is not None:
            if not isinstance(notes, basestring):
                raise TypeError("set_tree_rule arg type mismatch")
            theData["notes"]=notes
        if field is not None:
            if not isinstance(field, basestring):
                raise TypeError("set_tree_rule arg type mismatch")
            theData["field"]=field
        if customField is not None:
            if not isinstance(customField, basestring):
                raise TypeError("set_tree_rule arg type mismatch")
            theData["customField"]=customField
        if regex is not None:
            if not isinstance(regex, basestring):
                raise TypeError("set_tree_rule arg type mismatch")
            theData["regex"]=regex
        if separator is not None:
            if not isinstance(separator, basestring):
                raise TypeError("set_tree_rule arg type mismatch")
            theData["separator"]=separator
        if displayFormat is not None:
            if not isinstance(displayFormat, basestring):
                raise TypeError("set_tree_rule arg type mismatch")
            theData["displayFormat"]=displayFormat

        req = requests.post(utils.TREERULE_TEMPL % {'host': self.hosts[0][0],
                                                    'port': self.hosts[0][1]},
                            data = json.dumps(theData))

        err = checkErrors(req)
        if err is None:
            return req.json()
        else:
            return err

    def delete_tree_rule(self, treeId, level=0, order=0, deleteAll=False):
        """Using the DELETE method will remove a rule from a tree.
           If deleteAll is true, all rules from the tree will be deleted."""
        if deleteAll:
            theData = { "treeId":treeId }
        
            req = requests.delete(utils.TREERULES_TEMPL % {'host': self.hosts[0][0],
                                                           'port': self.hosts[0][1]},
                                  data = json.dumps(theData))
        else:
            theData = { "treeId":treeId, "level":level, "order":order }
        
            req = requests.delete(utils.TREERULE_TEMPL % {'host': self.hosts[0][0],
                                                          'port': self.hosts[0][1]},
                                  data = json.dumps(theData))

        err = checkErrors(req)
        return err


#    def _make_query(self, query, verb):
#        meth = getattr(requests, verb.lower(), None)
#        if meth is None:
#            pass
#        req = meth(utils.QUERY_TEMPL % {'host': self.hosts[0][0],
#                                        'port': self.hosts[0][1],
#                                        'query': query})
#        return req
#
#    def get_query(self, query):
#        return self._make_query(query, 'get')

#TODO: merge with the check for errors? or is the error function useless in the end?
    @staticmethod
    def process_response(resp):
        try:
            res = json.loads(resp.text)
        except Exception:
            raise opentsdbclient.OpenTSDBError(resp.text)

        if 'error' in res:
            raise opentsdbclient.OpenTSDBError(res['error'])

        return res
