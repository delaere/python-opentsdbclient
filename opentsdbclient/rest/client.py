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
from opentsdbclient import base
from opentsdbclient.rest import utils
from opentsdberrors import checkErrors

#TODO: missing API endpoints:
#/api/tree - for advanced uses

#TODO: add support for compression of the json content, at least in put.

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
