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
#/api/config/filters - simple... could also be used to prepare the query checks 
#/api/search
#/api/uid - retention is a special case of this
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
        meta_data = {'tsuid': tsuid, 'retention': retention_days}
        req = requests.post(utils.META_TEMPL % {'host': self.hosts[0][0],
                                                'port': self.hosts[0][1],
                                                'tsuid': tsuid},
                            data=json.dumps(meta_data))
        return req

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

    def get_version(self):
        """Used to check OpenTSDB version.

        That might be needed in case of unknown bugs - this code is written
        only for the 2.x REST API version, so some of the failures might refer
        to the wrong OpenTSDB version installed.
        """
        req = requests.get(utils.VERSION_TEMPL % {'host': self.hosts[0][0],
                                                  'port': self.hosts[0][1]})
        return req

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
