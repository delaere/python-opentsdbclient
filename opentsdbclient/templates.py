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

STATS_TEMPL = 'http://%(host)s:%(port)s/api/stats'
PUT_TEMPL = 'http://%(host)s:%(port)s/api/put%(options)s'
CONF_TEMPL = 'http://%(host)s:%(port)s/api/config'
FILT_TEMPL = 'http://%(host)s:%(port)s/api/config/filters'
AGGR_TEMPL = 'http://%(host)s:%(port)s/api/aggregators'
VERSION_TEMPL = 'http://%(host)s:%(port)s/api/version'
ANNOT_TEMPL = 'http://%(host)s:%(port)s/api/annotation'
DCACH_TEMPL = 'http://%(host)s:%(port)s/api/dropcaches'
SERIAL_TEMPL = 'http://%(host)s:%(port)s/api/serializers'
SUGGEST_TEMPL = 'http://%(host)s:%(port)s/api/suggest'
QUERY_TEMPL = 'http://%(host)s:%(port)s/api/query'
EXPQUERY_TEMPL = 'http://%(host)s:%(port)s/api/query/exp' 
QUERYLST_TEMPL = 'http://%(host)s:%(port)s/api/query/last'
SEARCH_TEMPL = 'http://%(host)s:%(port)s%(endpoint)s'
ASSIGNUID_TEMPL = 'http://%(host)s:%(port)s/api/uid/assign'
TSMETA_TEMPL = 'http://%(host)s:%(port)s/api/uid/tsmeta'
UIDMETA_TEMPL = 'http://%(host)s:%(port)s/api/uid/uidmeta'
TREE_TEMPL = 'http://%(host)s:%(port)s/api/tree'
TREETEST_TEMPL = 'http://%(host)s:%(port)s/api/tree/test'
TREECOLL_TEMPL = 'http://%(host)s:%(port)s/api/tree/collisions'
TREEMATCH_TEMPL = 'http://%(host)s:%(port)s/api/tree/notmatched/notmatched'
TREEBRANCH_TEMPL = 'http://%(host)s:%(port)s/api/tree/branch'
TREERULE_TEMPL = 'http://%(host)s:%(port)s/api/tree/rule'
TREERULES_TEMPL  ='http://%(host)s:%(port)s/api/tree/rules'
