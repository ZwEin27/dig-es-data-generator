# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-07-21 11:12:02
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-07-22 08:50:21

import urllib3
import re
import sys
from elasticsearch import Elasticsearch
import json
import getopt
import hashlib

urllib3.disable_warnings()

######################################################################
#   Constant
######################################################################

######################################################################
#   Regular Expression
######################################################################

re_tokenize = re.compile(r'[\s!\"#\$%&\'\(\)\*\+,\-\./:;<=>\?@\[\\\]\^_`{|}~]')

######################################################################
#   Query
######################################################################

sites_query = {
    "aggs": {
        "by-posttime": {
            "filter": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "version": "2.0"
                            }
                        },
                        {
                            "exists": {
                                "field": "extractions.text"
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "posttime": {
                    "terms": {
                        "field": "extractions.text.attribs.target",
                        "size": 1000
                    }
                }
            }
        }
    },
    "size": 0
}

search_query = {
    "query": {
        "filtered": {
            "filter": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "extractions.text.attribs.target": ""
                            }
                        },
                        {
                            "exists": {
                                "field": "extractions"
                            }
                        },
                        {
                            "exists": {
                                "field": "extractions.text.results"
                            }
                        }
                    ]
                }
            },
            "query": {
                "match": {
                    "extractions.text.results": "blvd"
                }
            }
        }
    },
    "_source": [
        "extractions.text.results"
    ],
    "size": 500
}



######################################################################
#   Main Class
######################################################################

class DIGESDG(object):

    def __init__(self, token):
        # self.username, self.password = token.split(':')
        self.cdr = 'https://' + token + '@cdr-es.istresearch.com:9200/memex-domains'
        self.es = Elasticsearch([self.cdr])

    def load_sites(self):
        buckets = self.es.search(index='escorts', body=sites_query)['aggregations']['by-posttime']['posttime']['buckets']
        sites = map(lambda x: x['key'], buckets)
        return sites

    def load_data(self, site_name, keyword):
        # load data for specifc site name
        try:
            search_query['query']['filtered']['filter']['bool']['must'][0]['term']['extractions.text.attribs.target'] = site_name
            search_query['query']['filtered']['query']['match']['extractions.text.results'] = keyword
            buckets = self.es.search(index='escorts', body=search_query)['hits']['hits']
        except Exception as e:
            raise Exception('site_name is incorrect')

        # load fetched data
        ans = []
        for bucket in buckets:
            try:
                text = bucket['_source']['extractions']['text']['results']
                if not isinstance(text, basestring):
                    text = ' '.join(text)
            except Exception as e:
                continue
            else:
                ans.append(text)
        return ans

    def dedup_data(self, data_lines):

        def clean(data):
            return ' '.join(sorted([_.strip() for _ in re_tokenize.split(data) if _.strip() != '']))

        def hash(data):
            return hashlib.sha224(clean(data).encode('ascii', 'ignore')).hexdigest()#+hashlib.sha256(data).hexdigest()+hashlib.md5(data).hexdigest()

        # clean
        # data_lines = [clean(_) for _ in data_lines]

        # dedup
        dedup = {}
        for data in data_lines:
            dedup[hash(data)] = data
        data_lines = dedup.values()

        return data_lines

    def generate(self, keywords=['ave','blvd','street','st','avenue','rd','boulevard','parkway','pkwy'], num_data=200):
        ans = []
        sites = self.load_sites()
        for site_name in sites:
            data = []
            for keyword in keywords:
                data += self.load_data(site_name, keyword)
            # data = data[:num_data]
            data = self.dedup_data(data)[:num_data]
            ans += data
        return ans


if __name__ == '__main__':
    import sys
    import argparse

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-t','--token', required=True)

    args = arg_parser.parse_args()

    dg = DIGESDG(args.token)  
       
    print dg.generate()


