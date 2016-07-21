# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-07-21 11:12:02
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-07-21 11:44:56

import urllib3
import sys
from elasticsearch import Elasticsearch
import json
import getopt

urllib3.disable_warnings()

######################################################################
#   Constant
######################################################################


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
                "exists": {
                    "field": "extractions.text"
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
        self.cdr = 'https://' + token + '@els.istresearch.com:19200/memex-domains'
        self.es = Elasticsearch(self.cdr)

    def load_sites(self):
        buckets = self.es.search(index='escorts', body=sites_query)['aggregations']['by-posttime']['posttime']['buckets']
        sites = map(lambda x: x['key'], buckets)
        return sites


    def generate(self):
        print self.load_sites()
        # print ", ".join(sites)



if __name__ == '__main__':
    import sys
    import argparse

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-t','--token', required=True)

    args = arg_parser.parse_args()

    dg = DIGESDG(args.token)     

    print dg.generate()


