# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-07-21 11:12:02
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-07-21 21:59:58

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
    "size": 1
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

    def load_data_by_site(self, site_name):
        try:
            # print site_name
            search_query['query']['filtered']['filter']['bool']['must'][0]['term']['extractions.text.attribs.target'] = site_name
            return search_query['query']
            
            # print json.dumps(search_query, indent=4)
            # buckets = self.es.search(index='escorts', body=search_query)['hits']['hits']
        except Exception as e:
            raise Exception('site_name is incorrect')
        return 'ss'
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

    def generate(self):
        site_name = 'backpage'
        # print self.load_sites()
        return self.load_data_by_site(site_name)
        # print ", ".join(sites)



if __name__ == '__main__':
    import sys
    import argparse

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-t','--token', required=True)

    args = arg_parser.parse_args()

    dg = DIGESDG(args.token)     

    print dg.generate()


