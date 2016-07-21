# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-07-21 11:12:02
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-07-21 11:24:04

import arg_parser
import urllib3
import sys
from elasticsearch import Elasticsearch
import json
import getopt

urllib3.disable_warnings()


class DIGESDG(object):

    def __init__(self, token):
        self.username, self.password = token.split(':')

    def generate(self):



if __name__ == '__main__':
    import sys
    import argparse

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-t','--token', required=True)

    args = arg_parser.parse_args()

    print args.token


