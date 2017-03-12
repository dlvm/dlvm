#!/usr/bin/env python

import os
import json
from types import MethodType
from threading import Lock
import requests
import urllib
import logging


logger = logging.getLogger('dlvm_client')


class Layer1(object):

    api_format_file = 'api_format.json'

    def __init__(self, api_server_list):
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        full_path = os.path.join(curr_dir, self.api_format_file)
        with open(full_path) as f:
            self.api = json.load(f)
        assert(len(api_server_list) > 0)
        self.api_server_list = api_server_list
        self.pos = -1
        self.lock = Lock()
        logger.debug('api_server_list: %s', api_server_list)

    def _get_api_server(self):
        with self.lock:
            self.pos += 1
            self.pos %= len(self.api_server_list)
            api_server = self.api_server_list[self.pos]
            return 'http://{api_server}'.format(api_server=api_server)

    def __getattr__(self, func_name):
        print(func_name)
        name, method = func_name.split('_')
        path = self.api[name]['path']
        items = path.split('/')
        path_args = {}
        for item in items:
            if item[0] == '{' and item[-1] == '}':
                path_arg = item[1:-1]
                path_args[path_arg] = None

        def api_func(self, **kwargs):
            api_server = self._get_api_server()
            for path_arg in path_args:
                path_args[path_arg] = kwargs[path_arg]
                del kwargs[path_arg]
            api_path = path.format(**path_args)
            if method == 'get':
                if kwargs:
                    params = urllib.urlencode(kwargs)
                    url = '{api_server}/{api_path}?{params}'.format(
                        api_server=api_server,
                        api_path=api_path,
                        params=params,
                    )
                else:
                    url = '{api_server}/{api_path}'.format(
                        api_server=api_server,
                        api_path=api_path,
                    )
                headers = None
                data = None
            else:
                url = '{api_server}/{api_path}'.format(
                    api_server=api_server,
                    api_path=api_path,
                )
                headers = {'content-type': 'application/json'}
                data = json.dumps(kwargs)
            api_call = getattr(requests, method)
            rep = api_call(url, data=data, headers=headers, verify=False)
            rep.close()
            ret = {}
            ret['url'] = url
            ret['status_code'] = rep.status
            ret['json'] = rep.json()
            return ret
        api_func.__name__ = func_name
        setattr(self, func_name, MethodType(api_func, self, self.__class__))
        return getattr(self, name)
