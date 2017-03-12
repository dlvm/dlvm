#!/usr/bin/env python

import unittest
from mock import Mock, patch
from dlvm.client.layer1 import Layer1


class ClientLayer1Test(unittest.TestCase):

    @patch('dlvm.client.layer1.requests')
    def test_api_get(self, requests):
        rep_mock = Mock()
        rep_mock.status = 200
        rep_mock.json.return_value = 'success'
        get_mock = Mock()
        get_mock.return_value = rep_mock
        requests.get = get_mock
        layer1 = Layer1(['localhost:9521'])
        ret = layer1.dpvs_get()
        self.assertEqual(ret['status_code'], 200)

    @patch('dlvm.client.layer1.requests')
    def test_api_post(self, requests):
        rep_mock = Mock()
        rep_mock.status = 200
        rep_mock.json.return_value = 'success'
        post_mock = Mock()
        post_mock.return_value = rep_mock
        requests.post = post_mock
        layer1 = Layer1(['localhost:9521'])
        ret = layer1.dpvs_post()
        self.assertEqual(ret['status_code'], 200)

    def test_update_api_server_list(self):
        layer1 = Layer1(['host1:9521'])
        layer1.update_api_server_list(['host2:9521'])
        self.assertEqual(layer1.api_server_list, ['host2:9521'])
