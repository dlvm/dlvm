import unittest
import json

from dlvm.api_server import app


class RootTest(unittest.TestCase):

    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()

    def test_root_get(self):
        resp = self.client.get('/')
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data['message'], 'succeed')
        self.assertEqual(data['data'], 'dlvm')
