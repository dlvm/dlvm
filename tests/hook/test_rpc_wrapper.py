import unittest
import logging
import time
from multiprocessing import Process
import uuid

from marshmallow import Schema, fields

from dlvm.common.utils import RequestContext
from dlvm.hook.rpc_wrapper import Rpc


class Args():

    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2


class Ret():

    def __init__(self, arg3, arg4):
        self.arg3 = arg3
        self.arg4 = arg4


class ArgsSchema(Schema):
    arg1 = fields.Integer()
    arg2 = fields.Integer()


class RetSchema(Schema):
    arg3 = fields.Integer()
    arg4 = fields.Integer()


class DlvmRpcTest(unittest.TestCase):

    def setUp(self):

        logger = logging.getLogger('rpc_server_logger')
        rpc = Rpc('localhost', 9522, logger)

        @rpc.rpc(ArgsSchema, RetSchema)
        def func(req_ctx, args):
            arg3 = args['arg1'] + args['arg2']
            arg4 = args['arg1'] - args['arg2']
            return Ret(arg3, arg4)

        def start_rpc_server():
            rpc.start_server()

        self.rpc = rpc
        self.p = Process(target=start_rpc_server)
        self.p.start()
        time.sleep(1)

    def tearDown(self):
        self.p.terminate()
        self.p.join()

    def test_rpc(self):
        arg1 = 7
        arg2 = 5
        arg = Args(arg1, arg2)
        req_id = uuid.uuid4()
        logger = logging.getLogger('rpc_client_logger')
        req_ctx = RequestContext(req_id, logger)
        t = self.rpc.async_call(
            req_ctx, 'localhost', 9522, 300,
            'func', 0, arg)
        ret = t.get_value()
        self.assertEqual(ret['arg3'], arg1+arg2)
        self.assertEqual(ret['arg4'], arg1-arg2)
