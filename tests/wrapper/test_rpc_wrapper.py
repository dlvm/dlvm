import unittest
import time
from multiprocessing import Process
import uuid
from logging import getLogger, LoggerAdapter

from marshmallow import fields

from dlvm.common.utils import RequestContext
from dlvm.common.marshmallow_ext import NtSchema
from dlvm.wrapper.rpc_wrapper import DlvmRpc
from dlvm.wrapper.local_ctx import frontend_local, get_empty_worker_ctx


server_logger = getLogger('server_logger')
client_logger = getLogger('client_logger')
dlvm_rpc = DlvmRpc('localhost', 9522, server_logger)


class AddArgSchema(NtSchema):
    a = fields.Integer()
    b = fields.Integer()


class AddRetSchema(NtSchema):
    the_sum = fields.Integer()


class DlvmRpcTest(unittest.TestCase):

    def setUp(self):

        @dlvm_rpc.register(arg_schema=AddArgSchema, ret_schema=AddRetSchema)
        def add(arg):
            return AddRetSchema.nt(arg.a + arg.b)

        @dlvm_rpc.register()
        def no_data():
            pass

        def start_server():
            dlvm_rpc.start_server()

        self.p = Process(target=start_server)
        self.p.start()
        req_id = uuid.uuid4()
        logger = LoggerAdapter(client_logger, {'req_id': req_id})
        frontend_local.req_ctx = RequestContext(req_id, logger)
        frontend_local.worker_ctx = get_empty_worker_ctx()
        time.sleep(1)

    def tearDown(self):
        self.p.terminate()
        self.p.join()

    def test_sync_client(self):
        client = dlvm_rpc.sync_client(
            frontend_local.req_ctx, 'localhost', 9522, 3,
            frontend_local.worker_ctx.lock_dt)
        arg = AddArgSchema.nt(2, 3)
        ret = client.add(arg)
        self.assertEqual(ret.the_sum, 5)

    def test_async_client(self):
        client = dlvm_rpc.async_client(
            frontend_local.req_ctx, 'localhost', 9522, 3,
            frontend_local.worker_ctx.lock_dt, None)
        client.no_data()
