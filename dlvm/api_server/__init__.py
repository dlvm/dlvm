from flask import Flask

from dlvm.common.loginit import loginit
from dlvm.hook.api_wrapper import Api
from dlvm.api_server.root import root_res
from dlvm.api_server.dpv import dpvs_res


loginit()


app = Flask(__name__)
api = Api(app)

api.add_resource(root_res)
api.add_resource(dpvs_res)
