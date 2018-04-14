from flask import Flask

from dlvm.hook.api_wrapper import Api
from dlvm.api_server.root import root_res
from dlvm.api_server.dpv import dpvs_res, dpv_res, dpv_update_res


app = Flask(__name__)
api = Api(app)

api.add_resource(root_res)
api.add_resource(dpvs_res)
api.add_resource(dpv_res)
api.add_resource(dpv_update_res)
