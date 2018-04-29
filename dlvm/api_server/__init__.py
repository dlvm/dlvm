from flask import Flask

from dlvm.wrapper.api_wrapper import Api
from dlvm.api_server.root import root_res
from dlvm.api_server.dpv import dpvs_res, dpv_res, dpv_update_res
from dlvm.api_server.dvg import dvgs_res, dvg_res, dvg_extend_res, \
    dvg_reduce_res


app = Flask(__name__)
api = Api(app)

api.add_resource(root_res)
api.add_resource(dpvs_res)
api.add_resource(dpv_res)
api.add_resource(dpv_update_res)
api.add_resource(dvgs_res)
api.add_resource(dvg_res)
api.add_resource(dvg_extend_res)
api.add_resource(dvg_reduce_res)
