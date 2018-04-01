from typing import Optional, Mapping, Tuple, MutableMapping, \
    List, cast
from collections import OrderedDict
import copy

from flask import Flask
from flask_restful import Api, Resource, reqparse, fields

from dlvm.common.utils import RequestContext, WorkContext
from dlvm.common.configure import cfg
from dlvm.common.error import LimitExceedError
from dlvm.common.loginit import loginit
from dlvm.hook.api_wrapper import handle_dlvm_api
from dlvm.core.modules import FieldType, DistributeLogicalVolume
from dlvm.core.helper import create_all
from dlvm.core.dpv import dpv_list, dpv_create


dpv_fields: MutableMapping = OrderedDict()
dpv_fields['dpv_name'] = fields.String
dpv_fields['total_size'] = fields.Integer
dpv_fields['free_size'] = fields.Integer
dpv_fields['status'] = fields.String
dpv_fields['dvg_name'] = fields.String
dpv_fields['lock_id'] = fields.String
dpv_fields['lock_timestamp'] = fields.Integer

dvg_fields: MutableMapping = OrderedDict()
dvg_fields['dvg_name'] = fields.String
dvg_fields['total_size'] = fields.Integer
dvg_fields['free_size'] = fields.Integer

group_fields: MutableMapping = OrderedDict()
group_fields['group_id'] = fields.String
group_fields['idx'] = fields.Integer
group_fields['group_size'] = fields.Integer
group_fields['dlv_name'] = fields.String

leg_fields: MutableMapping = OrderedDict()
leg_fields['leg_id'] = fields.String
leg_fields['idx'] = fields.Integer
leg_fields['leg_size'] = fields.Integer
leg_fields['dpv_name'] = fields.String


def root_get(
        req_ctx: RequestContext, wrok_ctx: WorkContext,
        params: Optional[Mapping], kwargs: Optional[Mapping])-> str:
    return 'dlvm_api'


class Root(Resource):

    def get(self)-> Tuple[OrderedDict, int]:
        return handle_dlvm_api(
            root_get, 200, None, None, {})


dpvs_get_parser = reqparse.RequestParser()
dpvs_get_parser.add_argument(
    'order_by',
    type=str,
    choices=(
        'dpv_name',
        'total_size',
        'free_size',
        'lock_timestamp',
    ),
    default='dpv_name',
    location='args',
)
dpvs_get_parser.add_argument(
    'reverse',
    type=str,
    choices=('true', 'false'),
    default='false',
    location='args',
)
dpvs_get_parser.add_argument(
    'offset',
    type=int,
    location='args',
)
dpvs_get_parser.add_argument(
    'limit',
    type=int,
    default=cfg.list_limit,
    location='args',
)
dpvs_get_parser.add_argument(
    'status',
    type=str,
    choices=('available', 'unavailable'),
    location='args',
)
dpvs_get_parser.add_argument(
    'locked',
    type=str,
    choices=('true', 'false'),
    location='args',
)
dpvs_get_parser.add_argument(
    'dvg_name',
    type=str,
    location='args',
)

dpv_summary_fields = copy.deepcopy(dpv_fields)

dpvs_post_parser = reqparse.RequestParser()
dpvs_post_parser.add_argument(
    'dpv_name',
    type=str,
    required=True,
    location='json'
)


def handle_dpvs_get(
        req_ctx: RequestContext,
        work_ctx: WorkContext,
        params: Mapping[str, FieldType],
        kwargs: Mapping[str, str],
)-> List[DistributeLogicalVolume]:
    order_by = params['order_by']
    if params['reverse'] == 'true':
        reverse = True
    else:
        reverse = False
    offset = params['offset']
    limit: int = cast(int, params['limit'])
    if limit > cfg.list_limit:
        raise LimitExceedError(limit, cfg.list_limit)
    status = params['status']
    if params['locked'] == 'true':
        locked = True
    else:
        locked = False
    dvg_name = params['dvg_name']
    return dpv_list(
        req_ctx, work_ctx,
        order_by, reverse,
        offset, limit,
        status, locked, dvg_name)


def handle_dpvs_post(
        req_ctx: RequestContext,
        work_ctx: WorkContext,
        Params: Mapping[str, FieldType],
        kwargs: Mapping[str, str],
)-> None:
    dpv_name = kwargs['dpv_name']
    dpv_create(dpv_name)
    return None


class Dpvs(Resource):

    def get(self)-> Tuple[OrderedDict, int]:
        return handle_dlvm_api(
            handle_dpvs_get, 200, dpvs_get_parser, dpv_summary_fields, {})

    def post(self, dpv_name: str)-> Tuple[OrderedDict, int]:
        kwargs = {'dpv_name': dpv_name}
        return handle_dlvm_api(
            handle_dpvs_post, 200, dpvs_post_parser, None, kwargs)


def create_app()-> Flask:
    loginit()
    create_all()
    app = Flask(__name__)
    api = Api(app)
    api.add_resource(Root, '/')
    api.add_resource(Dpvs, '/dpvs')
    return app


app = create_app()
