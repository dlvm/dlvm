#!/usr/bin/env python

from collections import OrderedDict
import logging
import datetime
from flask_restful import reqparse, Resource, fields, marshal
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.configure import conf
from dlvm.utils.error import CjStatusError, \
    SrcStatusError, DstStatusError, \
    SrcFjError, SrcEjError, \
    DpvError, IhostError
from modules import db, CloneJob
from handler import handle_dlvm_request, make_body, check_limit, \
    get_dm_context, dlv_get, \
    DpvClient, IhostClient, \
    obt_refresh, obt_encode, \
    dlv_detach_register, dlv_delete_register, \
    get_dlv_info


logger = logging.getLogger('dlvm_api')


class SnapName(fields.Raw):
    def format(self, value):
        return value.split('/')[1]


cj_summary_fields = OrderedDict()
cj_summary_fields['cj_name'] = fields.String
cj_summary_fields['timestamp'] = fields.DateTime
cj_summary_fields['status'] = fields.String
cj_summary_fields['snap_name'] = SnapName(attribute='snap_name')
cjs_get_fields = OrderedDict()
cjs_get_fields['cjs'] = fields.List(
    fields.Nested(cj_summary_fields))


cjs_get_parser = reqparse.RequestParser()
cjs_get_parser.add_argument(
    'prev',
    type=str,
    location='args',
)
cjs_get_parser.add_argument(
    'limit',
    type=check_limit(conf.cj_list_limit),
    default=conf.cj_list_limit,
    location='args',
)
cjs_get_parser.add_argument(
    'order_by',
    type=str,
    choices=(
        'cj_name',
        'timestamp',
    ),
    default='cj_name',
    location='args',
)
cjs_get_parser.add_argument(
    'reverse',
    type=str,
    choices=('true', 'false'),
    default='false',
    location='args',
)
cjs_get_parser.add_argument(
    'status',
    type=str,
    choices=(
        'creating',
        'create_failed',
        'canceling',
        'cancel_failed',
        'processing',
        'finishing',
        'finish_failed',
    ),
    location='args',
)


def handle_cjs_get(params, args):
    order_field = getattr(CloneJob, args['order_by'])
    prev = args['prev']
    if args['reverse'] == 'true':
        query = CloneJob.query.order_by(order_field.desc())
        if prev:
            query = query.filter(order_field < prev)
    else:
        query = CloneJob.query.order_by(order_field)
        if prev:
            query = query.filter(order_field > prev)
    if args['status']:
        query = query.filter_by(status=args['status'])
    query = query.limit(args['limit'])
    cjs = query.all()
    body = marshal({'cjs': cjs}, cjs_get_fields)
    return body['cjs'], 200


cjs_post_parser = reqparse.RequestParser()
cjs_post_parser.add_argument(
    'cj_name',
    type=str,
    required=True,
    location='json',
)
cjs_post_parser.add_argument(
    'src_name',
    type=str,
    required=True,
    location='json',
)
cjs_post_parser.add_argument(
    'dst_name',
    type=str,
    required=True,
    location='json',
)
cjs_post_parser.add_argument(
    'snap_name',
    type=str,
    required=True,
    location='json',
)
cjs_post_parser.add_argument(
    't_id',
    type=str,
    required=True,
    location='json',
)
cjs_post_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
cjs_post_parser.add_argument(
    't_stage',
    type=int,
    required=True,
    location='json',
)


def do_cj_create(cj, src_dlv, dst_dlv, obt):
    if cj.status != 'creating':
        raise CjStatusError(cj.status)
    if src_dlv.status != 'attached':
        raise SrcStatusError(src_dlv.status)
    if dst_dlv.status != 'detached':
        raise DstStatusError(dst_dlv.status)
    if src_dlv.fjs:
        raise SrcFjError
    if src_dlv.ej:
        raise SrcEjError


def cj_create(cj, src_dlv, dst_dlv, obt):
    try:
        do_cj_create(cj, src_dlv, dst_dlv, obt)
    except CjStatusError as e:
        return make_body('invalid_cj_status', e.message), 400
    except SrcStatusError as e:
        cj.status = 'create_failed'
        cj.timestamp = datetime.datetime.utcnow()
        db.session.add(cj)
        db.session.commit()
        return make_body('src_status_error', e.message), 400
    except DstStatusError as e:
        cj.status = 'create_failed'
        cj.timestamp = datetime.datetime.utcnow()
        db.session.add(cj)
        db.session.commit()
        return make_body('dst_status_error', e.message), 400
    except SrcFjError:
        cj.status = 'create_failed'
        cj.timestamp = datetime.datetime.utcnow()
        db.session.add(cj)
        db.session.commit()
        return make_body('src_has_fj'), 400
    except SrcEjError:
        cj.status = 'create_failed'
        cj.timestamp = datetime.datetime.utcnow()
        db.session.add(cj)
        db.session.commit()
        return make_body('src_has_ej'), 400
    except IhostError as e:
        cj.status = 'create_failed'
        cj.timestamp = datetime.datetime.utcnow()
        db.session.add(cj)
        obt_refresh(obt)
        db.session.commit()
        return make_body('ihost_failed', e.message), 500
    except DpvError as e:
        cj.status = 'create_failed'
        cj.timestamp = datetime.datetime.utcnow()
        db.session.add(cj)
        obt_refresh(obt)
        db.session.commit()
        return make_body('dpv_failed', e.message), 500
    else:
        cj.status = 'processing'
        cj.timestamp = datetime.datetime.utcnow()
        db.session.add(cj)
        obt_refresh(obt)
        db.session.commit()
        return make_body('success'), 200


def handle_cjs_post(params, args):
    cj_name = args['cj_name']
    src_name = args['src_name']
    dst_name = args['dst_name']
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    src_dlv, obt = dlv_get(src_name, t_id, t_owner, t_stage)
    dst_dlv, obt = dlv_get(dst_name, t_id, t_owner, t_stage)
    cj = CloneJob(
        cj_name=cj_name,
        status='creating',
        timestamp=datetime.datetime.utcnow(),
        src_dlv_name=src_name,
        dst_dlv_name=dst_name,
    )
    db.session.add(cj)
    obt_refresh(obt)
    db.session.commit()
    return cj_create(cj, src_dlv, dst_dlv, obt)


class Cjs(Resource):

    def get(self):
        return handle_dlvm_request(None, cjs_get_parser, handle_cjs_get)

    def post(self):
        return handle_dlvm_request(None, cjs_post_parser, handle_cjs_post)
