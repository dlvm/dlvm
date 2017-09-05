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
    DpvError, IhostError, \
    DependenceCheckError
from dlvm.utils.helper import chunks
from modules import db, CloneJob
from handler import handle_dlvm_request, make_body, check_limit, \
    get_dm_context, dlv_get, \
    DpvClient, IhostClient, \
    obt_refresh, obt_encode, \
    dlv_delete_register, \
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
    if dst_dlv.status != 'attached':
        raise DstStatusError(dst_dlv.status)
    if src_dlv.fjs:
        raise SrcFjError
    if src_dlv.ej:
        raise SrcEjError

    target_id = cj.snapshot.thin_id
    thin_id_list = []
    for snapshot in src_dlv.snapshots:
        thin_id_list.append(snapshot.thin_id)

    leg_dict = {}
    for group in src_dlv.groups[1:]:
        for leg in group.legs:
            src_client = DpvClient(leg.dpv_name)
            src_client.cj_leg_export(
                leg.leg_id,
                obt_encode(obt),
                cj.cj_name,
                dst_dlv.dlv_name,
                str(leg.leg_size),
            )
            key = '%s-%s' % (group.idx, leg.idx)
            leg_dict[key] = leg.leg_id
    for group in dst_dlv.groups[1:]:
        for leg in group.legs:
            key = '%s-%s' % (group.idx, leg.idx)
            dst_client = DpvClient(leg.dpv_name)
            dst_client.cj_login(
                leg.leg_id,
                obt_encode(obt),
                cj.cj_name,
                src_dlv.dlv_name,
                leg_dict[key],
                str(leg.leg_size),
            )
    src_info = get_dlv_info(src_dlv)
    dst_info = get_dlv_info(dst_dlv)
    dm_context = get_dm_context()
    ihost_client = IhostClient(src_dlv.ihost_name)
    try:
        ihost_client.dlv_suspend(
            src_dlv.dlv_name,
            obt_encode(obt),
            src_info,
        )

        bm_dict = ihost_client.bm_get(
            src_dlv.dlv_name,
            obt_encode(obt),
            src_info,
            'all',
            [],
        )

        ihost_client.metadata_copy(
            src_dlv.dlv_name,
            dst_dlv.dlv_name,
            obt_encode(obt),
            target_id,
            thin_id_list,
            dst_info,
        )
        for group in dst_dlv.groups[1:]:
            leg_list = []
            for leg in group.legs:
                ileg = {}
                ileg['dpv_name'] = leg.dpv_name
                ileg['leg_id'] = leg.leg_id
                ileg['idx'] = leg.idx
                ileg['leg_size'] = leg.leg_size
                leg_list.append(ileg)
            leg_list.sort(key=lambda x: x['idx'])
            for (ileg0, ileg1) in chunks(leg_list, 2):
                bm_key = '%s-%s' % (ileg0['leg_id'], ileg1['leg_id'])
                bm = bm_dict[bm_key]
                for ileg in (ileg0, ileg1):
                    key = '%s-%s' % (group.idx, ileg['idx'])
                    dst_client = DpvClient(ileg['dpv_name'])
                    dst_client.cj_mirror_start(
                        ileg['leg_id'],
                        obt_encode(obt),
                        cj.cj_name,
                        src_dlv.dlv_name,
                        leg_dict[key],
                        str(ileg['leg_size']),
                        dm_context,
                        bm,
                    )
    finally:
        ihost_client.dlv_resume(
            src_dlv.dlv_name,
            obt_encode(obt),
            src_info,
        )


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
    snap_name = '%s/%s' % (src_name, args['snap_name'])
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
        snap_name=snap_name,
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


cj_get_parser = reqparse.RequestParser()
cj_get_parser.add_argument(
    'with_process',
    type=str,
    choices=('true', 'false'),
    default='False',
    location='args',
)


cj_fields = OrderedDict()
cj_fields['cj_name'] = fields.String
cj_fields['timestamp'] = fields.DateTime
cj_fields['status'] = fields.String
cj_fields['snap_name'] = SnapName(attribute='snap_name')


def get_process_status(cj):
    pass


def handle_cj_get(params, args):
    cj_name = params[0]
    try:
        cj = CloneJob \
            .query \
            .with_lockmode('update') \
            .filter_by(cj_name=cj_name) \
            .one()
    except NoResultFound:
        return make_body('not_exist'), 404
    if args['with_process'] == 'true':
        process = get_process_status(cj)
    else:
        process = {}
    cj.process = process
    body = marshal(cj, cj_fields)
    return body, 200


class Cj(Resource):

    def get(self, cj_name):
        return handle_dlvm_request(
            [cj_name],
            cj_get_parser,
            handle_cj_get,
        )

    # def put(self, cj_name):
    #     return handle_dlvm_request(
    #         [cj_name],
    #         cj_put_parser,
    #         handle_cj_put,
    #     )

    # def delete(self, cj_name):
    #     return handle_dlvm_request(
    #         [cj_name],
    #         cj_delete_parser,
    #         handle_cj_delete,
    #     )


@dlv_delete_register
def cj_check_for_dlv_delete(dlv):
    for cj in dlv.src_cjs:
        msg = 'src_cj: %s' % cj.cj_name
        raise DependenceCheckError(msg)
    if dlv.dst_cj_name is not None:
        msg = 'dst_cj: %s' % dlv.dst_cj_name
        raise DependenceCheckError(msg)
