#!/usr/bin/env python

import uuid
from collections import OrderedDict
import logging
import random
import datetime
from flask_restful import reqparse, Resource, fields, marshal
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.configure import conf
from dlvm.utils.constant import dpv_search_overhead
from dlvm.utils.error import NoEnoughDpvError, DpvError, \
    DlvStatusError, \
    IhostError, FjStatusError, \
    DependenceCheckError
from modules import db, \
    DistributePhysicalVolume, DistributeVolumeGroup, \
    Leg, FailoverJob
from handler import handle_dlvm_request, make_body, check_limit, \
    get_dm_context, dlv_get, \
    DpvClient, IhostClient, \
    obt_refresh, obt_encode, \
    dlv_detach_register, dlv_delete_register, \
    get_dlv_info


logger = logging.getLogger('dlvm_api')


fj_summary_fields = OrderedDict()
fj_summary_fields['fj_name'] = fields.String
fj_summary_fields['timestamp'] = fields.DateTime
fj_summary_fields['status'] = fields.String
fj_summary_fields['dlv_name'] = fields.String
fjs_get_fields = OrderedDict()
fjs_get_fields['fjs'] = fields.List(
    fields.Nested(fj_summary_fields))

fjs_get_parser = reqparse.RequestParser()
fjs_get_parser.add_argument(
    'prev',
    type=str,
    location='args',
)
fjs_get_parser.add_argument(
    'limit',
    type=check_limit(conf.fj_list_limit),
    default=conf.fj_list_limit,
    location='args',
)
fjs_get_parser.add_argument(
    'order_by',
    type=str,
    choices=(
        'fj_name',
        'timestamp',
        'dlv_name',
    ),
    default='fj_name',
    location='args',
)
fjs_get_parser.add_argument(
    'reverse',
    type=str,
    choices=('true', 'false'),
    default='false',
    location='args',
)
fjs_get_parser.add_argument(
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


def handle_fjs_get(params, args):
    order_field = getattr(FailoverJob, args['order_by'])
    prev = args['prev']
    if args['reverse'] == 'true':
        query = FailoverJob.query.order_by(order_field.desc())
        if prev:
            query = query.filter(order_field < prev)
    else:
        query = FailoverJob.query.order_by(order_field)
        if prev:
            query = query.filter(order_field > prev)
    if args['status']:
        query = query.filter_by(status=args['status'])
    query = query.limit(args['limit'])
    fjs = query.all()
    body = marshal({'fjs': fjs}, fjs_get_fields)
    return body['fjs'], 200


def get_fj_legs(fj):
    src_leg = None
    dst_leg = None
    ori_leg = None
    for leg in fj.legs:
        if leg.fj_role == 'src':
            src_leg = leg
        elif leg.fj_role == 'dst':
            dst_leg = leg
        elif leg.fj_role == 'ori':
            ori_leg = leg
        else:
            assert(False)
    assert(src_leg is not None)
    assert(dst_leg is not None)
    assert(ori_leg is not None)
    if ori_leg.group_id is not None:
        assert(dst_leg.group_id is None)
        assert(src_leg.group_id == ori_leg.group_id)
    if dst_leg.group_id is not None:
        assert(ori_leg.group_id is None)
        assert(src_leg.group_id == dst_leg.group_id)
    return src_leg, dst_leg, ori_leg


fjs_post_parser = reqparse.RequestParser()
fjs_post_parser.add_argument(
    'fj_name',
    type=str,
    required=True,
    location='json',
)
fjs_post_parser.add_argument(
    'dlv_name',
    type=str,
    required=True,
    location='json',
)
fjs_post_parser.add_argument(
    'ori_id',
    type=str,
    required=True,
    location='json',
)
fjs_post_parser.add_argument(
    't_id',
    type=str,
    required=True,
    location='json',
)
fjs_post_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
fjs_post_parser.add_argument(
    't_stage',
    type=int,
    required=True,
    location='json',
)


def select_dpvs(dvg_name, required_size, batch_count, offset):
    dpvs = DistributePhysicalVolume \
        .query \
        .filter_by(dvg_name=dvg_name) \
        .filter_by(status='available') \
        .filter(DistributePhysicalVolume.free_size > required_size) \
        .order_by(DistributePhysicalVolume.free_size.desc()) \
        .limit(batch_count) \
        .offset(offset) \
        .with_entities(DistributePhysicalVolume.dpv_name) \
        .all()
    random.shuffle(dpvs)
    return dpvs


def allocate_dpv(leg, group, dvg_name, obt):
    dpvs = []
    batch_count = dpv_search_overhead
    exclude_name_set = set()
    if conf.test_mode is not True:
        for ileg in group.legs:
            assert(ileg.dpv_name not in exclude_name_set)
            exclude_name_set.add(ileg.dpv_name)
    while True:
        if len(dpvs) == 0:
            dpvs = select_dpvs(
                dvg_name, leg.leg_size, batch_count, 0)
            if len(dpvs) == 0:
                logger.warning(
                    'allocate dpv failed, %s %s %d',
                    leg.fj.fj_name, dvg_name, leg.leg_size)
                raise NoEnoughDpvError()
        dpv = dpvs.pop()
        if dpv.dpv_name in exclude_name_set:
            continue
        dpv = DistributePhysicalVolume \
            .query \
            .with_lockmode('update') \
            .filter_by(dpv_name=dpv.dpv_name) \
            .one()
        if dpv.status != 'available':
            continue
        if dpv.free_size < leg.leg_size:
            continue
        dpv.free_size -= leg.leg_size
        leg.dpv = dpv
        db.session.add(dpv)
        db.session.add(leg)
        break

    dvg = DistributeVolumeGroup \
        .query \
        .with_lockmode('update') \
        .filter_by(dvg_name=dvg_name) \
        .one()
    assert(dvg.free_size >= leg.leg_size)
    dvg.free_size -= leg.leg_size
    db.session.add(dvg)
    obt_refresh(obt)
    db.session.commit()


def do_fj_create(fj, dlv, obt):
    src_leg, dst_leg, ori_leg = get_fj_legs(fj)
    if dst_leg.dpv is None:
        allocate_dpv(dst_leg, ori_leg.group, dlv.dvg_name, obt)
    dm_context = get_dm_context()
    dst_client = DpvClient(dst_leg.dpv_name)
    dst_client.leg_create(
        dst_leg.leg_id,
        obt_encode(obt),
        str(dst_leg.leg_size),
        dm_context,
    )
    dst_client.fj_leg_export(
        dst_leg.leg_id,
        obt_encode(obt),
        fj.fj_name,
        src_leg.dpv_name,
        str(dst_leg.leg_size),
    )
    src_client = DpvClient(src_leg.dpv_name)
    src_client.fj_login(
        src_leg.leg_id,
        obt_encode(obt),
        fj.fj_name,
        dst_leg.dpv_name,
        dst_leg.leg_id,
    )
    dlv_info = get_dlv_info(dlv)
    ihost_client = IhostClient(dlv.ihost_name)
    try:
        ihost_client.dlv_suspend(
            dlv.dlv_name,
            obt_encode(obt),
            dlv_info,
        )
        bm_dict = ihost_client.bm_get(
            dlv.dlv_name,
            obt_encode(obt),
            dlv_info,
            'all',
            ori_leg.leg_id,
        )
        if ori_leg.idx < src_leg.idx:
            leg0 = ori_leg
            leg1 = src_leg
        else:
            leg0 = src_leg
            leg1 = ori_leg
        key = '{leg0_id}-{leg1_id}'.format(
            leg0_id=leg0.leg_id, leg1_id=leg1.leg_id)
        bm = bm_dict[key]
        src_client.fj_mirror_start(
            src_leg.leg_id,
            obt_encode(obt),
            fj.fj_name,
            dst_leg.dpv_name,
            dst_leg.leg_id,
            str(src_leg.leg_size),
            dm_context,
            bm,
        )
    finally:
        ihost_client.dlv_resume(
            dlv.dlv_name,
            obt_encode(obt),
            dlv_info,
        )


def fj_create(fj, dlv, obt):
    try:
        do_fj_create(fj, dlv, obt)
    except FjStatusError as e:
        return make_body('invalid_fj_status', e.message), 400
    except NoEnoughDpvError:
        fj.status = 'create_failed'
        fj.timestamp = datetime.datetime.utcnow()
        db.session.add(fj)
        db.session.commit()
        return make_body('no_enough_dpv', e.message), 500
    except DlvStatusError as e:
        fj.status = 'create_failed'
        fj.timestamp = datetime.datetime.utcnow()
        db.session.add(fj)
        obt_refresh(obt)
        db.session.commit()
        return make_body('invalid_dlv_status', e.message), 400
    except IhostError as e:
        fj.status = 'create_failed'
        fj.timestamp = datetime.datetime.utcnow()
        db.session.add(fj)
        obt_refresh(obt)
        db.session.commit()
        return make_body('ihost_failed', e.message), 500
    except DpvError as e:
        fj.status = 'create_failed'
        fj.timestamp = datetime.datetime.utcnow()
        db.session.add(fj)
        obt_refresh(obt)
        db.session.commit()
        return make_body('dpv_failed', e.message), 500
    else:
        fj.status = 'processing'
        fj.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('success'), 200


def handle_fjs_post(params, args):
    fj_name = args['fj_name']
    dlv_name = args['dlv_name']
    ori_id = args['ori_id']
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
    fj = FailoverJob(
        fj_name=fj_name,
        status='creating',
        timestamp=datetime.datetime.utcnow(),
        dlv_name=dlv_name,
    )
    ori_leg = Leg \
        .query \
        .filter_by(leg_id=ori_id) \
        .one()
    if ori_leg.fj is not None:
        return make_body('ori_busy'), 400
    ori_idx = ori_leg.idx
    if ori_idx % 2 == 0:
        src_idx = ori_idx + 1
    else:
        src_idx = ori_idx - 1
    group = ori_leg.group
    src_leg = None
    for leg in group.legs:
        if leg.idx == src_idx:
            src_leg = leg
            break
    assert(src_leg is not None)
    if src_leg.fj is not None:
        return make_body('src_busy'), 400
    src_leg.fj_role = 'src'
    ori_leg.fj_role = 'ori'
    src_leg.fj = fj
    ori_leg.fj = fj
    dst_leg = Leg(
        leg_id=uuid.uuid4().hex,
        idx=ori_leg.idx,
        leg_size=ori_leg.leg_size,
        fj_role='dst',
        fj=fj,
    )
    db.session.add(fj)
    db.session.add(src_leg)
    db.session.add(dst_leg)
    db.session.add(ori_leg)
    obt_refresh(obt)
    db.session.commit()
    return fj_create(fj, dlv, obt)


class Fjs(Resource):

    def get(self):
        return handle_dlvm_request(None, fjs_get_parser, handle_fjs_get)

    def post(self):
        return handle_dlvm_request(None, fjs_post_parser, handle_fjs_post)


fj_get_parser = reqparse.RequestParser()
fj_get_parser.add_argument(
    'with_process',
    type=str,
    choices=('true', 'false'),
    default='False',
    location='args',
)


fj_leg_fields = OrderedDict()
fj_leg_fields['idx'] = fields.String
fj_leg_fields['leg_id'] = fields.String
fj_leg_fields['leg_size'] = fields.Integer
fj_leg_fields['dpv_name'] = fields.String
fj_leg_fields['fj_role'] = fields.String
fj_fields = OrderedDict()
fj_fields['fj_name'] = fields.String
fj_fields['timestamp'] = fields.DateTime
fj_fields['status'] = fields.String
fj_fields['dlv_name'] = fields.String
fj_fields['g_idx'] = fields.Integer
fj_fields['process'] = fields.String
fj_fields['legs'] = fields.List(
    fields.Nested(fj_leg_fields))


def get_process_status(fj):
    if fj.status != 'processing':
        raise FjStatusError(fj.status)
    src_leg = None
    for leg in fj.legs:
        if leg.fj_role == 'src':
            src_leg = leg
            break
    assert(src_leg is not None)
    src_client = DpvClient(src_leg.dpv_name)
    ret = src_client.fj_mirror_status(src_leg.leg_id)
    return ret


def handle_fj_get(params, args):
    fj_name = params[0]
    try:
        fj = FailoverJob \
             .query \
             .with_lockmode('update') \
             .filter_by(fj_name=fj_name) \
             .one()
    except NoResultFound:
        return make_body('not_exist'), 404
    if args['with_process'] == 'true':
        process = get_process_status(fj)
    else:
        process = ''
    fj.process = process
    body = marshal(fj, fj_fields)
    return body, 200


fj_put_parser = reqparse.RequestParser()
fj_put_parser.add_argument(
    'action',
    type=str,
    choices=('cancel', 'finish'),
    required=True,
    location='json',
)
fj_put_parser.add_argument(
    't_id',
    type=str,
    required=True,
    location='json',
)
fj_put_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
fj_put_parser.add_argument(
    't_stage',
    type=int,
    required=True,
    location='json',
)


def free_dpv(leg, dlv_name, dvg_name, obt):
    dpv_name = leg.dpv_name
    leg_size = leg.leg_size
    dpv = DistributePhysicalVolume \
        .query \
        .with_lockmode('update') \
        .filter_by(dpv_name=dpv_name) \
        .one()
    dvg = DistributeVolumeGroup \
        .query \
        .with_lockmode('update') \
        .filter_by(dvg_name=dvg_name) \
        .one()
    dpv.free_size += leg_size
    db.session.add(dpv)
    dvg.free_size += leg_size
    db.session.add(dvg)
    leg.dpv_name = None
    db.session.add(leg)
    obt_refresh(obt)
    db.session.commit()


FJ_CAN_CANCEL_STATUS = (
    'creating',
    'create_failed',
    'cancel_failed',
    'processing',
)


def do_fj_cancel(fj, dlv, obt):
    if fj.status not in FJ_CAN_CANCEL_STATUS:
        raise FjStatusError(fj.status)
    fj.status = 'canceling'
    fj.timestamp = datetime.datetime.utcnow()
    db.session.add(fj)
    obt_refresh(obt)
    db.session.commit()
    src_leg, dst_leg, ori_leg = get_fj_legs(fj)
    if src_leg.dpv.status == 'available':
        src_client = DpvClient(src_leg.dpv_name)
        src_client.fj_mirror_stop(
            src_leg.leg_id,
            obt_encode(obt),
            fj.fj_name,
            dst_leg.leg_id,
            str(src_leg.leg_size),
        )
    if dst_leg.dpv is not None and dst_leg.dpv.status == 'available':
        dst_client = DpvClient(dst_leg.dpv_name)
        dst_client.fj_leg_unexport(
            dst_leg.leg_id,
            obt_encode(obt),
            fj.fj_name,
            src_leg.dpv_name,
        )
        dst_client.leg_delete(
            dst_leg.leg_id,
            obt_encode(obt),
        )

    if dst_leg.dpv is not None:
        free_dpv(dst_leg, fj.dlv_name, fj.dlv.dvg_name, obt)


def handle_fj_cancel(params, args):
    fj_name = params[0]
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    try:
        fj = FailoverJob \
            .query \
            .with_lockmode('update') \
            .filter_by(fj_name=fj_name) \
            .one()
        dlv_name = fj.dlv_name
        dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
        do_fj_cancel(fj, dlv, obt)
    except FjStatusError as e:
        return make_body('invalid_fj_status', e.message), 400
    except DpvError as e:
        fj.status = 'cancel_failed'
        fj.timestamp = datetime.datetime.utcnow()
        db.session.add(fj)
        obt_refresh(obt)
        db.session.commit()
        return make_body('dpv_failed', e.message), 500
    else:
        fj.status = 'canceled'
        fj.timestamp = datetime.datetime.utcnow()
        db.session.add(fj)
        obt_refresh(obt)
        db.session.commit()
        return make_body('success'), 200


CAN_FINISH_STATUS = (
    'processing',
    'finishing',
    'finish_failed',
)

DLV_FINISH_STATUS = (
    'attached',
    'detached',
)


def do_fj_finish(fj, dlv, obt):
    if fj.status not in CAN_FINISH_STATUS:
        raise FjStatusError(fj.status)
    if dlv.status not in DLV_FINISH_STATUS:
        raise FjStatusError('dlv:%s' % dlv.status)
    src_leg, dst_leg, ori_leg = get_fj_legs(fj)
    src_client = DpvClient(src_leg.dpv_name)
    dst_client = DpvClient(dst_leg.dpv_name)
    ori_client = DpvClient(ori_leg.dpv_name)
    if src_leg.dpv.status == 'available':
        src_client.fj_mirror_stop(
            src_leg.leg_id,
            obt_encode(obt),
            fj.fj_name,
            dst_leg.leg_id,
            str(dst_leg.leg_size),
        )
    if dst_leg.dpv.status == 'available':
        dst_client.fj_leg_unexport(
            dst_leg.leg_id,
            obt_encode(obt),
            fj.fj_name,
            src_leg.dpv_name,
        )
    if dlv.status == 'attached':
        if dst_leg.dpv.status == 'available':
            dst_client.leg_export(
                dst_leg.leg_id,
                obt_encode(obt),
                dlv.ihost_name,
            )
        if dlv.ihost.status == 'available':
            ihost_client = IhostClient(dlv.ihost_name)
            dlv_info = get_dlv_info(dlv)
            d_leg = {}
            d_leg['leg_id'] = dst_leg.leg_id
            d_leg['idx'] = dst_leg.idx
            d_leg['leg_size'] = str(dst_leg.leg_size)
            d_leg['dpv_name'] = dst_leg.dpv_name
            ihost_client.remirror(
                dlv.dlv_name,
                obt_encode(obt),
                dlv_info,
                src_leg.leg_id,
                d_leg,
            )
        if ori_leg.dpv.status == 'available':
            ori_client.leg_unexport(
                ori_leg.leg_id,
                obt_encode(obt),
                dlv.dlv_name,
            )

    if ori_leg.dpv.status == 'available':
        ori_client.leg_delete(
            ori_leg.leg_id,
            obt_encode(obt),
        )

    if ori_leg.dpv is not None:
        free_dpv(ori_leg, fj.dlv_name, fj.dlv.dvg_name, obt)
    dst_leg.group_id = ori_leg.group_id
    db.session.add(dst_leg)
    ori_leg.group_id = None
    db.session.add(ori_leg)
    # do not commit currently, commit with the fj status change togetehr


def handle_fj_finish(params, args):
    fj_name = params[0]
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    try:
        fj = FailoverJob \
            .query \
            .with_lockmode('update') \
            .filter_by(fj_name=fj_name) \
            .one()
        dlv_name = fj.dlv_name
        dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
        do_fj_finish(fj, dlv, obt)
    except FjStatusError as e:
        return make_body('invalid_fj_status', e.message), 400
    except IhostError as e:
        fj.status = 'finish_failed'
        fj.timestamp = datetime.datetime.utcnow()
        db.session.add(fj)
        obt_refresh(obt)
        db.session.commit()
        return make_body('ihost_failed', e.message), 500
    except DpvError as e:
        fj.status = 'finish_failed'
        fj.timestamp = datetime.datetime.utcnow()
        db.session.add(fj)
        obt_refresh(obt)
        db.session.commit()
        return make_body('dpv_failed', e.message), 500
    else:
        fj.status = 'finished'
        fj.timestamp = datetime.datetime.utcnow()
        db.session.add(fj)
        obt_refresh(obt)
        db.session.commit()
        return make_body('success'), 200


def handle_fj_put(params, args):
    if args['action'] == 'cancel':
        return handle_fj_cancel(params, args)
    elif args['action'] == 'finish':
        return handle_fj_finish(params, args)
    else:
        assert(False)


fj_delete_parser = reqparse.RequestParser()
fj_delete_parser.add_argument(
    't_id',
    type=str,
    required=True,
    location='json',
)
fj_delete_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
fj_delete_parser.add_argument(
    't_stage',
    type=int,
    required=True,
    location='json',
)


FJ_CAN_DELETE_STATUS = (
    'canceled',
    'finished',
)


def handle_fj_delete(params, args):
    fj_name = params[0]
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    try:
        fj = FailoverJob \
             .query \
             .with_lockmode('update') \
             .filter_by(fj_name=fj_name) \
             .one()
    except NoResultFound:
        return make_body('not_exist'), 404
    dlv_name = fj.dlv_name
    dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
    if fj.status not in FJ_CAN_DELETE_STATUS:
        return make_body('invalid_fj_status', fj.status), 400
    src_leg, dst_leg, ori_leg = get_fj_legs(fj)
    src_leg.role = None
    db.session.add(src_leg)
    if fj.status == 'finished':
        assert(ori_leg.dpv is None)
        db.session.delete(ori_leg)
        dst_leg.role = None
        db.session.add(dst_leg)
    else:
        assert(dst_leg.dpv is None)
        db.session.delete(dst_leg)
        ori_leg.role = None
        db.session.add(ori_leg)
    db.session.delete(fj)
    obt_refresh(obt)
    db.session.commit()
    return make_body('success'), 200


class Fj(Resource):

    def get(self, fj_name):
        return handle_dlvm_request(
            [fj_name],
            fj_get_parser,
            handle_fj_get,
        )

    def put(self, fj_name):
        return handle_dlvm_request(
            [fj_name],
            fj_put_parser,
            handle_fj_put,
        )

    def delete(self, fj_name):
        return handle_dlvm_request(
            [fj_name],
            fj_delete_parser,
            handle_fj_delete,
        )


@dlv_detach_register
def fj_check_for_dlv_detach(dlv):
    for fj in dlv.fjs:
        if fj.status in ('finishing', 'finish_failed'):
            msg = 'fj: %s %s' % (fj.fj_name, fj.status)
            raise DependenceCheckError(msg)


@dlv_delete_register
def fj_check_for_dlv_delete(dlv):
    for fj in dlv.fjs:
        msg = 'fj: %s' % fj.fj_name
        raise DependenceCheckError(msg)
