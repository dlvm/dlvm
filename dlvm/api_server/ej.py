#!/usr/bin/env python

import uuid
from collections import OrderedDict
import logging
import datetime
from flask_restful import reqparse, Resource, fields, marshal
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.configure import conf
from dlvm.utils.error import NoEnoughDpvError, DpvError, IhostError, \
    EjStatusError, DependenceCheckError
from dlvm.utils.helper import group_encode
from modules import db, DistributePhysicalVolume, \
    Group, Leg, ExtendJob
from handler import handle_dlvm_request, make_body, check_limit, \
    DpvClient, IhostClient, \
    div_round_up, dlv_get, \
    obt_refresh, obt_encode, get_dlv_info, \
    dlv_detach_register, dlv_delete_register
from allocator import allocate_dpvs_for_group, free_dpvs_from_group


logger = logging.getLogger('dlvm_api')


ej_summary_fields = OrderedDict()
ej_summary_fields['ej_name'] = fields.String
ej_summary_fields['ej_size'] = fields.Integer
ej_summary_fields['timestamp'] = fields.DateTime
ej_summary_fields['status'] = fields.String
ej_summary_fields['dlv_name'] = fields.String
ejs_get_fields = OrderedDict()
ejs_get_fields['ejs'] = fields.List(
    fields.Nested(ej_summary_fields))


ejs_get_parser = reqparse.RequestParser()
ejs_get_parser.add_argument(
    'prev',
    type=str,
    location='args',
)
ejs_get_parser.add_argument(
    'limit',
    type=check_limit(conf.ej_list_limit),
    default=conf.ej_list_limit,
    location='args',
)
ejs_get_parser.add_argument(
    'order_by',
    type=str,
    choices=(
        'ej_name',
        'timestamp',
        'dlv_name',
    ),
    default='ej_name',
    location='args',
)
ejs_get_parser.add_argument(
    'reverse',
    type=str,
    choices=('true', 'false'),
    default='false',
    location='args',
)
ejs_get_parser.add_argument(
    'status',
    type=str,
    choices=(
        'creating',
        'create_failed',
        'canceling',
        'cancel_failed',
        'created',
        'finishing',
        'finish_failed',
    ),
    location='args',
)


def handle_ejs_get(params, args):
    order_field = getattr(ExtendJob, args['order_by'])
    prev = args['prev']
    if args['reverse'] == 'true':
        query = ExtendJob.query.order_by(order_field.desc())
        if prev:
            query = query.filter(order_field < prev)
    else:
        query = ExtendJob.query.order_by(order_field)
        if prev:
            query = query.filter(order_field > prev)
    if args['status']:
        query = query.filter_by(status=args['status'])
    query = query.limit(args['limit'])
    ejs = query.all()
    body = marshal({'ejs': ejs}, ejs_get_fields)
    return body['ejs'], 200


ejs_post_parser = reqparse.RequestParser()
ejs_post_parser.add_argument(
    'ej_name',
    type=str,
    required=True,
    location='json',
)
ejs_post_parser.add_argument(
    'dlv_name',
    type=str,
    required=True,
    location='json',
)
ejs_post_parser.add_argument(
    'ej_size',
    type=int,
    required=True,
    location='json',
)
ejs_post_parser.add_argument(
    't_id',
    type=str,
    required=True,
    location='json',
)
ejs_post_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
ejs_post_parser.add_argument(
    't_stage',
    type=int,
    required=True,
    location='json',
)


def ej_create(ej, dlv, obt):
    group = ej.group
    try:
        allocate_dpvs_for_group(
            group, dlv.dvg_name, obt, conf.test_mode)
    except DpvError as e:
        ej.status = 'create_failed'
        ej.timestamp = datetime.datetime.utcnow()
        db.session.add(ej)
        obt_refresh(obt)
        db.session.commit()
        return make_body('dpv_failed', e.message), 500
    except NoEnoughDpvError:
        ej.status = 'create_failed'
        ej.timestamp = datetime.datetime.utcnow()
        db.session.add(ej)
        obt_refresh(obt)
        db.session.commit()
        return make_body('dpv_failed', 'no_enough_dpv'), 500
    else:
        ej.status = 'created'
        ej.timestamp = datetime.datetime.utcnow()
        db.session.add(ej)
        obt_refresh(obt)
        db.session.commit()
        return make_body('success'), 200


def handle_ejs_post(params, args):
    ej_name = args['ej_name']
    dlv_name = args['dlv_name']
    ej_size = args['ej_size']
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
    if dlv.ej is not None:
        return make_body('dlv_busy'), 400
    ej = ExtendJob(
        ej_name=ej_name,
        status='creating',
        timestamp=datetime.datetime.utcnow(),
        ej_size=ej_size,
        dlv_name=dlv_name,
    )
    db.session.add(ej)
    dlv.ej = ej
    max_idx = 0
    for group in dlv.groups:
        max_idx = max(max_idx, group.idx)
    idx = max_idx + 1
    group_size = ej_size
    stripe_number = dlv.stripe_number
    group = Group(
        group_id=uuid.uuid4().hex,
        idx=idx,
        group_size=group_size,
        ej_name=ej_name,
    )
    db.session.add(group)
    leg_size = div_round_up(
        group_size, stripe_number) + conf.mirror_meta_size
    leg_size = div_round_up(leg_size, conf.lvm_unit) * conf.lvm_unit
    legs_per_group = 2 * stripe_number
    for i in xrange(legs_per_group):
        leg = Leg(
            leg_id=uuid.uuid4().hex,
            idx=i,
            group=group,
            leg_size=leg_size,
        )
        db.session.add(leg)
    obt_refresh(obt)
    db.session.commit()
    return ej_create(ej, dlv, obt)


class Ejs(Resource):

    def get(self):
        return handle_dlvm_request(None, ejs_get_parser, handle_ejs_get)

    def post(self):
        return handle_dlvm_request(None, ejs_post_parser, handle_ejs_post)


leg_fields = OrderedDict()
leg_fields['leg_id'] = fields.String
leg_fields['idx'] = fields.Integer
leg_fields['leg_size'] = fields.Integer
leg_fields['dpv_name'] = fields.String
leg_fields['fj_role'] = fields.String
leg_fields['fj_name'] = fields.String
group_fields = OrderedDict()
group_fields['group_id'] = fields.String
group_fields['idx'] = fields.Integer
group_fields['group_size'] = fields.Integer
group_fields['legs'] = fields.List(fields.Nested(leg_fields))
ej_fields = OrderedDict()
ej_fields['ej_name'] = fields.String
ej_fields['ej_size'] = fields.Integer
ej_fields['status'] = fields.String
ej_fields['timestamp'] = fields.DateTime
ej_fields['dlv_name'] = fields.String
ej_fields['group'] = fields.Nested(group_fields)


def handle_ej_get(params, args):
    ej_name = params[0]
    try:
        ej = ExtendJob \
            .query \
            .with_lockmode('update') \
            .filter_by(ej_name=ej_name) \
            .one()
    except NoResultFound:
        return make_body('not_exist'), 404
    return marshal(ej, ej_fields), 200


ej_put_parser = reqparse.RequestParser()
ej_put_parser.add_argument(
    'action',
    type=str,
    choices=('cancel', 'finish'),
    required=True,
    location='json',
)
ej_put_parser.add_argument(
    't_id',
    type=str,
    required=True,
    location='json',
)
ej_put_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
ej_put_parser.add_argument(
    't_stage',
    type=int,
    required=True,
    location='json',
)


EJ_CAN_CANCEL_STATUS = (
    'creating',
    'create_failed',
    'cancel_faile',
    'created',
)


def do_ej_cancel(ej, dlv, obt):
    if ej.status not in EJ_CAN_CANCEL_STATUS:
        raise EjStatusError(ej.status)
    ej.status = 'canceling'
    ej.timestamp = datetime.datetime.utcnow()
    db.session.add(ej)
    obt_refresh(obt)
    db.session.commit()

    group = ej.group
    free_dpvs_from_group(group, dlv.dvg_name, obt)


def handle_ej_cancel(params, args):
    ej_name = params[0]
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    try:
        ej = ExtendJob \
            .query \
            .with_lockmode('update') \
            .filter_by(ej_name=ej_name) \
            .one()
        dlv_name = ej.dlv_name
        dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
        do_ej_cancel(ej, dlv, obt)
    except EjStatusError as e:
        return make_body('invalid_ej_status', e.message), 400
    except DpvError as e:
        ej.status = 'cancel_failed'
        ej.timestamp = datetime.datetime.utcnow()
        db.session.add(ej)
        obt_refresh(obt)
        db.session.commit()
    else:
        ej.status = 'canceled'
        ej.timestamp = datetime.datetime.utcnow()
        db.session.add(ej)
        obt_refresh(obt)
        db.session.commit()
        return make_body('success'), 200


CAN_FINISH_STATUS = (
    'created',
    'finish_failed',
    'finishing',
)

DLV_FINISH_STATUS = (
    'attached',
    'detached',
)


def do_ej_finish(ej, dlv, obt):
    if ej.status not in CAN_FINISH_STATUS:
        raise EjStatusError(ej.status)
    if dlv.status not in DLV_FINISH_STATUS:
        raise EjStatusError('dlv:%s' % dlv.status)
    if dlv.status == 'attached':
        group = ej.group
        ej_group = {}
        ej_group['group_id'] = group.group_id
        ej_group['idx'] = group.idx
        ej_group['group_size'] = group.group_size
        ej_group['legs'] = []
        for leg in group.legs:
            ileg = {}
            ileg['dpv_name'] = leg.dpv_name
            ileg['leg_id'] = leg.leg_id
            ileg['idx'] = leg.idx
            ileg['leg_size'] = leg.leg_size
            ej_group['legs'].append(ileg)
            dpv_name = leg.dpv_name
            dpv = DistributePhysicalVolume \
                .query \
                .with_lockmode('update') \
                .filter_by(dpv_name=dpv_name) \
                .one()
            if dpv.status == 'available':
                client = DpvClient(dpv_name)
                client.leg_export(
                    leg.leg_id,
                    obt_encode(obt),
                    dlv.ihost_name,
                )
        group_encode(ej_group)
        dlv_info = get_dlv_info(dlv)
        if dlv.ihost.status == 'available':
            ihost_client = IhostClient(dlv.ihost_name)
            ihost_client.dlv_extend(
                dlv.dlv_name,
                obt_encode(obt),
                dlv_info,
                ej_group,
            )
    dlv.data_size += ej.ej_size
    ej.group.dlv_name = dlv.dlv_name
    db.session.add(dlv)
    db.session.add(ej.group)
    obt_refresh(obt)
    db.session.commit()


def handle_ej_finish(params, args):
    ej_name = params[0]
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    try:
        ej = ExtendJob \
            .query \
            .with_lockmode('update') \
            .filter_by(ej_name=ej_name) \
            .one()
        dlv_name = ej.dlv_name
        dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
        do_ej_finish(ej, dlv, obt)
    except EjStatusError as e:
        return make_body('invalid_ej_status', e.message), 400
    except DpvError as e:
        ej.status = 'finish_failed'
        ej.timestamp = datetime.datetime.utcnow()
        db.session.add(ej)
        obt_refresh(obt)
        db.session.commit()
        return make_body('dpv_failed', e.message), 500
    except IhostError as e:
        ej.status = 'finish_failed'
        ej.timestamp = datetime.datetime.utcnow()
        db.session.add(ej)
        obt_refresh(obt)
        db.session.commit()
        return make_body('ihost_failed', e.message), 500
    else:
        ej.status = 'finished'
        ej.timestamp = datetime.datetime.utcnow()
        db.session.add(ej)
        obt_refresh(obt)
        db.session.commit()
        return make_body('success'), 200


def handle_ej_put(params, args):
    if args['action'] == 'cancel':
        return handle_ej_cancel(params, args)
    elif args['action'] == 'finish':
        return handle_ej_finish(params, args)
    else:
        assert(False)


ej_delete_parser = reqparse.RequestParser()
ej_delete_parser.add_argument(
    't_id',
    type=str,
    required=True,
    location='json',
)
ej_delete_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
ej_delete_parser.add_argument(
    't_stage',
    type=int,
    required=True,
    location='json',
)


EJ_CAN_DELETE_STATUS = (
    'canceled',
    'finished',
)


def handle_ej_delete(params, args):
    ej_name = params[0]
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    try:
        ej = ExtendJob \
            .query \
            .with_lockmode('update') \
            .filter_by(ej_name=ej_name) \
            .one()
    except NoResultFound:
        return make_body('not_exist'), 404
    dlv_name = ej.dlv_name
    dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
    if ej.status not in EJ_CAN_DELETE_STATUS:
        return make_body('invalid_ej_status', ej.status), 400
    group = ej.group
    group.ej_name = None
    db.session.add(group)
    db.session.delete(ej)
    obt_refresh(obt)
    db.session.commit()
    return make_body('success'), 200


class Ej(Resource):

    def get(self, ej_name):
        return handle_dlvm_request(
            [ej_name],
            None,
            handle_ej_get,
        )

    def put(self, ej_name):
        return handle_dlvm_request(
            [ej_name],
            ej_put_parser,
            handle_ej_put,
        )

    def delete(self, ej_name):
        return handle_dlvm_request(
            [ej_name],
            ej_delete_parser,
            handle_ej_delete,
        )


@dlv_detach_register
def ej_check_for_dlv_detach(dlv):
    ej = dlv.ej
    if ej is not None and ej.status in ('finishing', 'finish_failed'):
        msg = 'ej: %s %s' % (ej.ej_name, ej.status)
        raise DependenceCheckError(msg)


@dlv_delete_register
def ej_check_for_dlv_delete(dlv):
    ej = dlv.ej
    if ej is not None:
        msg = 'ej: %s' % ej.ej_name
        raise DependenceCheckError(msg)
