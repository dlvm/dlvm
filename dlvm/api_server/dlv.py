#!/usr/bin/env python

from collections import OrderedDict
import uuid
import logging
import datetime
from flask_restful import reqparse, Resource, fields, marshal
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.configure import conf
from dlvm.utils.error import NoEnoughDpvError, DpvError, \
    DlvStatusError, DependenceCheckError, \
    IhostError, SnapNameError, SnapshotStatusError, \
    DlvIhostMisMatchError
from dlvm.utils.helper import dlv_info_encode
from modules import db, \
    DistributePhysicalVolume, DistributeLogicalVolume, \
    Snapshot, Group, Leg
from handler import handle_dlvm_request, make_body, check_limit, \
    get_dm_context, div_round_up, dlv_get, \
    obt_get, obt_refresh, obt_encode, \
    DpvClient, IhostClient, \
    dlv_detach_check, dlv_delete_check
from allocator import allocate_dpvs_for_group, free_dpvs_from_group

logger = logging.getLogger('dlvm_api')


dlvs_get_parser = reqparse.RequestParser()
dlvs_get_parser.add_argument(
    'prev',
    type=str,
    location='args',
)
dlvs_get_parser.add_argument(
    'limit',
    type=check_limit(conf.dlv_list_limit),
    default=conf.dlv_list_limit,
    location='args',
)
dlvs_get_parser.add_argument(
    'order_by',
    type=str,
    choices=(
        'dlv_name',
        'dlv_size',
    ),
    default='dlv_name',
    location='args',
)
dlvs_get_parser.add_argument(
    'reverse',
    type=str,
    choices=('true', 'false'),
    default='false',
    location='args',
)
dlvs_get_parser.add_argument(
    'dvg_name',
    type=str,
    location='args',
)


dlv_summary_fields = OrderedDict()
dlv_summary_fields['dlv_name'] = fields.String
dlv_summary_fields['dlv_size'] = fields.Integer
dlv_summary_fields['data_size'] = fields.Integer
dlv_summary_fields['stripe_number'] = fields.Integer
dlv_summary_fields['status'] = fields.String
dlv_summary_fields['timestamp'] = fields.String
dlv_summary_fields['dvg_name'] = fields.String
dlv_summary_fields['ihost_name'] = fields.String
dlv_summary_fields['active_snap_name'] = fields.String
dlv_summary_fields['t_id'] = fields.String
dlvs_get_fields = OrderedDict()
dlvs_get_fields['dlvs'] = fields.List(fields.Nested(dlv_summary_fields))


def handle_dlvs_get(params, args):
    order_field = getattr(DistributeLogicalVolume, args['order_by'])
    prev = args['prev']
    if args['reverse'] == 'true':
        query = DistributeLogicalVolume.query.order_by(order_field.desc())
        if prev:
            query = query.filter(order_field < prev)
    else:
        query = DistributeLogicalVolume.query.order_by(order_field)
        if prev:
            query = query.filter(order_field > prev)
    if args['dvg_name']:
        query = query.filter_by(dvg_name=args['dvg_name'])
    query = query.limit(args['limit'])
    dlvs = query.all()
    body = marshal({'dlvs': dlvs}, dlvs_get_fields)
    return body['dlvs'], 200


dlvs_post_parser = reqparse.RequestParser()
dlvs_post_parser.add_argument(
    'dlv_name',
    type=str,
    required=True,
    location='json',
)
dlvs_post_parser.add_argument(
    'dlv_size',
    type=int,
    required=True,
    location='json',
)
dlvs_post_parser.add_argument(
    'init_size',
    type=int,
    required=True,
    location='json',
)
dlvs_post_parser.add_argument(
    'stripe_number',
    type=int,
    required=True,
    location='json',
)
dlvs_post_parser.add_argument(
    'src_dlv_name',
    type=str,
    location='json',
)
dlvs_post_parser.add_argument(
    'dvg_name',
    type=str,
    required=True,
    location='json',
)

dlvs_post_parser.add_argument(
    't_id',
    type=str,
    required=True,
    location='json',
)
dlvs_post_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
dlvs_post_parser.add_argument(
    't_stage',
    type=int,
    required=True,
    location='json',
)


def dlv_create_new(dlv_name, t_id, t_owner, t_stage):
    try:
        dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
        for group in dlv.groups:
            allocate_dpvs_for_group(
                group, dlv.dvg_name, obt, conf.test_mode)
    except DpvError as e:
        dlv.status = 'create_failed'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('dpv_failed', e.message), 500
    except NoEnoughDpvError:
        dlv.status = 'create_failed'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('dpv_failed', 'no_enough_dpv'), 500
    else:
        dlv.status = 'detached'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('success'), 200


def handle_dlvs_create_new(params, args):
    dlv_name = args['dlv_name']
    dlv_size = args['dlv_size']
    stripe_number = args['stripe_number']
    dvg_name = args['dvg_name']
    snap_name = '%s/base' % dlv_name
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    init_size = args['init_size']
    if init_size > conf.init_max:
        init_size = conf.init_max
    elif init_size < conf.init_min:
        init_size = conf.init_min
    obt = obt_get(t_id, t_owner, t_stage)

    dlv = DistributeLogicalVolume(
        dlv_name=dlv_name,
        dlv_size=dlv_size,
        data_size=init_size,
        stripe_number=stripe_number,
        status='creating',
        timestamp=datetime.datetime.utcnow(),
        dvg_name=dvg_name,
        active_snap_name=snap_name,
        t_id=obt.t_id,
    )
    db.session.add(dlv)
    snapshot = Snapshot(
        snap_name=snap_name,
        timestamp=datetime.datetime.utcnow(),
        thin_id=0,
        ori_thin_id=0,
        status='available',
        dlv_name=dlv_name,
    )
    db.session.add(snapshot)
    thin_meta_size = conf.thin_meta_factor * div_round_up(
        dlv_size, conf.thin_block_size)
    if thin_meta_size < conf.thin_meta_min:
        thin_meta_size = conf.thin_meta_min
    group = Group(
        group_id=uuid.uuid4().hex,
        idx=0,
        group_size=thin_meta_size,
        dlv_name=dlv_name,
    )
    db.session.add(group)
    leg_size = thin_meta_size + conf.mirror_meta_size
    leg_size = div_round_up(leg_size, conf.lvm_unit) * conf.lvm_unit
    for i in xrange(2):
        leg = Leg(
            leg_id=uuid.uuid4().hex,
            idx=i,
            group=group,
            leg_size=leg_size,
        )
        db.session.add(leg)
    group_size = init_size
    group = Group(
        group_id=uuid.uuid4().hex,
        idx=1,
        group_size=group_size,
        dlv_name=dlv_name,
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
    return dlv_create_new(dlv_name, t_id, t_owner, t_stage)


def handle_dlvs_clone(params, args):
    pass


def handle_dlvs_post(params, args):
    if args['src_dlv_name'] is None:
        return handle_dlvs_create_new(params, args)
    else:
        return handle_dlvs_clone(params, args)


class Dlvs(Resource):

    def get(self):
        return handle_dlvm_request(None, dlvs_get_parser, handle_dlvs_get)

    def post(self):
        return handle_dlvm_request(None, dlvs_post_parser, handle_dlvs_post)


dlv_delete_parser = reqparse.RequestParser()
dlv_delete_parser.add_argument(
    't_id',
    type=str,
    required=True,
    location='json',
)
dlv_delete_parser.add_argument(
    't_owner',
    type=str,
    required=True,
    location='json',
)
dlv_delete_parser.add_argument(
    't_stage',
    type=int,
    required=True,
    location='json',
)


CAN_DELETE_STATUS = (
    'detached',
    'create_failed',
    'delete_failed',
    'deleting',
    'creating',
)


def dlv_delete(dlv, obt):
    if dlv.status not in CAN_DELETE_STATUS:
        raise DlvStatusError(dlv.status)
    dlv_delete_check(dlv)
    dlv.status = 'deleting'
    dlv.timestamp = datetime.datetime.utcnow()
    obt_refresh(obt)
    db.session.commit()
    for group in dlv.groups:
        free_dpvs_from_group(
            group, dlv.dvg_name, obt)
    for snapshot in dlv.snapshots:
        db.session.delete(snapshot)
    db.session.delete(dlv)
    obt_refresh(obt)
    db.session.commit()


def handle_dlv_delete(params, args):
    dlv_name = params[0]
    t_id = args['t_id']
    t_owner = args['t_owner']
    t_stage = args['t_stage']
    try:
        dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
        dlv_delete(dlv, obt)
    except DpvError as e:
        dlv.status = 'delete_failed'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('dpv_failed', e.message), 500
    except DlvStatusError as e:
        return make_body('invalid_dlv_status', e.message), 400
    except DependenceCheckError as e:
        return make_body('dependence_check_failed', e.message), 400
    else:
        return make_body('success'), 200


dlv_put_parser = reqparse.RequestParser()
dlv_put_parser.add_argument(
    'action',
    type=str,
    choices=('attach', 'detach', 'set_snap'),
    required=True,
    location='json',
)
dlv_put_parser.add_argument(
    'ihost_name',
    type=str,
    location='json',
)
dlv_put_parser.add_argument(
    'snap_name',
    type=str,
    location='json',
)
dlv_put_parser.add_argument(
    't_id',
    type=str,
    location='json',
)
dlv_put_parser.add_argument(
    't_owner',
    type=str,
    location='json',
)
dlv_put_parser.add_argument(
    't_stage',
    type=int,
    location='json',
)


def do_attach(dlv, obt):
    dlv_info = {}
    dlv_info['dlv_name'] = dlv.dlv_name
    dlv_info['dlv_size'] = dlv.dlv_size
    dm_context = get_dm_context()
    dm_context['stripe_number'] = dlv.stripe_number
    dlv_info['dm_context'] = dm_context
    dlv_info['data_size'] = dlv.data_size
    snapshot = Snapshot \
        .query \
        .filter_by(snap_name=dlv.active_snap_name) \
        .with_entities(Snapshot.thin_id) \
        .one()
    dlv_info['thin_id'] = snapshot.thin_id
    dlv_info['groups'] = []
    for group in dlv.groups:
        igroup = {}
        igroup['group_id'] = group.group_id
        igroup['idx'] = group.idx
        igroup['group_size'] = group.group_size
        igroup['legs'] = []
        for leg in group.legs:
            ileg = {}
            ileg['dpv_name'] = leg.dpv_name
            ileg['leg_id'] = leg.leg_id
            ileg['idx'] = leg.idx
            ileg['leg_size'] = leg.leg_size
            igroup['legs'].append(ileg)
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
        dlv_info['groups'].append(igroup)
    dlv_info_encode(dlv_info)

    client = IhostClient(dlv.ihost_name)
    client.dlv_aggregate(
        dlv.dlv_name,
        obt_encode(obt),
        dlv_info,
    )


def dlv_attach(dlv, ihost_name, obt):
    if dlv.status != 'detached':
        raise DlvStatusError(dlv.status)
    dlv.status = 'attaching'
    dlv.timestamp = datetime.datetime.utcnow()
    dlv.ihost_name = ihost_name
    db.session.add(dlv)
    obt_refresh(obt)
    db.session.commit()
    return do_attach(dlv, obt)


def handle_dlv_attach(dlv_name, ihost_name, t_id, t_owner, t_stage):
    try:
        dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
        dlv_attach(dlv, ihost_name, obt)
    except IhostError as e:
        dlv.status = 'attach_failed'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('ihost_failed', e.message), 500
    except DlvStatusError as e:
        return make_body('invalid_dlv_status', e.message), 400
    else:
        dlv.status = 'attached'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        for group in dlv.groups:
            db.session.add(group)
        obt_refresh(obt)
        db.session.commit()
        return make_body('success'), 200


CAN_DETACH_STATUS = (
    'attached',
    'attaching',
    'attach_failed',
    'detach_failed',
)


def do_detach(dlv, obt):
    dlv_info = {}
    dlv_info['dlv_name'] = dlv.dlv_name
    dm_context = get_dm_context()
    dm_context['stripe_number'] = dlv.stripe_number
    dlv_info['dm_context'] = dm_context
    snapshot = Snapshot \
        .query \
        .filter_by(snap_name=dlv.active_snap_name) \
        .with_entities(Snapshot.thin_id) \
        .one()
    dlv_info['thin_id'] = snapshot.thin_id
    dlv_info['groups'] = []
    for group in dlv.groups:
        igroup = {}
        igroup['group_id'] = group.group_id
        igroup['idx'] = group.idx
        igroup['legs'] = []
        for leg in group.legs:
            ileg = {}
            ileg['dpv_name'] = leg.dpv_name
            ileg['leg_id'] = leg.leg_id
            ileg['idx'] = leg.idx
            igroup['legs'].append(ileg)
        dlv_info['groups'].append(igroup)
    dlv_info_encode(dlv_info)

    ihost = dlv.ihost
    if ihost.status == 'available':
        client = IhostClient(dlv.ihost_name)
        client.dlv_degregate(
            dlv.dlv_name,
            obt_encode(obt),
            dlv_info,
        )

    for group in dlv.groups:
        for leg in group.legs:
            dpv_name = leg.dpv_name
            dpv = DistributePhysicalVolume \
                .query \
                .with_lockmode('update') \
                .filter_by(dpv_name=dpv_name) \
                .one()
            if dpv.status == 'available':
                client = DpvClient(dpv_name)
                client.leg_unexport(
                    leg.leg_id,
                    obt_encode(obt),
                    dlv.ihost_name,
                )


def dlv_detach(dlv, ihost_name, obt):
    if dlv.status not in CAN_DETACH_STATUS:
        raise DlvStatusError(dlv.status)
    if dlv.ihost_name != ihost_name:
        context = {
            'dlv.ihost_name': dlv.ihost_name,
            'ihost_name': ihost_name,
        }
        raise DlvIhostMisMatchError(context)
    dlv_detach_check(dlv)
    dlv.status = 'detaching'
    dlv.timestamp = datetime.datetime.utcnow()
    db.session.add(dlv)
    obt_refresh(obt)
    db.session.commit()
    return do_detach(dlv, obt)


def handle_dlv_detach(dlv_name, ihost_name, t_id, t_owner, t_stage):
    try:
        dlv, obt = dlv_get(dlv_name, t_id, t_owner, t_stage)
        dlv_detach(dlv, ihost_name, obt)
    except DlvIhostMisMatchError as e:
        return make_body('ihost_mismatch', e.message), 500
    except DpvError as e:
        dlv.status = 'attach_failed'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('dpv_failed', e.message), 500
    except IhostError as e:
        dlv.status = 'attach_failed'
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('ihost_failed', e.message), 500
    except DlvStatusError as e:
        return make_body('invalid_dlv_status', e.message), 400
    except DependenceCheckError as e:
        return make_body('dependence_check_failed', e.message), 400
    else:
        dlv.status = 'detached'
        dlv.ihost_name = None
        dlv.timestamp = datetime.datetime.utcnow()
        db.session.add(dlv)
        obt_refresh(obt)
        db.session.commit()
        return make_body('success'), 200


def dlv_set_snap(dlv, snap_name, obt):
    full_snap_name = '{dlv_name}/{snap_name}'.format(
        dlv_name=dlv.dlv_name, snap_name=snap_name)
    if dlv.status != 'detached':
        raise DlvStatusError(dlv.status)
    try:
        snapshot = Snapshot \
            .query \
            .filter_by(snap_name=full_snap_name) \
            .one()
    except NoResultFound:
        raise SnapNameError(full_snap_name)
    if snapshot.status != 'available':
        raise SnapshotStatusError(snapshot.status)
    dlv.active_snap_name = full_snap_name
    db.session.add(dlv)
    obt_refresh(obt)
    db.session.commit()


def handle_dlv_set_snap(dlv_name, snap_name):
    try:
        dlv = DistributeLogicalVolume \
            .query \
            .with_lockmode('update') \
            .filter_by(dlv_name=dlv_name) \
            .one()
    except NoResultFound:
        return make_body('dlv_not_exist'), 404

    if dlv.status != 'detached':
        return make_body('invalid_dlv_status', dlv.status), 400

    full_snap_name = '{dlv_name}/{snap_name}'.format(
        dlv_name=dlv.dlv_name, snap_name=snap_name)
    try:
        snapshot = Snapshot \
            .query \
            .filter_by(snap_name=full_snap_name) \
            .one()
    except NoResultFound:
        return make_body('snap_not_exist'), 404
    if snapshot.status != 'available':
        return make_body('invalid_snap_status', snapshot.status), 400
    dlv.active_snap_name = full_snap_name
    db.session.add(dlv)
    db.session.commit()
    return make_body('success'), 200


def handle_dlv_put(params, args):
    dlv_name = params[0]
    if args['action'] != 'set_snap':
        t_id = args['t_id']
        t_owner = args['t_owner']
        t_stage = args['t_stage']
    if args['action'] == 'attach':
        if 'ihost_name' not in args:
            return make_body('no_ihost_name'), 400
        else:
            return handle_dlv_attach(
                dlv_name, args['ihost_name'], t_id, t_owner, t_stage)
    elif args['action'] == 'detach':
        if 'ihost_name' not in args:
            return make_body('no_ihost_name'), 400
        else:
            return handle_dlv_detach(
                dlv_name, args['ihost_name'], t_id, t_owner, t_stage)
    elif args['action'] == 'set_snap':
        if 'snap_name' not in args:
            return make_body('no_snap_name'), 400
        else:
            return handle_dlv_set_snap(
                dlv_name, args['snap_name'])
    else:
        assert(False)


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
dlv_fields = OrderedDict()
dlv_fields['dlv_name'] = fields.String
dlv_fields['dlv_size'] = fields.Integer
dlv_fields['data_size'] = fields.Integer
dlv_fields['stripe_number'] = fields.Integer
dlv_fields['status'] = fields.String
dlv_fields['timestamp'] = fields.String
dlv_fields['dvg_name'] = fields.String
dlv_fields['ihost_name'] = fields.String
dlv_fields['active_snap_name'] = fields.String
dlv_fields['ej_name'] = fields.String(
    attribute=lambda x: x.ej.ej_name if x.ej else None)
dlv_fields['t_id'] = fields.String
dlv_fields['groups'] = fields.List(fields.Nested(group_fields))


def handle_dlv_get(params, args):
    dlv_name = params[0]
    try:
        dlv = DistributeLogicalVolume \
              .query \
              .filter_by(dlv_name=dlv_name) \
              .one()
    except NoResultFound:
        return make_body('not_exist'), 404
    body = marshal(dlv, dlv_fields)
    return body, 200


class Dlv(Resource):

    def get(self, dlv_name):
        return handle_dlvm_request(
            [dlv_name],
            None,
            handle_dlv_get,
        )

    def put(self, dlv_name):
        return handle_dlvm_request(
            [dlv_name],
            dlv_put_parser,
            handle_dlv_put,
        )

    def delete(self, dlv_name):
        return handle_dlvm_request(
            [dlv_name],
            dlv_delete_parser,
            handle_dlv_delete,
        )
