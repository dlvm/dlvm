#!/usr/bin/env python

from collections import OrderedDict
import datetime
from flask_restful import reqparse, Resource, fields, marshal
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound
from dlvm.utils.configure import conf
from dlvm.utils.error import ObtConflictError, IhostError
from modules import db, DistributePhysicalVolume, InitiatorHost
from handler import handle_dlvm_request, make_body, check_limit, \
    get_dlv_info, DpvClient, IhostClient, \
    obt_get, obt_refresh, obt_encode


ihost_summary_fields = OrderedDict()
ihost_summary_fields['ihost_name'] = fields.String
# ihost_summary_fields['in_sync'] = fields.Boolean
ihost_summary_fields['status'] = fields.String
ihost_summary_fields['timestamp'] = fields.DateTime
ihosts_get_fields = OrderedDict()
ihosts_get_fields['ihosts'] = fields.List(
    fields.Nested(ihost_summary_fields))

ihosts_get_parser = reqparse.RequestParser()
ihosts_get_parser.add_argument(
    'prev',
    type=str,
    location='args',
)
ihosts_get_parser.add_argument(
    'limit',
    type=check_limit(conf.ihost_list_limit),
    default=conf.ihost_list_limit,
    location='args',
)
ihosts_get_parser.add_argument(
    'order_by',
    type=str,
    choices=(
        'ihost_name',
        'timestamp',
    ),
    default='ihost_name',
    location='args',
)
ihosts_get_parser.add_argument(
    'reverse',
    type=str,
    choices=('true', 'false'),
    default='false',
    location='args',
)
ihosts_get_parser.add_argument(
    'status',
    type=str,
    choices=(
        'available',
        'unavailable',
    ),
    location='args',
)


def handle_ihosts_get(params, args):
    order_field = getattr(InitiatorHost, args['order_by'])
    prev = args['prev']
    if args['reverse'] == 'true':
        query = InitiatorHost.query.order_by(order_field.desc())
        if prev:
            query = query.filter(order_field < prev)
    else:
        query = InitiatorHost.query.order_by(order_field)
        if prev:
            query = query.filter(order_field > prev)
    if args['status']:
        query = query.filter_by(status=args['status'])
    query = query.limit(args['limit'])
    ihosts = query.all()
    body = marshal({'ihosts': ihosts}, ihosts_get_fields)
    return body['ihosts'], 200


ihosts_post_parser = reqparse.RequestParser()
ihosts_post_parser.add_argument(
    'ihost_name',
    type=str,
    required=True,
    location='json',
)


def handle_ihosts_post(params, args):
    ihost_name = args['ihost_name']
    ihost = InitiatorHost(
        ihost_name=ihost_name,
        in_sync=True,
        status='available',
        timestamp=datetime.datetime.utcnow(),
    )
    try:
        db.session.add(ihost)
        db.session.commit()
    except IntegrityError:
        return make_body('duplicate_ihost'), 400
    else:
        return make_body('success'), 200


class Ihosts(Resource):

    def get(self):
        return handle_dlvm_request(
            None, ihosts_get_parser, handle_ihosts_get)

    def post(self):
        return handle_dlvm_request(
            None, ihosts_post_parser, handle_ihosts_post)


def handle_ihost_delete(params, args):
    ihost_name = params[0]
    try:
        ihost = InitiatorHost \
            .query \
            .with_lockmode('update') \
            .filter_by(ihost_name=ihost_name) \
            .one()
    except NoResultFound:
        return make_body('not_exist', 404)
    if len(ihost.dlvs) > 0:
        return make_body('ihost_busy'), 403
    db.session.delete(ihost)
    db.session.commit()
    return make_body('success'), 200


ihost_put_parser = reqparse.RequestParser()
ihost_put_parser.add_argument(
    'action',
    type=str,
    choices=('set_available', 'set_unavailable'),
    required=True,
    location='json',
)
ihost_put_parser.add_argument(
    't_id',
    type=str,
    location='json',
)
ihost_put_parser.add_argument(
    't_owner',
    type=str,
    location='json',
)
ihost_put_parser.add_argument(
    't_stage',
    type=int,
    location='json',
)


def handle_ihost_available(ihost_name, t_id, t_owner, t_stage):
    obt = obt_get(t_id, t_owner, t_stage)
    ihost = InitiatorHost \
        .query \
        .with_lockmode('update') \
        .filter_by(ihost_name=ihost_name) \
        .one()
    if ihost.status != 'unavailable':
        return make_body('invalid_ihost_status', ihost.status), 400
    ihost_info = []
    for dlv in ihost.dlvs:
        if dlv.obt is not None and dlv.obt.t_id != obt.t_id:
            raise ObtConflictError()
        else:
            dlv.obt = obt
            db.session.add(dlv)
        dlv_info = get_dlv_info(dlv)
        for group in dlv.groups:
            for leg in group.legs:
                dpv = DistributePhysicalVolume \
                    .query \
                    .with_lockmode('update') \
                    .filter_by(dpv_name=leg.dpv_name) \
                    .one()
                if dpv.status != 'available':
                    continue
                client = DpvClient(dpv.dpv_name)
                client.leg_export(
                    leg.leg_id,
                    obt_encode(obt),
                    dlv.ihost_name,
                )
        ihost_info.append((dlv.dlv_name, dlv_info))
    obt_refresh(obt)
    db.session.commit()
    ihost_client = IhostClient(ihost_name)
    try:
        ihost_client.ihost_sync(ihost_info, obt_encode(obt))
    except IhostError as e:
        return make_body('ihost_failed', e.message), 500
    else:
        obt_refresh(obt)
        ihost.status = 'available'
        db.session.add(ihost)
        db.session.commit()
        return make_body('success'), 200


def handle_ihost_unavailable(ihost_name):
    ihost = InitiatorHost \
        .query \
        .with_lockmode('update') \
        .filter_by(ihost_name=ihost_name) \
        .one()
    ihost.status = 'unavailable'
    db.session.add(ihost)
    db.session.commit()
    return make_body('success'), 200


def handle_ihost_put(params, args):
    ihost_name = params[0]
    if args['action'] == 'set_available':
        t_id = args.get('t_id')
        if t_id is None:
            return make_body('no_t_id'), 400
        t_owner = args.get('t_owner')
        if t_owner is None:
            return make_body('no_t_owner'), 400
        t_stage = args.get('t_stage')
        if t_stage is None:
            return make_body('no_t_stage'), 400
        return handle_ihost_available(
            ihost_name, t_id, t_owner, t_stage)
    elif args['action'] == 'set_unavailable':
        return handle_ihost_unavailable(ihost_name)
    else:
        assert(False)


dlv_fields = OrderedDict()
dlv_fields['dlv_name'] = fields.String

ihost_fields = OrderedDict()
ihost_fields['ihost_name'] = fields.String
# ihost_fields['in_sync'] = fields.Boolean
ihost_fields['status'] = fields.String
ihost_fields['timestamp'] = fields.DateTime
ihost_fields['dlvs'] = fields.List(fields.Nested(dlv_fields))


def handle_ihost_get(params, args):
    ihost_name = params[0]
    try:
        ihost = InitiatorHost \
            .query \
            .with_lockmode('update') \
            .filter_by(ihost_name=ihost_name) \
            .one()
    except NoResultFound:
        return make_body('not_exist'), 404
    return marshal(ihost, ihost_fields), 200


class Ihost(Resource):

    def get(self, ihost_name):
        return handle_dlvm_request([ihost_name], None, handle_ihost_get)

    def put(self, ihost_name):
        return handle_dlvm_request(
            [ihost_name],
            ihost_put_parser,
            handle_ihost_put,
        )

    def delete(self, ihost_name):
        return handle_dlvm_request(
            [ihost_name],
            None,
            handle_ihost_delete,
        )
