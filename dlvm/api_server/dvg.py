#!/usr/bin/env python

from collections import OrderedDict
from flask_restful import reqparse, Resource, fields, marshal
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from dlvm.utils.configure import conf
from modules import db, \
    DistributePhysicalVolume, DistributeVolumeGroup, DistributeLogicalVolume
from handler import handle_dlvm_request, make_body, check_limit


dvgs_get_parser = reqparse.RequestParser()
dvgs_get_parser.add_argument(
    'prev',
    type=str,
    location='args',
)
dvgs_get_parser.add_argument(
    'limit',
    type=check_limit(conf.dvg_list_limit),
    default=conf.dvg_list_limit,
    location='args',
)
dvgs_get_parser.add_argument(
    'order_by',
    type=str,
    choices=(
        'dvg_name',
        'total_size',
        'free_size',
    ),
    default='dvg_name',
    location='args',
)
dvgs_get_parser.add_argument(
    'reverse',
    type=str,
    choices=('true', 'false'),
    default='false',
    location='args',
)

dvg_summary_fields = OrderedDict()
dvg_summary_fields['dvg_name'] = fields.String
dvg_summary_fields['total_size'] = fields.Integer
dvg_summary_fields['free_size'] = fields.Integer
dvgs_get_fields = OrderedDict()
dvgs_get_fields['dvgs'] = fields.List(fields.Nested(dvg_summary_fields))


def handle_dvgs_get(params, args):
    order_field = getattr(DistributeVolumeGroup, args['order_by'])
    prev = args['prev']
    if args['reverse'] == 'true':
        query = DistributeVolumeGroup.query.order_by(order_field.desc())
        if prev:
            query = query.filter(order_field < prev)
    else:
        query = DistributeVolumeGroup.query.order_by(order_field)
        if prev:
            query = query.filter(order_field > prev)
    query = query.limit(args['limit'])
    dvgs = query.all()
    body = marshal({'dvgs': dvgs}, dvgs_get_fields)
    return body['dvgs'], 200


dvgs_post_parser = reqparse.RequestParser()
dvgs_post_parser.add_argument(
    'dvg_name',
    type=str,
    required=True,
    location='json',
)


def handle_dvgs_post(params, args):
    dvg = DistributeVolumeGroup(
        dvg_name=args['dvg_name'],
        total_size=0,
        free_size=0,
    )
    db.session.add(dvg)
    db.session.commit()
    return make_body('success'), 200


class Dvgs(Resource):

    def get(self):
        return handle_dlvm_request(None, dvgs_get_parser, handle_dvgs_get)

    def post(self):
        return handle_dlvm_request(None, dvgs_post_parser, handle_dvgs_post)


dvg_fields = OrderedDict()
dvg_fields['dvg_name'] = fields.String
dvg_fields['total_size'] = fields.Integer
dvg_fields['free_size'] = fields.Integer


def handle_dvg_get(params, args):
    dvg_name = params[0]
    try:
        dvg = DistributeVolumeGroup \
            .query \
            .with_lockmode('update') \
            .filter_by(dvg_name=dvg_name) \
            .one()
    except NoResultFound:
        return make_body('not_exist'), 404
    return marshal(dvg, dvg_fields), 200


dvg_put_parser = reqparse.RequestParser()
dvg_put_parser.add_argument(
    'action',
    type=str,
    choices=('extend', 'reduce'),
    required=True,
    location='json',
)
dvg_put_parser.add_argument(
    'dpv_name',
    type=str,
    required=True,
    location='json',
)


def dvg_extend(dvg_name, dpv_name):
    try:
        dvg = DistributeVolumeGroup \
            .query \
            .with_lockmode('update') \
            .filter_by(dvg_name=dvg_name) \
            .one()
    except NoResultFound:
        return make_body('not_exist'), 404

    try:
        dpv = DistributePhysicalVolume \
            .query \
            .with_lockmode('update') \
            .filter_by(dpv_name=dpv_name) \
            .one()
    except NoResultFound:
        return make_body('not_exist'), 404

    if dpv.dvg_name:
        return make_body('dvg_conflict', dpv.dvg_name), 403

    if dpv.status != 'available':
        return make_body('not_available', dpv.status)
    if (dpv.total_size != dpv.free_size):
        context = {
            'total_size': dpv.total_size,
            'free_size': dpv.free_size,
        }
        return make_body('size_error', context), 500

    dpv.dvg_name = dvg_name
    dvg.total_size += dpv.total_size
    dvg.free_size += dpv.free_size
    db.session.add(dpv)
    db.session.add(dvg)
    db.session.commit()
    return make_body('success'), 200


def dvg_reduce(dvg_name, dpv_name):
    try:
        dvg = DistributeVolumeGroup \
            .query \
            .with_lockmode('update') \
            .filter_by(dvg_name=dvg_name) \
            .one()
    except NoResultFound:
        return make_body('not_exist'), 200

    try:
        dpv = DistributePhysicalVolume \
            .query \
            .with_lockmode('update') \
            .filter_by(dpv_name=dpv_name) \
            .one()
    except NoResultFound:
        return make_body('not_exist'), 200

    if dpv.dvg_name != dvg_name:
        ctx = {
            'dpv.dvg_name': dpv.dvg_name,
            'dvg_name': dvg_name
        }
        return make_body('dvg_conflict', ctx), 403

    if dpv.status == 'available':
        if (dpv.total_size != dpv.free_size):
            context = {
                'total_size': dpv.total_size,
                'free_size': dpv.free_size,
            }
            return make_body('size_error', context)
    else:
        for leg in dpv.legs:
            context = {
                'dlv_name': leg.group.dlv_name,
                'leg_id': leg.leg_id,
            }
            return make_body('dpv_busy', context)

    dpv.dvg_name = None
    dvg.total_size -= dpv.total_size
    dvg.free_size -= dpv.free_size
    db.session.add(dpv)
    db.session.add(dvg)
    db.session.commit()
    return make_body('success'), 200


def handle_dvg_put(params, args):
    dvg_name = params[0]
    if args['action'] == 'extend':
        return dvg_extend(dvg_name, args['dpv_name'])
    else:
        return dvg_reduce(dvg_name, args['dpv_name'])


def handle_dvg_delete(params, args):
    dvg_name = params[0]
    try:
        dvg = DistributeVolumeGroup \
            .query \
            .with_lockmode('update') \
            .filter_by(dvg_name=dvg_name) \
            .one()
    except NoResultFound:
        return make_body('not_exist'), 404

    dpv = None
    try:
        dpv = dvg \
            .dpvs \
            .with_entities(DistributePhysicalVolume.dpv_name) \
            .one()
    except NoResultFound:
        pass
    except MultipleResultsFound:
        return make_body('dvg_busy'), 403
    if dpv is not None:
        return make_body('dvg_busy'), 403

    dlv = None
    try:
        dlv = dvg \
            .dlvs \
            .with_entities(DistributeLogicalVolume.dlv_name) \
            .one()
    except NoResultFound:
        pass
    except MultipleResultsFound:
        return make_body('dvg_busy'), 403
    if dlv is not None:
        return make_body('dvg_busy'), 403

    db.session.delete(dvg)
    db.session.commit()
    return make_body('success'), 200


class Dvg(Resource):

    def get(self, dvg_name):
        return handle_dlvm_request([dvg_name], None, handle_dvg_get)

    def put(self, dvg_name):
        return handle_dlvm_request([dvg_name], dvg_put_parser, handle_dvg_put)

    def delete(self, dvg_name):
        return handle_dlvm_request([dvg_name], None, handle_dvg_delete)
