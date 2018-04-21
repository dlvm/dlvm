from typing import NamedTuple, Type, Set
from abc import ABC, abstractmethod
import os
import sys
import uuid
import enum
from logging import getLogger, LoggerAdapter
import json

from celery import Celery
from marshmallow import Schema, fields, post_load
from marshmallow_enum import EnumField

from dlvm.common.constant import CELERY_APP_NAME, LC_PATH, CELERY_CFG_FILE
from dlvm.common.configuration import cfg
from dlvm.common.utils import RequestContext, ExcInfo
from dlvm.common.database import Session
from dlvm.wrapper.local_ctx import frontend_local, RpcError, WorkerContext, \
    Direction
from dlvm.wrapper.hook import build_hook_list, run_pre_hook, \
    run_post_hook, run_error_hook


class UniDirJob(ABC):

    @abstractmethod
    def __init__(self, job_args):
        raise NotImplementedError

    @abstractmethod
    def forward(self):
        raise NotImplementedError


class BiDirJob(ABC):

    @abstractmethod
    def __init__(self, job_args):
        raise NotImplementedError

    @abstractmethod
    def forward(self):
        raise NotImplementedError

    @abstractmethod
    def backward(self):
        raise NotImplementedError


class UniDirAction(NamedTuple):
    job_cls: Type[UniDirJob]
    f_state: str


class BiDirAction(NamedTuple):
    job_cls: Type[BiDirJob]
    f_state: str
    b_state: str


class StepType(enum.Enum):
    forward = 'forward'
    backward = 'backward'
    enforce = 'enforce'


class StateInfo(NamedTuple):
    sm_name: str
    action_name: str
    step_type: StepType
    retries: int
    backlog: Set
    lock_id: int
    lock_owner: str


class StateInfoSchema(Schema):
    sm_name = fields.String()
    action_name = fields.String()
    step_type = EnumField(StepType)
    retries = fields.Integer()
    backlog = fields.List(fields.String)
    lock_id = fields.Integer()
    lock_owner = fields.String()

    @post_load
    def make_state_info(self, data):
        data['backlog'] = set(data['backlog'])
        return StateInfo(**data)


class WorkerRecvContext(NamedTuple):
    req_ctx: RequestContext
    lock_owner: str
    si: StateInfo
    job_args: object


class State(enum.Enum):
    start = 'start'
    stop = 'stop'


state_machine_dict = {}


def state_machine_register(name, sm):
    state_machine_dict[name] = sm


app = Celery(CELERY_APP_NAME, broker=cfg.get('mq', 'broker'))

celery_cfg_path = os.path.join(LC_PATH, CELERY_CFG_FILE)
if os.path.isfile(celery_cfg_path):
    with open(celery_cfg_path) as f:
        celery_kwargs = json.load(f)
else:
    celery_kwargs = {}


app.conf.update(**celery_kwargs)


forward_max_retries = cfg.getint('mq', 'forward_max_retries')
backward_max_retries = cfg.getint('mq', 'backward_max_retries')
enforce_max_retries = cfg.getint('mq', 'enforce_max_retries')
max_retries = cfg.getint('mq', 'max_retries')
assert(max_retries > (
    forward_max_retries + backward_max_retries + enforce_max_retries) * 5)
retry_delay = cfg.getint('mq', 'retry_delay')


worker_recv_hook_list = build_hook_list('worker_recv_hook')
ori_logger = getLogger('dlvm_worker')


class JobRetry(Exception):

    def __init__(self, exc_info):
        self.exc_info = exc_info
        super(JobRetry, self).__init__()


def run_state_machine(sm, si):
    while si.state_name != State.stop:
        action = sm[si.sm_name]
        frontend_local.worker_ctx = build_worker_ctx(si)
        func = choose_func(si, action)
        func()
        update_for_succeed(si, action)


@app.task(bind=True, max_retries=max_retries, default_retry_delay=retry_delay)
def dlvm_mq_handler(
        self, req_id_hex, si, job_args):
    req_id = uuid.UUID(hex=req_id_hex)
    logger = LoggerAdapter(ori_logger, {'req_id': req_id})
    req_ctx = RequestContext(req_id, logger)
    session = Session()
    lock_owner = uuid.uuid4().hex
    hook_ctx = WorkerRecvContext(
        req_ctx, si, lock_owner, job_args)
    hook_ret_dict = run_pre_hook(
        'worker_recv', worker_recv_hook_list, hook_ctx)
    try:
        si = StateInfoSchema().load(si)
        frontend_local.req_ctx = req_ctx
        frontend_local.session = session
        acquire_lock(si.lock_id, si.lock_owner, lock_owner)
        si.lock_owner = lock_owner
        sm = state_machine_dict[si.sm_name]
        run_state_machine(sm, si)
    except Exception as e:
        etype, value, tb = sys.exc_info()
        exc_info = ExcInfo(etype, value, tb)
        session.rollback()
        run_error_hook(
            'worker_recv', worker_recv_hook_list,
            hook_ctx, hook_ret_dict, exc_info)
        if isinstance(e, RpcError):
            update_for_failed(si)
            si = StateInfoSchema().dump(si)
            args = (req_id_hex, si, job_args)
            raise self.retry(args=args)
        else:
            raise e
    else:
        run_post_hook(
            'worker_recv', worker_recv_hook_list,
            hook_ctx, hook_ret_dict, None)
        release_lock(si.lock_id, si.lock_owner)
    finally:
        session.close()
