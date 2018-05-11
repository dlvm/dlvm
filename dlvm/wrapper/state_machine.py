from typing import NamedTuple, Set, Tuple, Type, Mapping
from abc import ABCMeta, abstractmethod
import sys
import enum
import uuid
from logging import getLogger, LoggerAdapter

from marshmallow import fields

from dlvm.common.configure import cfg
from dlvm.common.utils import RequestContext, ExcInfo
from dlvm.common.marshmallow_ext import NtSchema, SetField, EnumField
from dlvm.common.database import Session, acquire_lock, release_lock, \
    verify_lock
from dlvm.wrapper.hook import build_hook_list, run_pre_hook, \
    run_post_hook, run_error_hook
from dlvm.wrapper.local_ctx import frontend_local, Direction, WorkerContext
from dlvm.wrapper.mq_wrapper import app


ori_logger = getLogger('state_machine')
sm_send_hook_list = build_hook_list('sm_send_hook')
sm_recv_hook_list = build_hook_list('sm_recv_hook')


class SmSendContext(NamedTuple):
    req_ctx: RequestContext
    args: Tuple
    queue: str


class SmRecvContext(NamedTuple):
    req_ctx: RequestContext
    sm_ctx_d: Mapping
    sm_arg_d: Tuple


class UniDirJob(metaclass=ABCMeta):

    @abstractmethod
    def __init__(self, sm_arg):
        raise NotImplementedError

    @abstractmethod
    def forward(self):
        raise NotImplementedError


class BiDirJob(metaclass=ABCMeta):

    @abstractmethod
    def __init__(self, res_id):
        raise NotImplementedError

    @abstractmethod
    def forward(self):
        raise NotImplementedError

    @abstractmethod
    def backward(self):
        raise NotImplementedError


class UniDirState(NamedTuple):
    job_cls: Type[UniDirJob]
    f_state: str


class BiDirState(NamedTuple):
    job_cls: Type[BiDirJob]
    f_state: str
    b_state: str


class StepType(enum.Enum):
    forward = 'forward'
    backward = 'backward'
    enforce = 'enforce'


class StateMachineContextSchema(NtSchema):
    sm_name = fields.String()
    state_name = fields.String()
    step_type = EnumField(StepType)
    retries = fields.Integer()
    worklog = SetField(fields.String())
    lock_id = fields.Integer()
    lock_owner = fields.String()
    lock_dt = fields.DateTime()


class StateMachineContext(NamedTuple):
    sm_name: str
    state_name: str
    step_type: StepType
    retries: int
    worklog: Set[str]
    lock_id: int
    lock_owner: str


class SmRetry(Exception):

    def __init__(self, message=None):
        self.message = message
        super(SmRetry, self).__init__(message)


class DoRetry(Exception):

    def __init__(self, args, message):
        self.args = args
        self.message = message
        super(DoRetry, self).__init__(message)


class EnforceError(Exception):
    pass


sm_dict = {}

forward_max_retries = cfg.getint('mq', 'forward_max_retries')
backward_max_retries = cfg.getint('mq', 'backward_max_retries')
enforce_max_retries = cfg.getint('mq', 'enforce_max_retries')
max_retries = cfg.getint('mq', 'max_retries')
assert(max_retries > (
    forward_max_retries + backward_max_retries + enforce_max_retries) * 10)
retry_delay = cfg.getint('mq', 'retry_delay')


def build_worker_ctx(sm_ctx, state):
    worklog = sm_ctx.worklog
    if sm_ctx.step_type == StepType.forward:
        direction = Direction.forward
        enforce = False
    elif sm_ctx.step_type == StepType.backward:
        direction = Direction.backward
        enforce = False
    else:
        if isinstance(state, UniDirState):
            direction = Direction.forward
        else:
            assert(isinstance(state, BiDirState))
            direction = Direction.backward
        enforce = True
    return WorkerContext(
        worklog, direction, enforce, sm_ctx.lock_owner, sm_ctx.lock_dt)


def update_for_failed(sm_ctx, state):
    if sm_ctx.step_type == StepType.forward:
        if sm_ctx.retries < forward_max_retries:
            return sm_ctx._replace(retries=sm_ctx.retries+1)
        else:
            if isinstance(state, UniDirState):
                return sm_ctx._replace(step_type=StepType.enforce, retries=0)
            else:
                assert(isinstance(state, BiDirState))
                return sm_ctx._replace(step_type=StepType.backward, retries=0)
    elif sm_ctx.step_type == StepType.backward:
        assert(isinstance(state, BiDirState))
        if sm_ctx.retries < backward_max_retries:
            return sm_ctx._replace(retries=sm_ctx.retries+1)
        else:
            return sm_ctx._replace(step_type=StepType.enforce, retries=0)
    else:
        if sm_ctx.retries < enforce_max_retries:
            return sm_ctx._replace(retries=sm_ctx.retries+1)
        else:
            raise EnforceError


def update_for_succeed(sm_ctx, state):
    if sm_ctx.step_type == StepType.forward:
        next_state = state.f_state
    elif sm_ctx.step_type == StepType.backward:
        next_state = state.b_state
    else:
        if isinstance(state, UniDirState):
            next_state = state.f_state
        else:
            next_state = state.b_state
    return sm_ctx._replace(
        state_name=next_state,
        step_type=StepType.forward,
        retries=0, worklog=set())


@app.task(bind=True, max_retries=max_retries, default_retry_delay=retry_delay)
def sm_handler(self, req_id_str, sm_ctx_d, res_id):
    req_id = uuid.UUID(req_id_str)
    logger = LoggerAdapter(ori_logger, {'req_id': req_id})
    req_ctx = RequestContext(req_id, logger)
    frontend_local.req_ctx = req_ctx
    session = Session()
    frontend_local.session = session
    hook_ctx = SmRecvContext(
        req_ctx, sm_ctx_d, res_id)
    hook_ret_dict = run_pre_hook(
        'sm_recv', sm_recv_hook_list, hook_ctx)
    try:
        sm_ctx = StateMachineContextSchema().load(sm_ctx_d)
        sm = sm_dict[sm_ctx.sm_name]
        new_owner, lock_dt = acquire_lock(
            session, sm_ctx.lock_id, sm_ctx.lock_owner)
        sm_ctx = sm_ctx._replace(lock_owner=new_owner, lock_dt=lock_dt)
        while sm_ctx.state_name != 'stop':
            state = sm[sm_ctx.state_name]
            worker_ctx = build_worker_ctx(sm_ctx, state)
            job = state.job_cls(res_id)
            if worker_ctx.direction == Direction.forward:
                func = job.forward
            else:
                func = job.backward
            frontend_local.worker_ctx = worker_ctx
            try:
                func()
            except SmRetry as e:
                sm_ctx = update_for_failed(sm_ctx, state)
                sm_ctx_d = StateMachineContextSchema().dump(sm_ctx)
                args = (req_id_str, sm_ctx_d, res_id)
                raise DoRetry(args, e.message)
            else:
                verify_lock(session, sm_ctx.lock_id, sm_ctx.lock_owner)
                session.commit()
                sm_ctx = update_for_succeed(sm_ctx, state)

        release_lock(session, sm_ctx.lock_id, sm_ctx.lock_owner, res_id)
    except Exception as e:
        etype, value, tb = sys.exc_info()
        exc_info = ExcInfo(etype, value, tb)
        session.rollback()
        run_error_hook(
            'sm_recv', sm_recv_hook_list, hook_ctx,
            hook_ret_dict, exc_info)
        if isinstance(e, DoRetry):
            raise self.retry(args=e.args)
        else:
            raise e
    else:
        run_post_hook(
            'sm_recv', sm_recv_hook_list, hook_ctx,
            hook_ret_dict, None)
    finally:
        session.close()


class StateMachine(metaclass=ABCMeta):

    def __init__(self, res_id):
        self.res_id = res_id

    @classmethod
    @abstractmethod
    def get_sm_name(cls):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_queue(self):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_sm(cls):
        raise NotImplementedError

    def start(self, lock):
        sm_name = self.get_sm_name()
        queue = self.get_queue()
        req_id_str = str(frontend_local.req_ctx.req_id)
        sm_ctx = StateMachineContextSchema.nt(
            sm_name, 'start', StepType.forward, 0,
            set(), lock.lock_id, lock.lock_owner, lock.lock_dt)
        sm_ctx_d = StateMachineContextSchema().dump(sm_ctx)
        args = (req_id_str, sm_ctx_d, self.res_id)
        req_ctx = frontend_local.req_ctx
        hook_ctx = SmSendContext(
            req_ctx, args, queue)
        hook_ret_dict = run_pre_hook(
            'sm_send', sm_send_hook_list, hook_ctx)
        try:
            ret = sm_handler.apply_async(
                args=args, queue=queue)
        except Exception:
            etype, value, tb = sys.exc_info()
            exc_info = ExcInfo(etype, value, tb)
            run_error_hook(
                'sm_send', sm_send_hook_list, hook_ctx,
                hook_ret_dict, exc_info)
        else:
            run_post_hook(
                'sm_send', sm_send_hook_list, hook_ctx,
                hook_ret_dict, ret)


def sm_register(cls):
    sm_dict[cls.get_sm_name()] = cls.get_sm()
    return cls
