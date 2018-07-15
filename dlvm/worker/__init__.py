from dlvm.common.configure import cfg
from dlvm.wrapper.mq_wrapper import get_celery_app
from dlvm.wrapper.state_machine import sm_register
from dlvm.worker.dlv import DlvCreate, DlvDelete, DlvAttach, DlvDetach
from dlvm.worker.lock_handler_task import lock_handler
from dlvm.worker.dpv_handler_task import dpv_handler

app = get_celery_app()

sm_register(DlvCreate)
sm_register(DlvDelete)
sm_register(DlvAttach)
sm_register(DlvDetach)

monitor_queue = cfg.get('monitor', 'queue')

lock_handler_interval = cfg.getfloat('monitor', 'lock_handler_interval')
lock_handler_batch = cfg.getfloat('monitor', 'lock_handler_batch')

lock_handler = app.task(name='dlvm_lock_handler')(lock_handler)


dpv_handler_interval = cfg.getfloat('monitor', 'dpv_handler_interval')
dpv_handler_batch = cfg.getfloat('monitor', 'dpv_handler_batch')
dpv_handler = app.task(name='dlvm_dpv_handler')(dpv_handler)


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        lock_handler_interval, lock_handler.signature(
            args=(lock_handler_batch,), queue=monitor_queue))
    sender.add_periodic_task(
        dpv_handler_interval, dpv_handler.signature(
            args=(dpv_handler_batch,), queue=monitor_queue))
