from dlvm.wrapper.mq_wrapper import get_celery_app
from dlvm.wrapper.state_machine import sm_register
from dlvm.worker.dlv import DlvCreate, DlvDelete

app = get_celery_app()

sm_register(DlvCreate)
sm_register(DlvDelete)
