import os
import json

from celery import Celery

from dlvm.common.constant import CELERY_APP_NAME, LC_PATH, CELERY_CFG_FILE
from dlvm.common.configure import cfg
from dlvm.common.loginit import loginit

app = Celery(
    CELERY_APP_NAME,
    broker=cfg.get('mq', 'broker'),
    include=['dlvm.wrapper.state_machine'],
)


celery_cfg_path = os.path.join(LC_PATH, CELERY_CFG_FILE)
if os.path.isfile(celery_cfg_path):
    with open(celery_cfg_path) as f:
        celery_kwargs = json.load(f)
else:
    celery_kwargs = {}


app.conf.update(**celery_kwargs)


def get_celery_app():
    loginit()
    return app
