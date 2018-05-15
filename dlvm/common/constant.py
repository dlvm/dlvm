import os

LC_PATH = os.environ.get('DLVM_CONF', '/etc/dlvm')
DEFAULT_CFG_FILE = 'default.cfg'
DLVM_CFG_FILE = 'dlvm.cfg'
LOGGER_CFG_FILE = 'logger.json'
CELERY_CFG_FILE = 'celery.json'
SQLALCHEMY_CFG_FILE = 'sqlalchemy.json'

CELERY_APP_NAME = 'dlvm_celery_app'

RES_NAME_REGEX = r'[a-z,A-Z][a-z,A-Z,_]*'
RES_NAME_LENGTH = 32
DNS_NAME_LENGTH = 64

DEFAULT_SNAP_NAME = 'base'

MAX_GROUP_SIZE = 1024*1024*1024*1024
MIN_THIN_BLOCK_SIZE = 64*1024
MAX_BM_SIZE = MAX_GROUP_SIZE // MIN_THIN_BLOCK_SIZE
MAX_THIN_MAPPING = 100*1024*1024

API_LOGGER_NAME = 'dlvm_api'
DPV_LOGGER_NAME = 'dlvm_dpv'
IHOST_LOGGER_NAME = 'dlvm_ihost'
WORKER_LOGGER_NAME = 'dlvm_worker'
MONITOR_LOGGER_NAME = 'dlvm_minotor'
