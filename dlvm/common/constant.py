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
