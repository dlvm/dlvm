import os
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dlvm.common.constant import LC_PATH, SQLALCHEMY_CFG_FILE
from dlvm.common.configure import cfg

sqlalchemy_cfg_path = os.path.join(LC_PATH, SQLALCHEMY_CFG_FILE)

if os.path.isfile(sqlalchemy_cfg_path):
    with open(sqlalchemy_cfg_path) as f:
        sqlalchemy_kwargs = json.load(f)
else:
    sqlalchemy_kwargs = {}

engine = create_engine(cfg.get('database', 'db_uri'), **sqlalchemy_kwargs)
Session = sessionmaker(bind=engine)
