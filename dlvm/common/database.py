from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dlvm.common.configure import cfg

engine = create_engine(cfg.get('general', 'db_uri'))
Session = sessionmaker(bind=engine)
