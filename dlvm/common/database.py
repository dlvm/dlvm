from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dlvm.common.configure import cfg

engine = create_engine(cfg.db_uri)
Session = sessionmaker(bind=engine)
