from threading import local
import enum

backend_local = local()
frontend_local = local()


class Direction(enum.Enum):
    forward = 'forward'
    backward = 'backward'
