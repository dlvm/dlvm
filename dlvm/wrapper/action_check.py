import enum


class Action(enum.Enum):
    dlv_attach = 'dlv_attach'
    dlv_detach = 'dlv_detach'
    dlv_delete = 'dlv_delete'


checker_dict = {
    Action.dlv_attach: [],
    Action.dlv_detach: [],
    Action.dlv_delete: [],
}


def add_checker(action):

    def add_checker_to_action(func):
        checker_dict[action].append(func)
        return func

    return add_checker_to_action


def run_checker(action, arg):
    for func in checker_dict[action]:
        func(arg)
