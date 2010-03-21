import pkg_resources
from pylons import config

__MODEL_VARS = ['setup',
                'GitObject',
                'Blob',
                'Tree',
                'Tag',
                'Commit',
                'Repository']

__BACKEND_NAME = config.get('backend', 'database')
__BACKEND_EP = pkg_resources.iter_entry_points('anygit.backend', __BACKEND_NAME).next()
__BACKEND = __BACKEND_EP.load()

for __VAR in __MODEL_VARS:
    globals()[__VAR] = getattr(__BACKEND, __VAR)
