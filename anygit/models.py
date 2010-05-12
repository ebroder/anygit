import pkg_resources
from pylons import config

# Top level names to import
__MODEL_VARS = ['create_schema',
                'setup',
                'flush',
                'destroy_session',
                'GitObject',
                'Blob',
                'Tree',
                'Tag',
                'Commit',
                'Repository']

# Get the first (and only) entry point, and extract the given
# names into our namespace.
__BACKEND_NAME = config.get('backend', 'mongodb')
__BACKEND_EP = pkg_resources.iter_entry_points('anygit.backend', __BACKEND_NAME).next()
__BACKEND = __BACKEND_EP.load()

for __VAR in __MODEL_VARS:
    globals()[__VAR] = getattr(__BACKEND, __VAR)
