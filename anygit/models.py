import pkg_resources
from pylons import config

from anygit.backends import common

# Top level names to import
__BACKEND_VARS = ['create_schema',
                  'setup',
                  'flush',
                  'destroy_session']

__COMMON_VARS = ['GitObject',
                'Blob',
                'Tree',
                'Tag',
                'Commit',
                'Repository',
                'BlobTree',
                'BlobTag',
                'TreeParentTree',
                'TreeCommit',
                'TreeTag',
                'CommitParentCommit',
                'CommitTree',
                'CommitTag',
                'TagParentTag',
                'Aggregate']

# Get the first (and only) entry point, and extract the given
# names into our namespace.
__BACKEND_NAME = config.get('backend', 'mongodb')
__BACKEND_EP = pkg_resources.iter_entry_points('anygit.backend', __BACKEND_NAME).next()
__BACKEND = __BACKEND_EP.load()

for module, variables in [(common, __COMMON_VARS), (__BACKEND, __BACKEND_VARS)]:
    for variable in variables:
        globals()[variable] = getattr(module, variable)
del module, variable, variables
