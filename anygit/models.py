import pkg_resources

__MODEL_VARS = ['GitObject',
                'Blob',
                'Tree',
                'Tag',
                'Commit',
                'Repository']

__BACKEND_EP = pkg_resources.iter_entry_points('anygit.backend', 'database').next()
__BACKEND = __BACKEND_EP.load()

for __VAR in __MODEL_VARS:
    globals()[__VAR] = getattr(__BACKEND, __VAR)
