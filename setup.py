from setuptools import setup, find_packages

setup(
    name='anygit',
    version='0.0.0',
    description='Look up any git object',
    url='http://anyg.it',
    install_requires=[
        "Pylons>=0.9.7",
        "SQLAlchemy>=0.5",
    ],
    setup_requires=["PasteScript>=1.6.3"],
    packages=find_packages(),
    test_suite='nose.collector',
    zip_safe=False,
    paster_plugins=['PasteScript', 'Pylons'],
    entry_points="""
    [paste.app_factory]
    main = anygit.config.middleware:make_app

    [paste.app_install]
    main = pylons.util:PylonsInstaller

    [paste.paster_command]
    anygit = anygit.command:AnygitCommand

    [anygit.backend]
    database = anygit.backends.database
    """,
)
