from setuptools import setup 
setup( 
    name='pgdummy', 
    version='0.5.3',
    author='Prem',
    author_email='contactprem@gmail.com',
    license='MIT',
    packages=['pgdummy'],
    python_requires=">=3.0",
    install_requires = [
        'pglast >=3.1, <4.0',
        'pyyaml >=6.0',
        'faker >=14.0',
    ],
    entry_points={
        "console_scripts": ["pgdummy=pgdummy.fakedata:cli_execute"]
    },
)
