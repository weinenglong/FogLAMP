from setuptools import setup

with open('requirements.txt') as f:
    requires = f.read().splitlines()

setup(
    name='FogLAMP',
    version='0.1',
    description='FogLAMP',
    url='http://github.com/foglamp/FogLAMP',
    author='OSIsoft, LLC',
    author_email='info@dianomic.com',
    license='Apache 2.0',
    install_requires=requires,
    packages=[
    'foglamp.translators',
    'foglamp.device',
    'foglamp.data_purge',
    'foglamp.admin_api',
    'foglamp',
    'foglamp.core.api',
    'foglamp.core',
    'foglamp.core.service_registry',
    ],
    entry_points={
        'console_scripts': [
            'foglamp = foglamp.core.server_daemon:main'
        ],
    },
    zip_safe=False

)

