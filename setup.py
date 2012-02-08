from setuptools import setup, find_packages

setup(

    name = "hiitrack",

    version = "0.1.0",

    packages = find_packages(),

    dependency_links = [
        'https://github.com/hiidef/pylogd/zipball/master#egg=pylogd-0.3',
        'https://github.com/steiza/txroutes/zipball/master#egg=txroutes-0.0.2',
        'https://github.com/hiidef/Telephus/zipball/master#egg=telephus-1.0.0',
        'https://github.com/hiidef/cityhash/zipball/master#egg=cityhash-0.2.0'],

    install_requires = [
        'Routes>=1.12.3',
        'Twisted>=11.1.0',
        'thrift>=0.8.0',
        'ujson>=1.15',
        'cityhash>=0.2.0',
        'telephus>=1.0.0',
        'txroutes>=0.0.2',
        'ordereddict>=1.1',
        'pylogd>=0.3'],

    include_package_data = True,

    # metadata for upload to PyPI
    author = "John Wehr",
    author_email = "johnwehr@gmail.com",
    description = "HiiTrack",
    license = "MIT License",
    keywords = "",
)