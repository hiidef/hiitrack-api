from setuptools import setup, find_packages

setup(

    name = "hiitrack",

    version = "0.0.1",

    packages = find_packages(),

    dependency_links = [
        'http://github.com/Amper/cityhash/tarball/master#egg=cityhash-0.1',
        'http://github.com/steiza/txroutes/tarball/master#egg=txroutes-0.0.2',
        'http://github.com/driftx/Telephus/tarball/master#egg=telephus-0.8.0'],

    install_requires = [
        'Cython>=0.15.1',
        'Routes>=1.12.3',
        'Twisted>=11.1.0',
        'thrift>=0.8.0',
        'ujson>=1.15',
        'cityhash>=0.1',
        'txroutes>=0.0.2',
        'telephus>=0.8.0'],

    include_package_data = True,

    # metadata for upload to PyPI
    author = "John Wehr",
    author_email = "johnwehr@gmail.com",
    description = "HiiTrack",
    license = "MIT License",
    keywords = "",
)