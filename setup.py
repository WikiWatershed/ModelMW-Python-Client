from setuptools import setup, find_packages

VERSION = '0.0.1'
DESCRIPTION = 'A python client for accessing analysis and modeling data from ModelMyWatershed'
LONG_DESCRIPTION = 'A python client for accessing analysis and modeling data from ModelMyWatershed'

# Setting up
setup(
        name="modelmw_client",
        version=VERSION,
        author="Sara Geleskie Damiano",
        author_email="<sdamiano@stoudcenter.org>",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=['requests', 'pandas'],


        keywords=['ModelMyWatershed', 'WikiWatershed', 'ModelMW'],
        classifiers= [
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Education",
            "Programming Language :: Python :: 3",
            "Operating System :: Microsoft :: Windows",
        ]
)