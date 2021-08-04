import setuptools

setuptools.setup(packages=setuptools.find_packages(),
                 install_requires=['python-dateutil', 'apache-beam[gcp]'])
