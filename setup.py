from setuptools import setup, find_packages

setup(
    name='comdm',
    version='1.0.0',
    description='Cisco Commercial Sales Data Manager',
    url='https://github.com/jeydurai',
    author='Jeyaraj Durairaj',
    author_email='jeyaraj.durairaj@gmail.com',
    packages=find_packages(),
    install_requires=['Click', 'pymongo', 'pandas', 'numpy', 'xlrd', 
        'xlsxwriter'],
    entry_points = {
        'console_scripts': [
            'comdm=comdm.comdm:main'
        ]
    }
)
