from setuptools import setup, find_packages

setup(
    name='retentioneering',
    version='1.0.4',
    license='Mozilla Public License',
    description='Python package for user trajectories analysis in the app',
    long_description='Python package for user trajectories analysis in the app',
    author='Retentioneering User Trajectory Analysis Lab',
    author_email='maxim.godzi@gmail.com',
    url='https://github.com/retentioneering/retentioneering-tools',
    keywords=['ANALYTICS', 'CLICKSTREAM', 'RETENTIONEERING', 'RETENTION', 'GRAPHS', 'TRAJECTORIES'],
    install_requires=[
        'pandas>=0.24.2',
        'numpy>=1.16.1',
        'networkx>=2.3',
        'seaborn>=0.9.0',
        'scikit-learn>=0.20.2',
        'shap>=0.29.1',
        'eli5>=0.8.2',
        'plotly>=4.1.0',
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)"

    ],
    packages=find_packages()
)