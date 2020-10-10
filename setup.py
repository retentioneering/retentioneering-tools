from setuptools import setup, find_packages

setup(
    name='retentioneering',
    version='2.0.2',
    license='Retentioneering Software Non-Exclusive License (License)',
    description='Product analytics and marketing optimization framework based on deep user trajectories analysis',
    long_description="""
    Retentioneering is the framework to explore, grow and optimize your product based on deep analysis of user trajectories. Retentioneering provides systematic and quantitative approach to search for insights, continuous KPI optimization, product improvement and marketing optimization.

    Retentioneering analysis can:
    - Reveal why customers leave your product and reduce churn rate
    - Increase conversion rate and engagement as well as LTV, ARPU, NPS and ROI
    - Identify hidden patterns how customers use your app or website
    - Improve the quality of incoming traffic due to personalization of advertising and increase its conversion to applications
    """,

    author='Retentioneering User Trajectory Analysis Lab',
    author_email='retentioneering@gmail.com',
    url='https://github.com/retentioneering/retentioneering-tools',
    keywords=['ANALYTICS', 'CLICKSTREAM', 'RETENTIONEERING', 'RETENTION', 'GRAPHS', 'TRAJECTORIES', 'PREDICTIVE-ANALYTICS', 'CUSTOMER-SEGMENTATION'],
    install_requires=[
        'pandas>=1.1.1',
        'numpy>=1.19.1',
        'networkx>=2.4',
        'seaborn>=0.11.0',
        'matplotlib>=3.3.1',
        'scikit-learn>=0.23.2',
        'statsmodels>=0.12.0',
        'scipy',
        'altair',
        'vega',
        'pymongo',
        'plotly',
        'tqdm',
        'matplotlib',
        'umap-learn'
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: Unix',
        'Operating System :: MacOS'

    ],
    python_requires=">=3.6",
    packages=find_packages(),


    package_data={'retentioneering': ['datasets/data/*']},
    include_package_data=True
)
