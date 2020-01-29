from setuptools import setup, find_packages

setup(
    name='retentioneering',
    version='1.0.7.3',
    license='Mozilla Public License',
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
    author_email='maxim.godzi@gmail.com',
    url='https://github.com/retentioneering/retentioneering-tools',
    keywords=['ANALYTICS', 'CLICKSTREAM', 'RETENTIONEERING', 'RETENTION', 'GRAPHS', 'TRAJECTORIES', 'PREDICTIVE-ANALYTICS', 'CUSTOMER-SEGMENTATION'],
    install_requires=[
        'pandas>=0.24.2',
        'numpy>=1.16.1',
        'networkx>=2.3',
        'seaborn>=0.9.0',
        'scikit-learn>=0.20.2',
        'altair',
        'vega',
        'pymongo'
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)"

    ],
    packages=find_packages()
)
