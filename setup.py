from setuptools import setup

requirements = open('requirements.txt').read().split()

setup(
    name="malevich",
    version="0.0.1",
    description=(
        "A user-friendly interface to communicate with Malevich AI (malevich.ai)",
    ),
    entry_points={
        'console_scripts': [
            'malevich = malevich.cli:main',
        ]
    },
    install_requires=requirements
)
