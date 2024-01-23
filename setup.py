from setuptools import setup

requirements = open('requirements.txt').read().split()
version = open('VERSION').read().strip()

setup(
    name="malevich",
    version=version,
    description=(
        "A user-friendly interface to communicate with Malevich AI (malevich.ai)",
    ),
    entry_points={
        'console_scripts': [
            'malevich = malevich.cli:main',
        ]
    },
    install_requires=requirements,
    package_data={
        "malevich": ["**/*.yml", "_templates/*"]
    }
)
