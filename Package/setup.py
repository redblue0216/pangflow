from setuptools import setup,find_packages



# read requirements.txt
with open("requirements.txt", "r", encoding="utf-8") as f:
    requirements = f.read().splitlines()



setup(
        ### information about package and author.
        name = 'pangflow',
        version = '0.1.1',
        author = 'shihua',
        author_email = "15021408795@163.com",
        python_requires = ">=3.13.5",
        license = "pangflow license",

        ### source codes and dependencies
        packages = find_packages(),
        include_package_data = True,
        description = 'pangflow is a workflow management tool that primarily utilizes CLI to schedule and trigger workflows. Its main technologies include Prefect, CLI, TOML, and SQLite',
        install_requires = requirements,

        ### package entry point and CLI index
        entry_points = {
            'console_scripts': [
                'pangflowctl = pangflow.cli.main:cli'
            ]
        }      
)
