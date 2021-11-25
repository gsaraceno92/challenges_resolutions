# Challenges_resolutions

Project used to complete a task using python.

## Requirements

- Python ^3.8

Install all necessary dependencies using pip3:

```bash
pip install -r requirements.txt
```

**Note: there is also the possibility to use [poetry](https://python-poetry.org/docs/basic-usage/#installing-with-poetrylock) in order to install all the requirements.**

## Usage

The script can be launched passing some parameters, e.g. in order to define a custom mysql port:

```bash
python3 bank_process.py --port <port>
```

For further info you can launch:

```bash
python3 bank_process.py --help
```

The script will create a folder **_reports_** inside the project and inside a folder named with the actual date; this last folder will contain the files (one for each user)  with the operations grouped by day and with the specific amount.