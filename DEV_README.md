### Install venv

```bash
python -m venv venv
```

### active venv

```bash
venv\Scripts\activate.bat
```

### install dependencies

```bash
python -m pip install --upgrade --force-reinstall -r requirements.txt
```

### build project

# to execute junit tests

```bash
python -m pytest tests/
```

```bash
python -m build
```

### dump dependencies periodically if dependencies changed

```bash
python -m pip freeze > requirements.txt
```


# execute linter

```bash
# stop the build if there are Python syntax errors or undefined names
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics --exclude=.svn,CVS,.bzr,.hg,.git,__pycache__,.tox,.eggs,*.egg,*.sql,venv
# exit-zero treats all errors as warnings. The GitHub editor is 127 chars wide
flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics --exclude=.svn,CVS,.bzr,.hg,.git,__pycache__,.tox,.eggs,*.egg,*.sql,venv
```
