have python3.10 installed

create a virtual enviroment with python 3.10:

```bash
pip install virtualenv
virtualenv venv -p=python3.10
source venv/bin/activate


pip install -r requirements.txt
```

TO add new packages:

```bash
pip install package
pip freeze > requirements.txt
```
