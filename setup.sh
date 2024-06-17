#!/bin/bash

pip install virtualenv
virtualenv venv -p=python3.10
source venv/bin/activate
pip install -r requirements.txt
deactivate
