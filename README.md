# Prozorro bridge contracting


## Docker install

```
docker compose build
docker compose up -d
```


## Manual install

1. Install requirements

```
virtualenv -p python3.8.2 venv
source venv/bin/activate
pip install -r requirements.txt
pip install .
```

2. Set variables in **settings.py**

3. Run application

```
python -m prozorro_bridge_contracting.main
```

## Tests and coverage 

```
coverage run --source=./src/prozorro_bridge_contracting -m pytest tests/main.py
```
