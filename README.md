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

## Config settings (env variables):

**Required**

```API_OPT_FIELDS``` - Fields to parse from feed (need for crawler)
```PUBLIC_API_HOST``` - API host on which chronograph will iterate by feed (need for crawler also)
```MONGODB_URL``` - String of connection to database (need for crawler also)

**Optional**
- ```CRAWLER_USER_AGENT``` - Set value of variable to all requests header `User-Agent`
- ```MONGODB_DATABASE``` - Name of database
- ```MONGODB_CONTRACTS_COLLECTION``` - Name of collection where will be stored processed contracts
- ```MONGODB_CONFIG_COLLECTION``` - Name collection for chronograph settings (weekends and max streams count)
- ```API_TOKEN``` - Service access token to CDB
- ```USER_AGENT``` - Value of header to be added to requests

**Doesn't set by env**
- ```ERROR_INTERVAL``` - timeout interval between requests if something goes wrong and need to retry


## Workflow

Service takes contracts from tender and post them to `/contracts`
