# Helper targets

.PHONY: venv install yaml check run-extract run-update docker-build

venv:
	python -m venv .venv

install: venv
	. .venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

yaml:
	python - << 'PY'
	import yaml
	print('Loading detectors.yaml ...')
	yaml.safe_load(open('detectors.yaml','r',encoding='utf-8'))
	print('OK')
	PY

check:
	python -m py_compile es_pii_extract_update.py

run-extract:
	python es_pii_extract_update.py --es-url $$ES_URL --index $$ES_INDEX --user $$ES_USER --password $$ES_PASSWORD --detectors-yaml detectors.yaml --out pii_extract.csv --dedupe

run-update:
	python es_pii_extract_update.py --es-url $$ES_URL --index $$ES_INDEX --user $$ES_USER --password $$ES_PASSWORD --detectors-yaml detectors.yaml --field-map "NAS=nas_norm,EMAIL=emails,PHONE_CA=phones,POSTAL_CA=postal_codes,QC_RAMQ=ramq,QC_PERM_CODE=qc_perm_code,STUDENT_ID=student_ids,URL_HTTP=urls,URL_WWW=urls" --apply-updates --out pii_extract.csv --dedupe

docker-build:
	docker build -t es-pii-extract:latest .
