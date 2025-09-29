# Elasticsearch PII Extract & Update

This project provides a **Python utility** and a set of **regex detectors** for identifying and extracting sensitive
information (PII — Personally Identifiable Information) in **Canada / Québec**, with a focus on the **education sector**.

The tool can:
- Parse documents already indexed in **Elasticsearch** by **FSCrawler: https://github.com/dadoonet/fscrawler**.

- Extract sensitive fields such as NAS, RAMQ, permanent code, student IDs, emails, phone numbers, postal codes, IPs, credit cards, etc.
- Write results to a **CSV** for auditing.
- Optionally, update the original documents in Elasticsearch by appending normalized values to specific fields (without overwriting existing data).

## Files

- `es_pii_extract_update.py`: Main Python script.
- `detectors.yaml`: Regex definitions for sensitive data patterns in Canada/Québec.
- `pii_test_samples.txt`: Sample file containing synthetic test data.
- `README.md`: This documentation.

## Requirements

- Python 3.9+
- Dependencies:
  ```bash
  pip install requests pyyaml
  ```

## Usage

### Extract only (CSV output)

```bash
python es_pii_extract_update.py   --es-url http://localhost:9200   --index index_name   --user elastic --password secret   --detectors-yaml detectors.yaml   --out pii_extract.csv   --dedupe
```

### Extract + Update Elasticsearch documents

```bash
python es_pii_extract_update.py   --es-url http://localhost:9200   --index index_name   --user elastic --password secret   --apply-updates   --detectors-yaml detectors.yaml   --field-map "NAS=nas_norm,EMAIL=emails,PHONE_CA=phones,POSTAL_CA=postal_codes,QC_RAMQ=ramq,QC_PERM_CODE=qc_perm_code,STUDENT_ID=student_ids,URL_HTTP=urls,URL_WWW=urls"   --out pii_extract.csv   --dedupe
```

- `--apply-updates`: Enables bulk updates in Elasticsearch.
- `--detectors-yaml`: Loads custom detectors from the YAML file.
- `--field-map`: Maps detector names to Elasticsearch field names.

### Test with synthetic data

The repository includes `pii_test_samples.txt`, which contains fake examples of PII data that should be detected.

## Mapping notes

Before running updates, ensure the fields exist in Elasticsearch with the right type (preferably `keyword`):

```json
PUT index_name/_mapping
{
  "properties": {
    "nas_norm": { "type": "keyword" },
    "emails":   { "type": "keyword" },
    "phones":   { "type": "keyword" },
    "postal_codes": { "type": "keyword" },
    "ramq": { "type": "keyword" },
    "qc_perm_code": { "type": "keyword" },
    "student_ids": { "type": "keyword" },
    "urls": { "type": "keyword" }
  }
}
```

## License

This project is released under the MIT License.

---

⚠️ **Disclaimer**: The regex patterns provided are heuristics, not official validators.  
False positives or negatives may occur. Always validate results before relying on them in production.


## Docker

Build and run:

```bash
make docker-build
# Show help
docker run --rm es-pii-extract:latest --help
```

Run extract (example):

```bash
docker run --rm -e ES_URL=http://host.docker.internal:9200 -e ES_INDEX=index_name \
  -e ES_USER=elastic -e ES_PASSWORD=secret es-pii-extract:latest \
  --es-url $ES_URL --index $ES_INDEX --user $ES_USER --password $ES_PASSWORD \
  --detectors-yaml detectors.yaml --out /app/pii_extract.csv --dedupe
```

## GitHub Actions
This repo includes `.github/workflows/ci.yml` which validates the YAML, checks Python syntax, and runs tests.

## Windows (PowerShell)
Example command:

```powershell
python es_pii_extract_update.py `
  --es-url https://127.0.0.1:9200 `
  --index index_name `
  --user elastic `
  --password "secret" `
  --no-verify-tls `
  --detectors-yaml detectors.yaml `
  --apply-updates `
  --field-map "NAS=nas_norm,EMAIL=emails,PHONE_CA=phones,POSTAL_CA=postal_codes,QC_RAMQ=ramq,QC_PERM_CODE=qc_perm_code,STUDENT_ID=student_ids,URL_HTTP=urls,URL_WWW=urls" `
  --out pii_extract.csv --dedupe
```


## License
This project is licensed under the MIT License — see [LICENSE](LICENSE).


## Kibana / Index Alias Example
Switch an alias `cs_current` from `index_name` to `index_name_v2`:

```bash
POST /_aliases
{
  "actions": [
    {"remove": {"index": "index_name", "alias": "cs_current"}},
    {"add":    {"index": "index_name_v2", "alias": "cs_current"}}
  ]
}
```
