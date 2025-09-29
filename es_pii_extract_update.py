#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
es_pii_extract_update.py
------------------------
Extraction modulaire (ex: NAS) depuis un index Elasticsearch + mise à jour des documents :
- Scroll API pour parcourir l’index
- Détecteurs extensibles (regex + normalisation)
- Sortie CSV (detector, value, path, _id)
- Mises à jour BULK optionnelles (ajout dans des tableaux sans doublon)

Exemples :
  # Extraire + mettre à jour les champs (NAS -> nas_norm)
  python es_pii_extract_update.py --es-url http://localhost:9200 --index INDEX_NAME \
      --user elastic --password secret --apply-updates --out nas_path.csv

  # Utiliser des détecteurs supplémentaires définis en YAML
  python es_pii_extract_update.py --es-url http://localhost:9200 --no-verify-tls --index INDEX_NAME \
      --user elastic --password secret --apply-updates \
      --detectors-yaml detectors.yaml \
      --field-map "NAS=nas_norm,EMAIL=emails" \
      --out pii.csv \
      --apply-updates --bulk-size 1000

#exec in powershell
      python es_pii_extract_update.py --es-url https://IP_ADDRESS:9200  --no-verify-tls --index INDEX_NAME `
      --user elastic --password xxxxx --apply-updates `
      --detectors-yaml detectors.yaml `
      --field-map "NAS=nas_norm,QC_RAMQ=ramq,QC_PERM_CODE=qc_perm_code,STUDENT_ID=student_ids,EMAIL=emails,PHONE_CA=phones,POSTAL_CA=postal_codes,URL_HTTP=urls,URL_WWW=urls" `
      --out pii.csv `
      --apply-updates --bulk-size 1000
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Any

import requests


# ---------- Normalisation ----------

UNICODE_DASHES = "\u2010\u2011\u2012\u2013\u2014"
NBSP_SET = {"\u00A0", "\u2007", "\u202F", "\u2009", "\u200B"}  # NBSP, figure, narrow, thin, zero-width


def normalize_separators(s: str) -> str:
    """Normalise les séparateurs courants (espaces/tirets unicode) dans un texte."""
    if not s:
        return s
    for ch in NBSP_SET:
        s = s.replace(ch, " ")
    dash_re = re.compile("[" + re.escape(UNICODE_DASHES) + "]")
    s = dash_re.sub("-", s)
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    return s


def unicode_digits_to_ascii(s: str) -> str:
    """Convertit des chiffres Unicode (category Nd) en ASCII 0-9, supprime le reste."""
    out = []
    for ch in s:
        if ch.isdigit():
            try:
                v = int(ch)
            except Exception:
                v = None
            if v is not None and 0 <= v <= 9:
                out.append(str(v))
    return "".join(out)


# ---------- Détecteurs ----------

@dataclass
class Detector:
    name: str
    pattern: re.Pattern
    normalizer: Optional[Callable[[str], Optional[str]]] = None
    desc: str = ""

    def find(self, text: str) -> Iterable[str]:
        for m in self.pattern.finditer(text):
            raw = m.group(0)
            val = self.normalizer(raw) if self.normalizer else raw
            if val:
                yield val


def make_nas_detector() -> Detector:
    """
    NAS canadien : capture 9 chiffres avec séparateurs optionnels (espace/tiret/underscore/point/slash),
    accepte chiffres unicode puis normalise en ###-###-###.
    """
    pat = re.compile(r"(\d[\d\-\s_./]{7,24}\d)|(\d{9})")

    def _norm(s: str) -> Optional[str]:
        digits = unicode_digits_to_ascii(s)
        if len(digits) != 9:
            return None
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:9]}"

    return Detector(
        name="NAS",
        pattern=pat,
        normalizer=_norm,
        desc="Canadian SIN ###-###-### (separators and unicode digits accepted).",
    )


def load_detectors_from_yaml(path: str) -> List[Detector]:
    """
    Charge des détecteurs depuis YAML (facultatif).
    Format YAML (exemple):
      - name: NAS
        regex: '(\\d[\\d\\-\\s_./]{7,24}\\d)|(\\d{9})'
        normalize: nas
      - name: EMAIL
        regex: '(?i)[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}'
        flags: IGNORECASE
    """
    try:
        import yaml  # type: ignore
    except Exception:
        print("PyYAML n'est pas installé. `pip install pyyaml` si tu veux utiliser --detectors-yaml", file=sys.stderr)
        return []

    with open(path, "r", encoding="utf-8") as f:
        spec = yaml.safe_load(f) or []

    detectors: List[Detector] = []
    for item in spec:
        name = item["name"]
        regex = item["regex"]
        flags = 0
        if "flags" in item:
            flags_map = {
                "IGNORECASE": re.IGNORECASE,
                "MULTILINE": re.MULTILINE,
                "DOTALL": re.DOTALL,
                "VERBOSE": re.VERBOSE,
            }
            fl = item["flags"]
            if isinstance(fl, str):
                flags |= flags_map.get(fl.upper(), 0)
            elif isinstance(fl, list):
                for f in fl:
                    flags |= flags_map.get(str(f).upper(), 0)
        pat = re.compile(regex, flags)

        normalizer = None
        if item.get("normalize") == "nas":
            def _norm_local(s: str) -> Optional[str]:
                digits = unicode_digits_to_ascii(s)
                return f"{digits[0:3]}-{digits[3:6]}-{digits[6:9]}" if len(digits) == 9 else None
            normalizer = _norm_local

        detectors.append(Detector(name=name, pattern=pat, normalizer=normalizer, desc=item.get("desc", "")))
    return detectors


# ---------- Client Elasticsearch (REST minimal) ----------

class ESClient:
    def __init__(
        self,
        base_url: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        api_key: Optional[str] = None,
        bearer: Optional[str] = None,
        ca_cert: Optional[str] = None,
        timeout: int = 60,
        verify_tls: bool = True,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.timeout = timeout
        self.verify = verify_tls if not ca_cert else ca_cert
        # auth
        if api_key:
            self.session.headers["Authorization"] = f"ApiKey {api_key}"
        elif bearer:
            self.session.headers["Authorization"] = f"Bearer {bearer}"
        elif user and password:
            self.session.auth = (user, password)
        self.session.headers["Content-Type"] = "application/json"

    def search_scroll(self, index: str, query: Dict[str, Any], size: int = 500, scroll: str = "2m"):
        """Générateur sur tous les hits via Scroll API."""
        url = f"{self.base_url}/{index}/_search?scroll={scroll}"
        body = {"size": size, **query}
        r = self.session.post(url, data=json.dumps(body), timeout=self.timeout, verify=self.verify)
        r.raise_for_status()
        data = r.json()
        scroll_id = data.get("_scroll_id")
        hits = data.get("hits", {}).get("hits", [])
        for h in hits:
            yield h
        while True:
            if not hits or not scroll_id:
                break
            r = self.session.post(
                f"{self.base_url}/_search/scroll",
                data=json.dumps({"scroll": scroll, "scroll_id": scroll_id}),
                timeout=self.timeout,
                verify=self.verify,
            )
            r.raise_for_status()
            data = r.json()
            scroll_id = data.get("_scroll_id")
            hits = data.get("hits", {}).get("hits", [])
            for h in hits:
                yield h

    def bulk(self, actions_ndjson: str):
        url = f"{self.base_url}/_bulk"
        headers = {"Content-Type": "application/x-ndjson"}
        r = self.session.post(url, data=actions_ndjson, headers=headers, timeout=self.timeout, verify=self.verify)
        # Montrer la réponse brute si status >= 400
        if r.status_code >= 400:
            print("=== BULK HTTP ERROR ===", file=sys.stderr)
            print(f"Status: {r.status_code}", file=sys.stderr)
            try:
                print(r.text[:4000], file=sys.stderr)   # affiche le début de la réponse ES
            except Exception:
                pass
            r.raise_for_status()
        data = r.json()
        if data.get("errors"):
            # Afficher 1-5 erreurs d’items
            items = data.get("items", [])
            errs = [it for it in items if any(v.get("error") for v in it.values())]
            msg = json.dumps(errs[:5], ensure_ascii=False, indent=2)
            print("=== BULK ITEM ERRORS ===", file=sys.stderr)
            print(msg, file=sys.stderr)
        return data


# ---------- Extraction + MAJ ----------

def get_text_from_source(src: Dict[str, Any], content_field: str, alt_field: Optional[str]) -> str:
    if content_field in src and src[content_field] is not None:
        return str(src[content_field])
    if alt_field:
        cur: Any = src
        for part in alt_field.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                cur = None
                break
        if cur:
            return str(cur)
    return ""


def get_path_virtual(src: Dict[str, Any], path_field: str = "path.virtual") -> str:
    cur: Any = src
    for p in path_field.split("."):
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return ""
    return str(cur)


def extract_from_text(text: str, detectors: List[Detector]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    if not text:
        return out
    text = normalize_separators(text)
    for det in detectors:
        for val in det.find(text):
            out.append((det.name, val))
    return out


def parse_field_map(s: Optional[str]) -> Dict[str, str]:
    """
    Convertit "NAS=nas_norm,EMAIL=emails" -> {"NAS":"nas_norm","EMAIL":"emails"}
    """
    if not s:
        return {}
    m: Dict[str, str] = {}
    for part in s.split(","):
        if not part.strip():
            continue
        if "=" in part:
            k, v = part.split("=", 1)
            m[k.strip()] = v.strip()
    return m


def target_field(detector_name: str, fmap: Dict[str, str], prefix: str) -> str:
    # priorité au mapping explicite
    if detector_name in fmap:
        return fmap[detector_name]
    # défaut: champ "pii.<nom>" en snake_case
    field = detector_name.lower().replace(" ", "_")
    # CAS SPÉCIAL: NAS -> nas_norm
    if field == "nas":
        return "nas_norm"
    return f"{prefix}{field}" if prefix else field


def build_update_script_params(field_to_values: Dict[str, List[str]]) -> Tuple[str, Dict[str, Any]]:
    """
    Construit le script painless + params pour ajouter des valeurs dans des tableaux sans doublon.
    """
    script = """
      def up = params.upd;
      for (entry in up.entrySet()) {
        def f = entry.getKey();
        def vals = entry.getValue();
        if (ctx._source[f] == null) { ctx._source[f] = new ArrayList(); }
        for (v in vals) {
          if (!ctx._source[f].contains(v)) { ctx._source[f].add(v); }
        }
      }
    """
    return script, {"upd": field_to_values}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Extraire des motifs (ex: NAS) depuis Elasticsearch, écrire un CSV et (optionnel) mettre à jour les documents.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument("--es-url", required=True, help="URL Elasticsearch (ex: http://localhost:9200)")
    p.add_argument("--index", required=True, help="Nom de l'index (ex: INDEX_NAME)")
    p.add_argument("--user", help="Utilisateur basic auth")
    p.add_argument("--password", help="Mot de passe basic auth")
    p.add_argument("--api-key", help="ApiKey base64")
    p.add_argument("--bearer", help="Bearer token")
    p.add_argument("--ca-cert", help="Chemin CA (si TLS self-signed)")
    p.add_argument("--no-verify-tls", action="store_true", help="Désactiver la vérification TLS")
    p.add_argument("--batch-size", type=int, default=500, help="Taille de lot pour le scroll")
    p.add_argument("--content-field", default="content", help="Champ texte principal (def: content)")
    p.add_argument("--alt-content-field", default="attachment.content", help="Champ texte alternatif")
    p.add_argument("--path-field", default="path.virtual", help="Champ chemin (def: path.virtual)")
    p.add_argument("--query-json", help="Fichier JSON de requête (_source, query, filtre, etc.)")
    p.add_argument("--out", default="nas_path.csv", help="Fichier CSV de sortie")
    p.add_argument("--dedupe", action="store_true", help="Dédupliquer (detector,value,path) sur tout le dataset")
    p.add_argument("--detectors-yaml", help="Fichier YAML de détecteurs personnalisés")
    p.add_argument("--field-map", help='Ex: "NAS=nas_norm,EMAIL=emails"')
    p.add_argument("--field-prefix", default="pii.", help='Préfixe par défaut pour les champs (défaut: "pii.")')
    p.add_argument("--apply-updates", action="store_true", help="Appliquer les mises à jour dans ES")
    p.add_argument("--bulk-size", type=int, default=1000, help="Nb d’updates par bulk")
    return p.parse_args()


def main():
    args = parse_args()

    es = ESClient(
        base_url=args.es_url,
        user=args.user,
        password=args.password,
        api_key=args.api_key,
        bearer=args.bearer,
        ca_cert=args.ca_cert,
        verify_tls=not args.no_verify_tls,
    )

    # Détecteurs
    detectors: List[Detector] = [make_nas_detector()]
    if args.detectors_yaml:
        detectors.extend(load_detectors_from_yaml(args.detectors_yaml))

    # Mapping détecteur -> champ
    fmap = parse_field_map(args.field_map or "")

    # Construire la requête initiale
    if args.query_json:
        with open(args.query_json, "r", encoding="utf-8") as f:
            query = json.load(f)
        # s'assurer que _source inclut les champs nécessaires
        src_fields = set(query.get("_source") or [])
        src_fields.update({args.content_field, args.path_field.split(".")[0]})
        query["_source"] = list(src_fields)
    else:
        query = {"query": {"match_all": {}}, "_source": [args.content_field, args.path_field.split(".")[0]]}

    # Écriture CSV
    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    out_f = open(args.out, "w", newline="", encoding="utf-8")
    writer = csv.writer(out_f)
    writer.writerow(["detector", "value", "path", "doc_id"])

    seen_csv: set[Tuple[str, str, str]] = set()

    # Bulk buffer
    bulk_lines: List[str] = []

    def flush_bulk():
        nonlocal bulk_lines
        if not bulk_lines:
            return
        ndjson = "\n".join(bulk_lines) + "\n"
        es.bulk(ndjson)
        bulk_lines = []

    docs_count = 0
    pairs_count = 0
    updates_count = 0

    try:
        for hit in es.search_scroll(index=args.index, query=query, size=args.batch_size):
            docs_count += 1
            _id = hit.get("_id", "")
            src = hit.get("_source") or {}
            text = get_text_from_source(src, args.content_field, args.alt_content_field)
            if not text:
                continue
            path = get_path_virtual(src, args.path_field)
            pairs = extract_from_text(text, detectors)

            # Écrire CSV
            for det_name, value in pairs:
                row = (det_name, value, path, _id)
                if args.dedupe:
                    key = (det_name, value, path)
                    if key in seen_csv:
                        continue
                    seen_csv.add(key)
                writer.writerow(row)
                pairs_count += 1

            # Construire la mise à jour doc si demandé
            if args.apply_updates and pairs:
                # Regrouper par champ cible
                field_to_values: Dict[str, List[str]] = {}
                for det_name, value in pairs:
                    field = target_field(det_name, fmap, args.field_prefix)
                    field_to_values.setdefault(field, [])
                    # éviter les doublons intra-doc pour ce bulk
                    if value not in field_to_values[field]:
                        field_to_values[field].append(value)

                # Préparer l'action BULK update avec script "append if missing"
                if field_to_values:
                    if not _id:
                        continue
                    header = {"update": {"_index": args.index, "_id": _id , "retry_on_conflict": 3}}
                    script_src, params = build_update_script_params(field_to_values)
                    body = {
                        "script": {
                            "lang": "painless",
                            "source": script_src,
                            "params": params,
                        }
                    }
                    bulk_lines.append(json.dumps(header, ensure_ascii=False))
                    bulk_lines.append(json.dumps(body, ensure_ascii=False))
                    updates_count += 1

                    # Flush par paquets
                    if len(bulk_lines) // 2 >= args.bulk_size:
                        flush_bulk()
        # flush final
        if args.apply_updates:
            flush_bulk()
    finally:
        out_f.close()

    print(f"Docs parcourus: {docs_count:,} | Valeurs extraites: {pairs_count:,} | Updates envoyées: {updates_count:,}")
    print(f"CSV écrit dans: {args.out}")


if __name__ == "__main__":
    main()

