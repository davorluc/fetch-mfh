#!/usr/bin/env python3
import xml.etree.ElementTree as ET
from collections import Counter
import requests

def fetch_latest(canton, page_size=500):
    url = "https://amtsblattportal.ch/api/v1/publications/xml"
    resp = requests.get(url, params={"cantons": canton, "page": 0, "publicationStates": "PUBLISHED", "pageRequest.size": 2000})
    resp.raise_for_status()
    return resp.text

def extract_rubrics(xml_text):
    root = ET.fromstring(xml_text)
    ctr = Counter()
    for pub in root.findall(".//publication"):
        meta = pub.find("meta")
        if meta is None:
            continue
        rubric = (meta.findtext("rubric") or "").strip()
        sub = (meta.findtext("subRubric") or "").strip()
        ctr[(rubric, sub)] += 1
    return ctr

def main():
    for canton in ["AG", "LU"]:
        xml = fetch_latest(canton)
        counts = extract_rubrics(xml)
        print(f"\n=== {canton} ===")
        for (r, s), c in counts.items():
            print(f"{r or '(empty)'} / {s or '(empty)'} — {c}")

if __name__ == "__main__":
    main()
