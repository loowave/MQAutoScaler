#!/usr/bin/env python3
"""Сверяет документацию с манифестами: чтобы описание и правила не разъезжались."""
import json
import re
import sys
from pathlib import Path

STAND = Path(__file__).resolve().parent.parent
rules = set(re.findall(r"record: (mq:\S+)", (STAND / "manifests/monitoring/rules.yaml").read_text()))
documented = set(re.findall(r"`(mq:[a-z0-9_:]+)`", (STAND / "docs/metrics.md").read_text()))

dashboard = json.loads((STAND / "manifests/monitoring/dashboards/mqautoscaler.json").read_text())
used = {match for panel in dashboard["panels"] for target in panel.get("targets", [])
        for match in re.findall(r"mq:[a-z0-9_:]+", target["expr"])}

problems = {
    "правила без описания в docs/metrics.md": rules - documented,
    "в docs/metrics.md описаны несуществующие ряды": documented - rules,
    "дашборд использует несуществующие ряды": used - rules,
}

links = []
for doc in [STAND / "README.md", *sorted((STAND / "docs").glob("*.md"))]:
    for target in re.findall(r"\]\((?!https?://|#)([^)#]+)", doc.read_text()):
        if not (doc.parent / target).exists():
            links.append(f"{doc.relative_to(STAND)} → {target}")
problems["битые ссылки"] = set(links)

failed = False
for title, items in problems.items():
    if items:
        failed = True
        print(f"{title}:")
        for item in sorted(items):
            print(f"  {item}")
if failed:
    sys.exit(1)
print(f"ok: {len(rules)} правил описаны, ссылки и дашборд в порядке")
