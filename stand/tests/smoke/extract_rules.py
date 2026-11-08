#!/usr/bin/env python3
"""Достаёт группы правил из PrometheusRule в формат, который понимает Prometheus и promtool."""
import sys

import yaml

with open(sys.argv[1]) as src:
    rule = yaml.safe_load(src)
with open(sys.argv[2], "w") as dst:
    yaml.safe_dump(rule["spec"], dst, allow_unicode=True, sort_keys=False)
