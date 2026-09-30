#!/usr/bin/env bash
# Download the Olist dataset from Kaggle into data/source.
# Needs a Kaggle API token at ~/.kaggle/kaggle.json (Kaggle > Settings > API > Create New Token).
# Dataset licence: CC BY-NC-SA 4.0, (c) Olist. It is not committed to this repository.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data/source
uvx kaggle datasets download -d olistbr/brazilian-ecommerce -p data/source --unzip
ls -1 data/source
