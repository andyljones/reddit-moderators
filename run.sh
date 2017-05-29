#!/bin/bash
cd "$(dirname "$0")"

source activate reddit && python -c "import scratch; scratch.scrape()"