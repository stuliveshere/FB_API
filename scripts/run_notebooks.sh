#!/usr/bin/env bash
# Execute notebooks in place on the DATA7201 edge node.
#
# Usage:
#   ./scripts/run_notebooks.sh                 # run the default set
#   ./scripts/run_notebooks.sh 05 07           # run only 05 and 07
#
# Each argument is matched as a prefix against notebooks/<prefix>*.ipynb.
# Notebooks are executed in the order given and saved back in place
# (outputs embedded in the .ipynb JSON).
#
# Timeouts: install scripts/jupyter_nbconvert_config.py into ~/.jupyter/
# for the long cluster startup/exec timeouts. The flags below are kept as
# a fallback in case that config file is not installed.
#
# The default set is the notebooks changed by the 'mixed' category
# exclusion, in dependency order (04 builds v3 inputs, 05 the temporal
# bands, 07 the combined PAC tables).

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
NB_DIR="$REPO_ROOT/notebooks"

DEFAULT_NOTEBOOKS=(04_topic_join 05_election_spenders 07_top_ads_per_advertiser)

if ! command -v jupyter >/dev/null 2>&1; then
    echo "ERROR: jupyter not found in PATH. Activate the project environment first." >&2
    exit 1
fi

if [ "$#" -gt 0 ]; then
    REQUESTED=("$@")
else
    REQUESTED=("${DEFAULT_NOTEBOOKS[@]}")
fi

ok=()
failed=()

for name in "${REQUESTED[@]}"; do
    matches=("$NB_DIR/$name"*.ipynb)
    if [ ! -e "${matches[0]}" ]; then
        echo "ERROR: no notebook matches '$name' in $NB_DIR" >&2
        failed+=("$name")
        continue
    fi
    nb="${matches[0]}"
    echo "==> $(basename "$nb")"
    if jupyter nbconvert --to notebook --execute \
            --ExecutePreprocessor.timeout=3600 \
            --ExecutePreprocessor.startup_timeout=3600 \
            --output-dir "$NB_DIR" "$nb"; then
        echo "    ok"
        ok+=("$(basename "$nb")")
    else
        echo "    FAILED"
        failed+=("$(basename "$nb")")
    fi
done

echo
echo "Ran ${#ok[@]} notebook(s) ok."
if [ "${#failed[@]}" -gt 0 ]; then
    echo "Failed: ${failed[*]}"
    exit 1
fi
