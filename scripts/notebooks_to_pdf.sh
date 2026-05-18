#!/usr/bin/env bash
# Convert every .ipynb in notebooks/ to PDF, writing to notebooks/pdf/.
#
# Intended to be run on the DATA7201 edge node after the notebooks have
# been executed (so cached outputs are present in the .ipynb JSON).
#
# Tries jupyter nbconvert --to pdf first (LaTeX-based). If that fails for a
# given notebook (most often due to missing LaTeX packages or HTML-heavy
# output cells that don't render in LaTeX), falls back to --to webpdf
# (headless Chromium / Playwright).
#
# Usage:
#   chmod +x scripts/notebooks_to_pdf.sh
#   ./scripts/notebooks_to_pdf.sh
#
# Output:
#   notebooks/pdf/<notebook_name>.pdf

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
NOTEBOOKS_DIR="$REPO_ROOT/notebooks"
OUTPUT_DIR="$NOTEBOOKS_DIR/pdf"

if ! command -v jupyter >/dev/null 2>&1; then
    echo "ERROR: jupyter not found in PATH. Activate the project environment first." >&2
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

ok=()
failed=()

for nb in "$NOTEBOOKS_DIR"/*.ipynb; do
    [ -e "$nb" ] || continue
    name=$(basename "$nb" .ipynb)
    echo "==> $name"

    if jupyter nbconvert --to pdf --output-dir "$OUTPUT_DIR" "$nb" >/dev/null 2>&1; then
        echo "    ok (pdf)"
        ok+=("$name")
    elif jupyter nbconvert --to webpdf --allow-chromium-download \
                            --output-dir "$OUTPUT_DIR" "$nb" >/dev/null 2>&1; then
        echo "    ok (webpdf fallback)"
        ok+=("$name")
    else
        echo "    FAILED -- run manually for diagnostics:"
        echo "      jupyter nbconvert --to pdf --output-dir $OUTPUT_DIR $nb"
        failed+=("$name")
    fi
done

echo
echo "Converted ${#ok[@]} notebooks to $OUTPUT_DIR/"
if [ "${#failed[@]}" -gt 0 ]; then
    echo "Failed: ${failed[*]}"
    exit 1
fi
