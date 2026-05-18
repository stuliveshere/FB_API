# Convert every notebooks/*.ipynb to PDF locally on Windows.
# Path: nbconvert -> HTML -> headless Chrome -> PDF.
#
# Prerequisites (install with winget if missing):
#   - Python + jupyter
#       winget install astral-sh.uv
#       uv tool install --with nbconvert jupyter
#     (alternatively: winget install Python.Python.3.12; pip install jupyter nbconvert)
#   - Google Chrome
#       winget install Google.Chrome
#
# Usage:
#   ./scripts/notebooks_to_pdf.ps1

# Do not use Stop: nbconvert writes informational messages to stderr, which
# PowerShell 5.1 promotes to NativeCommandError records. We rely on
# $LASTEXITCODE and Test-Path on the output files for success/failure
# detection instead.
$ErrorActionPreference = 'Continue'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$RepoRoot  = Split-Path -Parent $ScriptDir
$NbDir     = Join-Path $RepoRoot 'notebooks'
$OutDir    = Join-Path $NbDir 'pdf'

# Locate Chrome
$ChromeCandidates = @(
    "$env:ProgramFiles\Google\Chrome\Application\chrome.exe",
    "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe",
    "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
)
$Chrome = $ChromeCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $Chrome) {
    Write-Error "Chrome not found. Install via: winget install Google.Chrome"
    exit 1
}

# Locate a Python with nbconvert. We call `python -m nbconvert` rather than
# the `jupyter nbconvert` subcommand because some conda envs install
# nbconvert without its CLI shim.
$PythonCandidates = @(
    "$env:USERPROFILE\.conda\envs\jupyter\python.exe",
    "$env:USERPROFILE\anaconda3\python.exe",
    "$env:USERPROFILE\miniconda3\python.exe",
    "$env:LOCALAPPDATA\miniconda3\python.exe",
    "C:\ProgramData\miniconda3\python.exe",
    (Get-Command python -ErrorAction SilentlyContinue).Source
)
$Python = $null
foreach ($candidate in $PythonCandidates) {
    if (-not $candidate -or -not (Test-Path $candidate)) { continue }
    # Verify it has nbconvert.
    & $candidate -c "import nbconvert" 2>$null
    if ($LASTEXITCODE -eq 0) { $Python = $candidate; break }
}
if (-not $Python) {
    Write-Error @"
No Python with nbconvert found. Tried common conda env locations and PATH.
Install nbconvert into your jupyter env, e.g.:
  conda install -n jupyter nbconvert
or globally:
  winget install astral-sh.uv ; uv tool install --with nbconvert jupyter
"@
    exit 1
}
Write-Host "Using python at: $Python"

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$ok     = @()
$failed = @()

foreach ($nb in Get-ChildItem -Path $NbDir -Filter '*.ipynb') {
    $name     = $nb.BaseName
    $htmlPath = Join-Path $OutDir "$name.html"
    $pdfPath  = Join-Path $OutDir "$name.pdf"

    Write-Host "==> $name"

    # Step 1: nbconvert -> HTML (suppress stderr via 2>&1 piped to Out-Null)
    & $Python -m nbconvert --to html --output-dir $OutDir $nb.FullName 2>&1 | Out-Null
    if (-not (Test-Path $htmlPath)) {
        Write-Host "    nbconvert failed"
        $failed += $name
        continue
    }

    # Step 2: headless Chrome -> PDF
    $absHtml = (Resolve-Path $htmlPath).Path
    $fileUri = "file:///" + ($absHtml -replace '\\','/' -replace ' ','%20')

    & $Chrome `
        --headless=new `
        --disable-gpu `
        --no-pdf-header-footer `
        --print-to-pdf="$pdfPath" `
        "$fileUri" 2>&1 | Out-Null

    if (-not (Test-Path $pdfPath)) {
        Write-Host "    Chrome PDF conversion failed"
        $failed += $name
        Remove-Item $htmlPath -ErrorAction SilentlyContinue
        continue
    }

    Remove-Item $htmlPath -ErrorAction SilentlyContinue
    Write-Host "    ok -> $pdfPath"
    $ok += $name
}

Write-Host ""
Write-Host "Converted $($ok.Count) notebooks to $OutDir/"
if ($failed.Count -gt 0) {
    Write-Host "Failed: $($failed -join ', ')"
    exit 1
}
