$ErrorActionPreference = 'Stop'

$reportRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$buildRoot = Join-Path $reportRoot 'build'
$buildDirs = @(
    $buildRoot,
    (Join-Path $buildRoot 'chapters/main')
)

foreach ($directory in $buildDirs) {
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
}

$oldTexInputs = $env:TEXINPUTS
$env:TEXINPUTS = if ($oldTexInputs) { ".;$reportRoot//;$oldTexInputs" } else { ".;$reportRoot//;" }

Push-Location $buildRoot
try {
    $pdfLatexArgs = @(
        '-interaction=nonstopmode',
        '-halt-on-error',
        '-file-line-error',
        "-output-directory=$buildRoot",
        "-aux-directory=$buildRoot",
        '..\main.tex'
    )

    & pdflatex @pdfLatexArgs
    if ($LASTEXITCODE -ne 0) { throw "pdflatex failed (exit code $LASTEXITCODE)." }

    & pdflatex @pdfLatexArgs
    if ($LASTEXITCODE -ne 0) { throw "pdflatex pass 2 failed (exit code $LASTEXITCODE)." }

    Copy-Item -LiteralPath (Join-Path $buildRoot 'main.pdf') `
        -Destination (Join-Path $reportRoot 'main.pdf') -Force
    Write-Host "PDF: $reportRoot\main.pdf"
    Write-Host "Build intermediates: $buildRoot"
}
finally {
    Pop-Location
    $env:TEXINPUTS = $oldTexInputs
}