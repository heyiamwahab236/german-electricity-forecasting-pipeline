param(
    [string]$Profile = 'electricity',
    [Parameter(Mandatory=$true)][string]$ConfigPath,
    [string]$NotebookRoot,
    [string]$Python = 'python'
)
$ErrorActionPreference = 'Stop'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    & $Python scripts/build_cleaned_notebook.py
    if ($LASTEXITCODE -ne 0) { throw 'Cleaning notebook build failed' }
    & $Python scripts/build_workflow.py
    if ($LASTEXITCODE -ne 0) { throw 'Workflow build failed' }
    $identity = databricks current-user me --profile $Profile --output json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw 'Authenticate with Databricks first' }
    if (-not $NotebookRoot) { $NotebookRoot = '/Users/' + $identity.userName + '/Electricity Portfolio' }
    databricks workspace mkdirs $NotebookRoot --profile $Profile
    if ($LASTEXITCODE -ne 0) { throw 'Notebook folder creation failed' }
    $configFolder = 'dbfs:' + $ConfigPath.Substring(0, $ConfigPath.LastIndexOf('/'))
    databricks fs mkdir $configFolder --profile $Profile
    if ($LASTEXITCODE -ne 0) { throw 'Config folder creation failed' }
    $configDestination = 'dbfs:' + $ConfigPath
    databricks fs cp config.json $configDestination --overwrite --profile $Profile
    if ($LASTEXITCODE -ne 0) { throw 'Config upload failed; check the Volume and path' }
    $localConfig = Get-Content config.json -Raw | ConvertFrom-Json
    foreach ($notebook in (Get-ChildItem notebooks -Filter '*.py')) {
        databricks workspace import ($NotebookRoot + '/' + $notebook.BaseName) --file $notebook.FullName --format SOURCE --language PYTHON --overwrite --profile $Profile
        if ($LASTEXITCODE -ne 0) { throw "Notebook import failed: $($notebook.Name)" }
    }
    $fixtureFolder = 'dbfs:' + $localConfig.output_root + '/project/tests/fixtures'
    databricks fs mkdir $fixtureFolder --profile $Profile
    if ($LASTEXITCODE -ne 0) { throw 'Fixture folder creation failed' }
    databricks fs cp tests/fixtures/corrupt_price.csv ($fixtureFolder + '/corrupt_price.csv') --overwrite --profile $Profile
    if ($LASTEXITCODE -ne 0) { throw 'Fixture upload failed' }
    $bundleVariables = 'config_path=' + $ConfigPath + ',notebook_root=' + $NotebookRoot
    databricks bundle validate --var $bundleVariables --profile $Profile
    if ($LASTEXITCODE -ne 0) { throw 'Bundle validation failed' }
    databricks bundle deploy --var $bundleVariables --profile $Profile
    if ($LASTEXITCODE -ne 0) { throw 'Bundle deployment failed' }
    Write-Output "Run with: databricks bundle run smard_pipeline --var `"$bundleVariables`" --profile $Profile"
} finally {
    Pop-Location
}
