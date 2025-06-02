function Set-Up-Env {
    param (
        [string]$Component
    )
    if (-not (Test-Path $Component -PathType Container)) {
        Write-Error "Directory '$Component' does not exist."
        return
    }
    cd $Component
    uv venv
    uv sync
}

function Build-Flink-Image {
    $FLINK_EXTRACTOR_PATH="flink-extractor"
    Set-Up-Env -Component $FLINK_EXTRACTOR_PATH
    cd $FLINK_EXTRACTOR_PATH
    docker build -t FLINK_EXTRACTOR_PATH .
}

# Check for preqs
if (-not (Get-Command "uv" -ErrorAction Stop)) {
    Write-Error "'uv' is not installed or not in PATH."
    return
}
if (-not (Get-Command "python" -ErrorAction Stop)) {
    Write-Error "'python' is not installed or not in PATH. uv might fail."
}

# Setup UV envs & docker
Write-Host "Setting up Flink Extractor..."
Build-Flink-Image
Write-Host "Setting up mock db client..."
Set-Up-Env "mock-db-client"
Write-Host "Setting up Kafka direct consumer..."
Set-Up-Env "consumer"
Write-Host "Setting up dbt modeling..."
Set-Up-Env "modeling"

