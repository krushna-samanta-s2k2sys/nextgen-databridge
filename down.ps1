# down.ps1 - Tear down the entire NextGenDatabridge environment.
# Run from the project root: .\down.ps1

$DB_PASSWORD    = "ActionDag!1"
$MSSQL_PASSWORD = "ActionDag!1"
$REGION         = if ($env:AWS_DEFAULT_REGION) { $env:AWS_DEFAULT_REGION } else { "us-east-1" }
$SCRIPT_DIR     = $PSScriptRoot
$TF_DIR         = "$SCRIPT_DIR\infra\terraform"

$env:TF_VAR_db_password    = $DB_PASSWORD
$env:TF_VAR_mssql_password = $MSSQL_PASSWORD

function Step($msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Info($msg)  { Write-Host "[ok]  $msg" -ForegroundColor Green }
function Warn($msg)  { Write-Host "[warn] $msg" -ForegroundColor Yellow }

Write-Host ""
Write-Host "  This will permanently destroy ALL NextGenDatabridge AWS resources." -ForegroundColor Red
Write-Host ""
$CONFIRM = Read-Host "  Type YES to continue"
if ($CONFIRM -ne "YES") { Write-Host "Aborted."; exit 0 }

# --- Read Terraform outputs --------------------------------------------------
Step "Reading Terraform state..."
Set-Location $TF_DIR

function TfOut($name) {
    $val = terraform output -raw $name 2>$null
    if ($LASTEXITCODE -ne 0) { return "" }
    return $val
}

$DUCKDB_BUCKET    = TfOut "duckdb_bucket"
$CONFIGS_BUCKET   = TfOut "pipeline_configs_bucket"
$ARTIFACTS_BUCKET = TfOut "artifacts_bucket"
$MWAA_BUCKET      = TfOut "mwaa_bucket"
$ECR_API          = TfOut "api_ecr_url"
$ECR_UI           = TfOut "ui_ecr_url"
$ECR_TRANSFORM    = TfOut "transform_ecr_url"
$CLUSTER_NAME     = TfOut "cluster_name"

# --- Step 1: Delete K8s namespaces to release NLBs ---------------------------
Step "STEP 1/4 - Removing K8s namespaces (releases NLBs)"
if ($CLUSTER_NAME) {
    aws eks update-kubeconfig --region $REGION --name $CLUSTER_NAME 2>$null
    kubectl delete namespace nextgen-databridge nextgen-databridge-jobs --ignore-not-found=true 2>$null
    Info "Waiting 60s for NLBs to deprovision..."
    Start-Sleep -Seconds 60
} else {
    Warn "Could not read cluster name - skipping kubectl cleanup"
}

# --- Step 2: Empty S3 buckets ------------------------------------------------
Step "STEP 2/4 - Emptying S3 buckets"
function Empty-S3Bucket($bucket) {
    if (-not $bucket) { return }
    Write-Host "  Emptying s3://$bucket ..."
    aws s3 rm "s3://$bucket" --recursive 2>$null

    foreach ($query in @("Versions[].{Key:Key,VersionId:VersionId}", "DeleteMarkers[].{Key:Key,VersionId:VersionId}")) {
        $result = aws s3api list-object-versions --bucket $bucket `
            --query "{Objects: $query}" --output json 2>$null | ConvertFrom-Json
        if ($result.Objects) {
            $tmp = [System.IO.Path]::ChangeExtension([System.IO.Path]::GetTempFileName(), ".json")
            [System.IO.File]::WriteAllText($tmp, (@{Objects = $result.Objects} | ConvertTo-Json -Compress), [System.Text.Encoding]::ASCII)
            aws s3api delete-objects --bucket $bucket --delete "file://$tmp" | Out-Null
            Remove-Item $tmp -Force
        }
    }
}

foreach ($bucket in @($DUCKDB_BUCKET, $CONFIGS_BUCKET, $ARTIFACTS_BUCKET, $MWAA_BUCKET)) {
    Empty-S3Bucket $bucket
}

# --- Step 3: Purge ECR images ------------------------------------------------
Step "STEP 3/4 - Purging ECR images"
function Purge-ECR($repoUrl) {
    if (-not $repoUrl) { return }
    $repoName = ($repoUrl -split "/", 2)[1]
    Write-Host "  Purging ECR: $repoName"
    $images = aws ecr list-images --repository-name $repoName `
        --query "imageIds[*]" --output json 2>$null | ConvertFrom-Json
    if ($images -and $images.Count -gt 0) {
        $tmp = [System.IO.Path]::ChangeExtension([System.IO.Path]::GetTempFileName(), ".json")
        [System.IO.File]::WriteAllText($tmp, ($images | ConvertTo-Json), [System.Text.Encoding]::ASCII)
        aws ecr batch-delete-image --repository-name $repoName --image-ids "file://$tmp" | Out-Null
        Remove-Item $tmp -Force
    }
}

foreach ($repo in @($ECR_API, $ECR_UI, $ECR_TRANSFORM)) {
    Purge-ECR $repo
}

# --- Step 4: Terraform destroy -----------------------------------------------
Step "STEP 4/4 - Terraform destroy"
terraform destroy -auto-approve -var="db_password=$DB_PASSWORD" -var="mssql_password=$MSSQL_PASSWORD"

Write-Host ""
Write-Host "=====================================================" -ForegroundColor Green
Write-Host "  All NextGenDatabridge resources destroyed." -ForegroundColor Green
Write-Host "=====================================================" -ForegroundColor Green
Write-Host ""
