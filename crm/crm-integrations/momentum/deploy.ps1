param (
    [Parameter(Mandatory=$true)]
    [string]$e,
    
    [string]$r = "us"
)

function Show-Help {
    Write-Host "
Deploy the momentum CRM integration.
Usage:
  .\deploy.ps1 -e <env> [-r <region>]
Options:
  -e   REQUIRED: Environment to deploy to (e.g. dev, test, prod)
  -r   Optional: Impel region (default is 'us')
"
    exit 1
}

# Validate env
if (-not $e) {
    Show-Help
}

# Region logic
if ($r -eq "us" -or [string]::IsNullOrWhiteSpace($r)) {
    $region = "us-east-1"
} else {
    Write-Host "Invalid region: $r"
    exit 2
}

# Get user and commit info
try {
    $user = (aws iam get-user --output json | ConvertFrom-Json).User.UserName
} catch {
    Write-Host "Failed to retrieve IAM user: $_"
    exit 2
}

$commit_id = git log -1 --format=%H

# Run sam build
sam build --parallel

# Deploy logic
switch ($e) {
    "prod" {
        sam deploy --config-env "prod" `
            --tags "Commit=`"$commit_id`" Environment=`"prod`" UserLastModified=`"$user`"" `
            --region $region `
            --s3-bucket "spincar-deploy-$region" `
            --parameter-overrides "Environment=`"prod`""
    }
    "stage" {
        sam deploy --config-env "stage" `
            --tags "Commit=`"$commit_id`" Environment=`"stage`" UserLastModified=`"$user`"" `
            --region $region `
            --s3-bucket "spincar-deploy-$region" `
            --parameter-overrides "Environment=`"stage`""
    }
    "test" {
        sam deploy --config-env "test" `
            --tags "Commit=`"$commit_id`" Environment=`"test`" UserLastModified=`"$user`"" `
            --region $region `
            --s3-bucket "spincar-deploy-$region" `
            --parameter-overrides "Environment=`"test`""
    }
    default {
        $env = "$user-$(git rev-parse --abbrev-ref HEAD)"
        sam deploy `
            --tags "Commit=`"$commit_id`" Environment=`"$env`" UserLastModified=`"$user`"" `
            --stack-name "momentum-crm-integration-$env" `
            --region $region `
            --s3-bucket "spincar-deploy-$region" `
            --parameter-overrides "Environment=`"$env`""
    }
}
