param(
    [string]$Command = "debug",
    [string]$Select = ""
)

$env:POSTGRES_HOST="localhost"
$env:POSTGRES_PORT="5433"
$env:POSTGRES_DB="retail_dw"
$env:POSTGRES_USER="retail_user"
$env:POSTGRES_PASSWORD="retail_password"

$ProjectDir = "dbt"
$ProfilesDir = "dbt"

if ($Command -eq "freshness") {
    dbt source freshness --project-dir $ProjectDir --profiles-dir $ProfilesDir
}
elseif ($Select -eq "") {
    dbt $Command --project-dir $ProjectDir --profiles-dir $ProfilesDir
}
else {
    dbt $Command --select $Select --project-dir $ProjectDir --profiles-dir $ProfilesDir
}