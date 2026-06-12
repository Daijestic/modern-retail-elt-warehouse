param(
    [string]$Command = "build",
    [string]$Select = ""
)

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
