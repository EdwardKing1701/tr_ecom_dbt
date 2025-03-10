cd C:\Users\edwar\Documents\dbt_auto
git fetch
git rebase
dbt test --target prod
dbt run --select tag:daily --target prod