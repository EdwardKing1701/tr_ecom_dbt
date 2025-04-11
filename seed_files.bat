cd C:\Users\edwar\Documents\dbt_auto
git fetch
git rebase
git add seeds/*
git commit -m "Automated update of seed files"
git push
dbt seed --select tag:%1 --target prod