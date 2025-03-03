cd C:\Users\edwar\Documents\dbt_auto
git add seeds/*
git commit -m "Automated update of seed files"
git fetch
git rebase
git push
dbt seed --select tag:%1