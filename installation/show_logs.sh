psql -p 5431 -d logs -c "select * from logs order by time desc" -P format=wrapped
