# Start mysql
echo "STARTING MYSQL SERVER IN BACKGROUND"
mysqld 2>&1 &

# Keep the container active and running
tail -f /dev/null