echo "hello"
tail -f /var/log/nginx/access.log | awk '{print $1}' 
