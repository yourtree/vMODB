docker build -f Dockerfile.local -t warehouse-docker .
docker run -p 8081:8081 --cpus 2 -d -h localhost --name warehouse --rm warehouse-docker

# to get logs
sudo docker logs -f container-id

# to get log path
docker inspect warehouse | grep "LogPath"