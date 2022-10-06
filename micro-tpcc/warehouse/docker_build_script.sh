docker build -f Dockerfile.local -t warehouse-docker .
docker run -p 8081:8081 --cpus 2 -d -h localhost --name warehouse --rm warehouse-docker
docker run -p 8081:8081 --name warehouse --rm warehouse-docker

# to get logs
sudo docker logs -f container-id

# to get log path
docker inspect warehouse | grep "LogPath"

# modify java version
# https://stackoverflow.com/questions/21964709/how-to-set-or-change-the-default-java-jdk-version-on-macos
export JAVA_HOME=`/usr/libexec/java_home -v 18`