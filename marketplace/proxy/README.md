# OnlineMarketplace Proxy

Compile:
```
clean package -DskipTests=true
```

Run:
```
java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar proxy-1.0-SNAPSHOT-jar-with-dependencies.jar
```

