#!/bin/bash

# Access individual command-line arguments
echo "Script name: $0"
echo "Number of arguments: $#"
echo "The arguments passed: $*"

help_="--help"
param1="$1"

if [ "$param1" = "$help_" ]; then
    echo "It is expected that the script runs in the project's root folder."
    echo "You can specify the apps using the following pattern:"
    echo "<app-id1> ... <app-idn>"
    exit 1
fi

var1=1
current_dir=$(pwd)
echo "Current dir is" $current_dir

if [ $# -eq 0 ];
then
  echo "No arguments passed"
  exit 1
fi

if `echo "$*" | grep -q product`; then
    p=`ps | grep -c product`
    if [ $p = $var1 ]
    then
        echo "product already running"
    else
        echo "initializing product..."
        osascript -e 'tell app "Terminal"
            do script "java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar '$current_dir'/product/target/product-1.0-SNAPSHOT-jar-with-dependencies.jar"
        end tell'
    fi
fi

if `echo "$*" | grep -q stock`; then
    s=`ps | grep -c stock`
    if [ $s = $var1 ]
    then
        echo "stock already running"
    else
        echo "initializing stock..."
        osascript -e 'tell app "Terminal"
            do script "java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar '$current_dir'/stock/target/stock-1.0-SNAPSHOT-jar-with-dependencies.jar"
        end tell'
    fi
fi

echo "Wait for microservices before setting up the proxy (coordinator)"
sleep 2

if `echo "$*" | grep -q proxy`; then
    p=`ps | grep -c proxy`
    if [ $p = $var1 ]
    then
        echo "proxy already running"
    else
        echo "initializing proxy..."
        osascript -e 'tell app "Terminal"
            do script "java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar '$current_dir'/proxy/target/proxy-1.0-SNAPSHOT-jar-with-dependencies.jar"
        end tell'
    fi
fi