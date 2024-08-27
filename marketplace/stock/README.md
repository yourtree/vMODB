# Online Marketplace Stock Microservice

## Running the project

And then run the project with the command as follows:
```
java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar target/stock-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Playing with the APIs

Let's start adding an <b>item</b> to the <i>Stock</i> microservice
```
curl -X POST -H "Content-Type: application/json" -d '{"seller_id": "1", "product_id": "1", "name" : "productTest", "sku" : "skuTest", "category" : "categoryTest", "status" : "AVAILABLE", "description": "descriptionTest", "price" : 10, "freight_value" : 0, "version": "1"}' localhost:8002/stock
```

Let's send a GET request to verify whether the function have successfully stored the state
```
curl -X GET localhost:8002/stock/1/1
```

If everything worked, you should see the following output:

```
{"seller_id":"1", "product_id":"1", "name":"productTest", "sku":"skuTest", "category":"categoryTest", "description":"descriptionTest", "price":"10.0", "freight_value":"0.0", "status":"AVAILABLE", "version":"1"}
```

