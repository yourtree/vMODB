# Online Marketplace Seller Microservice

## Running the project

Run the project with the command as follows:
```
java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar target/seller-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Playing with the APIs

Let's start adding a <b>seller</b> to the <i>Seller</i> microservice
```
curl -X POST -H "Content-Type: application/json" -d '{"id": "1", "name" : "test", "company_name" : "test", "email" : "test", "phone" : "test", "mobile_phone": "test", "cpf" : "test", "cnpj" : "test", "address": "test", "complement" : "test", "city" : "test", "state" : "test", "zip_code": "test"}' localhost:8007/seller
```

Let's send a GET request to verify whether the function have successfully stored the state
```
curl -X GET localhost:8007/seller/1
```

If everything worked, you should see the following output:

```
{"id": "1", "name" : "test", "company_name" : "test", "email" : "test", "phone" : "test", "mobile_phone": "test", "cpf" : "test", "cnpj" : "test", "address": "test", "complement" : "test", "city" : "test", "state" : "test", "zip_code": "test"}
```

