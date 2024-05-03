#!/bin/bash

param1=1

if [ $# -eq 0 ];
then
  echo "No arguments passed. Assuming one customer checkout only"
else
  param1="$1"
fi


echo "Adding stock item 1/1"

curl -X POST -H "Content-Type: application/json" -d '{"seller_id": "1", "product_id": "1", "qty_available" : 10, "qty_reserved" : 0, "order_count" : 0, "ytd": 0, "data" : "", "version": "0"}' localhost:8002/stock

echo "Retrieving stock item 1/1"

curl -X GET localhost:8002/stock/1/1

echo ""

echo "Adding cart item 1/1"

curl -X PATCH -H "Content-Type: application/json" -d '{"SellerId": "1", "ProductId": "1", "ProductName" : "test", "UnitPrice" : "10", "FreightValue" : "0", "Quantity": "3", "Voucher" : "0", "Version": "0"}' localhost:8000/cart/1

echo "Retrieving cart item 1/1"

curl -X GET localhost:8000/cart/1/1/1

echo "Submitting checkout request to proxy"

curl -X POST -H "Content-Type: application/json" -d '{ "CustomerId" : 1, "FirstName" : "test", "LastName" : "test", "Street" : "test", "Complement" : "test", "City" : "test", "State" : "test", "ZipCode" : "test", "PaymentType : "CREDIT_CARD", "CardNumber" : "test", "CardHolderName" : "test", "CardExpiration" : "test", "CardBrand" : "test", "Installments" : "test",  "instanceId" : "1" }' localhost:8090/cart

echo "Checkout script done"
