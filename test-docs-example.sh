#!/bin/bash

# Test script to verify the documentation example works
# Run this after starting the aggregator server with: go run cmd/aggregator/main.go

echo "Testing the documentation example payload..."
echo "Make sure the aggregator is running on localhost:3000"
echo ""

curl -X POST http://localhost:3000/ \
  -H "Content-Type: application/json" \
  -d '{
  "jsonrpc": "2.0",
  "method": "certification_request",
  "params": {
    "stateId": "00004d1b938134c52340952357dd89c4c270b9b0b523bd69c03c1774fed907f1ebb5",
    "certificationData": {
      "publicKey": "033cf8de37cec427b5e3d782e5fc516dcc43f8e9c7bc03530833879f6ee7987d4e",
      "signature": "2061590eeaf9c3fc3e894454b43410d0410f37ab17e5104a08db3d018d072880f9715dc3b60989cf9cc4589850edecac344702594aa264b2789792bb855a30f39c",
      "sourceStateHash": "0000026581b5546639dc5110634df8cbbdf4150f3583fc54a0db98ef413574396dd0",
      "transactionHash": "0000d89cdfd6716717577adeb4149e22646cca3b4daf76632d35e97bd19642f8478a"
    },
    "receipt": true
  },
  "id": 1
}' | jq .

echo ""
echo "If you see a SUCCESS status, the signature validation is working!"
echo "If you see any validation error, there's an issue with the implementation."