package gateway

// GenerateDocsHTML generates interactive API documentation
func GenerateDocsHTML() string {
	return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Unicity Aggregator API Documentation</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
            line-height: 1.6;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        h1 {
            color: #2c3e50;
            text-align: center;
            margin-bottom: 30px;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }
        .method-section {
            margin: 30px 0;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            overflow: hidden;
        }
        .method-header {
            background: #3498db;
            color: white;
            padding: 15px 20px;
            font-size: 18px;
            font-weight: bold;
        }
        .method-content {
            padding: 20px;
        }
        .params-container {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin: 20px 0;
        }
        @media (max-width: 768px) {
            .params-container {
                grid-template-columns: 1fr;
            }
        }
        .params-section {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 5px;
            border-left: 4px solid #28a745;
        }
        .response-section {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 5px;
            border-left: 4px solid #17a2b8;
        }
        textarea {
            width: 100%;
            height: 200px;
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
            font-size: 14px;
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 10px;
            resize: vertical;
            box-sizing: border-box;
        }
        .response-box {
            width: 100%;
            height: 200px;
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
            font-size: 14px;
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 10px;
            background: #2d3748;
            color: #e2e8f0;
            overflow-y: auto;
            white-space: pre-wrap;
            box-sizing: border-box;
        }
        .button-group {
            margin: 15px 0;
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }
        button {
            background: #3498db;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
            transition: background-color 0.3s;
        }
        button:hover {
            background: #2980b9;
        }
        button:disabled {
            background: #bdc3c7;
            cursor: not-allowed;
        }
        .status {
            display: inline-block;
            padding: 5px 10px;
            border-radius: 3px;
            font-size: 12px;
            font-weight: bold;
            margin-left: 10px;
        }
        .status-loading {
            background: #d1ecf1;
            color: #0c5460;
        }
        .status-success {
            background: #d4edda;
            color: #155724;
        }
        .status-error {
            background: #f8d7da;
            color: #721c24;
        }
        .info-box {
            background: #fff3cd;
            border: 1px solid #ffeaa7;
            border-radius: 4px;
            padding: 15px;
            margin: 20px 0;
        }
        .description {
            color: #666;
            margin-bottom: 15px;
        }
        h3 {
            margin-top: 0;
            color: #2c3e50;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 Unicity Aggregator API - Interactive Documentation</h1>
        
        <div class="info-box">
            <p><strong>Base URL:</strong> <code>http://localhost:3000/</code></p>
            <p><strong>Protocol:</strong> JSON-RPC 2.0 via HTTP POST</p>
            <p><strong>Content-Type:</strong> <code>application/json</code></p>
            <p><strong>💡 How to use:</strong> Edit the JSON parameters below and click "Send Request" to test the API live!</p>
        </div>

        <!-- certification_request -->
        <div class="method-section">
            <div class="method-header">certification_request</div>
            <div class="method-content">
                <div class="description">Submit a state transition certification request to the aggregator. The example below uses a real secp256k1 signature that will pass validation. Note: All hash fields (stateId, transactionHash, sourceStateHash) start with "0000" (SHA256 algorithm prefix).</div>
                
                <div class="params-container">
                    <div class="params-section">
                        <h3>Request Parameters</h3>
                        <textarea id="certification_request_params">"8458203e331d1f9c0265105db6173d932a419049fdbdfdd32f9e488fe48e8598a23eed8483014101582102d1a5a7bebd4118d9a46feb00686f2236bc745c8378eb54078c43003799e042395820b54d5ddf1772aca589c8fbfad03eca8d6ffaca0e1f1c1cd89ecd0afe3a73b6f05820f5c5161937726e0f24b074115e4752899a2f4d3f154764209ae11d05ec63c72e5841a000a2c83ddf683f8ca388cbd0b1bfd52bcd919641a0774d09459ca97aeeaf4e3682d863ac77afb71dc8efe0b8c1b983ee9b5231adad9adc5c27a0b97be6147c00f500"</textarea>
                        <div class="button-group">
                            <button onclick="sendRequest('certification_request')">🚀 Send Request</button>
                            <button onclick="clearResponse('certification_request')">🗑️ Clear</button>
                            <button onclick="copyAsCurl('certification_request')">📋 Copy cURL</button>
                        </div>
                    </div>
                    
                    <div class="response-section">
                        <h3>Response <span id="certification_request_status" class="status"></span></h3>
                        <div id="certification_request_response" class="response-box">Click "Send Request" to see the response here...</div>
                    </div>
                </div>
            </div>
        </div>

        <!-- get_inclusion_proof -->
        <div class="method-section">
            <div class="method-header">get_inclusion_proof</div>
            <div class="method-content">
                <div class="description">Retrieve the inclusion proof for a submitted certification request.</div>
                
                <div class="params-container">
                    <div class="params-section">
                        <h3>Request Parameters</h3>
                        <textarea id="get_inclusion_proof_params">{
  "stateId": "0000c7aa6962316c0eeb1469dc3d7793e39e140c005e6eea0e188dcc73035d765937"
}</textarea>
                        <div class="button-group">
                            <button onclick="sendRequest('get_inclusion_proof')">🚀 Send Request</button>
                            <button onclick="clearResponse('get_inclusion_proof')">🗑️ Clear</button>
                            <button onclick="copyAsCurl('get_inclusion_proof')">📋 Copy cURL</button>
                        </div>
                    </div>
                    
                    <div class="response-section">
                        <h3>Response <span id="get_inclusion_proof_status" class="status"></span></h3>
                        <div id="get_inclusion_proof_response" class="response-box">Click "Send Request" to see the response here...</div>
                    </div>
                </div>
            </div>
        </div>

        <!-- get_block_height -->
        <div class="method-section">
            <div class="method-header">get_block_height</div>
            <div class="method-content">
                <div class="description">Get the current block height (number of the latest block).</div>
                
                <div class="params-container">
                    <div class="params-section">
                        <h3>Request Parameters</h3>
                        <textarea id="get_block_height_params">{}</textarea>
                        <div class="button-group">
                            <button onclick="sendRequest('get_block_height')">🚀 Send Request</button>
                            <button onclick="clearResponse('get_block_height')">🗑️ Clear</button>
                            <button onclick="copyAsCurl('get_block_height')">📋 Copy cURL</button>
                        </div>
                    </div>
                    
                    <div class="response-section">
                        <h3>Response <span id="get_block_height_status" class="status"></span></h3>
                        <div id="get_block_height_response" class="response-box">Click "Send Request" to see the response here...</div>
                    </div>
                </div>
            </div>
        </div>

        <!-- get_block -->
        <div class="method-section">
            <div class="method-header">get_block</div>
            <div class="method-content">
                <div class="description">Get detailed information about a specific block.</div>
                
                <div class="params-container">
                    <div class="params-section">
                        <h3>Request Parameters</h3>
                        <textarea id="get_block_params">{
  "blockNumber": "latest"
}</textarea>
                        <div class="button-group">
                            <button onclick="sendRequest('get_block')">🚀 Send Request</button>
                            <button onclick="clearResponse('get_block')">🗑️ Clear</button>
                            <button onclick="copyAsCurl('get_block')">📋 Copy cURL</button>
                        </div>
                    </div>
                    
                    <div class="response-section">
                        <h3>Response <span id="get_block_status" class="status"></span></h3>
                        <div id="get_block_response" class="response-box">Click "Send Request" to see the response here...</div>
                    </div>
                </div>
            </div>
        </div>

        <!-- get_block_records -->
        <div class="method-section">
            <div class="method-header">get_block_records</div>
            <div class="method-content">
                <div class="description">Get all certification requests included in a specific block.</div>
                
                <div class="params-container">
                    <div class="params-section">
                        <h3>Request Parameters</h3>
                        <textarea id="get_block_records_params">{
  "blockNumber": 123
}</textarea>
                        <div class="button-group">
                            <button onclick="sendRequest('get_block_records')">🚀 Send Request</button>
                            <button onclick="clearResponse('get_block_records')">🗑️ Clear</button>
                            <button onclick="copyAsCurl('get_block_records')">📋 Copy cURL</button>
                        </div>
                    </div>
                    
                    <div class="response-section">
                        <h3>Response <span id="get_block_records_status" class="status"></span></h3>
                        <div id="get_block_records_response" class="response-box">Click "Send Request" to see the response here...</div>
                    </div>
                </div>
            </div>
        </div>

        <!-- get_no_deletion_proof -->
        <div class="method-section">
            <div class="method-header">get_no_deletion_proof</div>
            <div class="method-content">
                <div class="description">Retrieve the global no-deletion proof for the aggregator data structure.</div>
                
                <div class="params-container">
                    <div class="params-section">
                        <h3>Request Parameters</h3>
                        <textarea id="get_no_deletion_proof_params">{}</textarea>
                        <div class="button-group">
                            <button onclick="sendRequest('get_no_deletion_proof')">🚀 Send Request</button>
                            <button onclick="clearResponse('get_no_deletion_proof')">🗑️ Clear</button>
                            <button onclick="copyAsCurl('get_no_deletion_proof')">📋 Copy cURL</button>
                        </div>
                    </div>
                    
                    <div class="response-section">
                        <h3>Response <span id="get_no_deletion_proof_status" class="status"></span></h3>
                        <div id="get_no_deletion_proof_response" class="response-box">Click "Send Request" to see the response here...</div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        async function sendRequest(method) {
            const paramsField = document.getElementById(method + '_params');
            const responseField = document.getElementById(method + '_response');
            const statusField = document.getElementById(method + '_status');
            
            // Show loading status
            statusField.className = 'status status-loading';
            statusField.textContent = 'Sending...';
            responseField.textContent = 'Sending request...';
            
            let params = {};
            try {
                const paramsText = paramsField.value.trim();
                if (paramsText && paramsText !== '{}') {
                    params = JSON.parse(paramsText);
                }
            } catch (e) {
                statusField.className = 'status status-error';
                statusField.textContent = 'Parse Error';
                responseField.textContent = 'Error parsing JSON parameters: ' + e.message;
                return;
            }
            
            const requestBody = {
                jsonrpc: '2.0',
                method: method,
                params: params,
                id: Date.now()
            };
            
            try {
                const startTime = Date.now();
                const response = await fetch('/', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(requestBody)
                });
                
                const duration = Date.now() - startTime;
                const text = await response.text();
                
                let formatted;
                try {
                    const json = JSON.parse(text);
                    formatted = JSON.stringify(json, null, 2);
                } catch (e) {
                    formatted = text;
                }
                
                responseField.textContent = formatted;
                
                if (response.ok) {
                    statusField.className = 'status status-success';
                    statusField.textContent = 'Success (' + duration + 'ms)';
                } else {
                    statusField.className = 'status status-error';
                    statusField.textContent = 'Error ' + response.status + ' (' + duration + 'ms)';
                }
                
            } catch (error) {
                statusField.className = 'status status-error';
                statusField.textContent = 'Network Error';
                responseField.textContent = 'Network error: ' + error.message;
            }
        }
        
        function clearResponse(method) {
            const responseField = document.getElementById(method + '_response');
            const statusField = document.getElementById(method + '_status');
            
            responseField.textContent = 'Click "Send Request" to see the response here...';
            statusField.className = 'status';
            statusField.textContent = '';
        }
        
        function copyAsCurl(method) {
            const paramsField = document.getElementById(method + '_params');
            
            let params = {};
            try {
                const paramsText = paramsField.value.trim();
                if (paramsText && paramsText !== '{}') {
                    params = JSON.parse(paramsText);
                }
            } catch (e) {
                alert('Error parsing JSON parameters: ' + e.message);
                return;
            }
            
            const requestBody = {
                jsonrpc: '2.0',
                method: method,
                params: params,
                id: 1
            };
            
            const curlCommand = 'curl -X POST ' + window.location.origin + '/ \\\n' +
                '  -H "Content-Type: application/json" \\\n' +
                '  -d \'' + JSON.stringify(requestBody, null, 2) + '\'';
            
            navigator.clipboard.writeText(curlCommand).then(function() {
                alert('cURL command copied to clipboard!');
            }).catch(function(err) {
                // Fallback for older browsers
                const textArea = document.createElement('textarea');
                textArea.value = curlCommand;
                document.body.appendChild(textArea);
                textArea.select();
                try {
                    document.execCommand('copy');
                    alert('cURL command copied to clipboard!');
                } catch (e) {
                    alert('Failed to copy cURL command');
                }
                document.body.removeChild(textArea);
            });
        }
        
        // Add keyboard shortcut Ctrl+Enter to send requests
        document.addEventListener('keydown', function(e) {
            if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                const activeElement = document.activeElement;
                if (activeElement && activeElement.tagName === 'TEXTAREA') {
                    const methodName = activeElement.id.replace('_params', '');
                    if (methodName) {
                        sendRequest(methodName);
                        e.preventDefault();
                    }
                }
            }
        });
    </script>
</body>
</html>`
}
