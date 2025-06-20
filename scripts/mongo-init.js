// MongoDB initialization script
// This script runs when the MongoDB container starts for the first time

print('Initializing Unicity Aggregator database...');

// Switch to the aggregator database
db = db.getSiblingDB('aggregator');

// Create collections with proper indexes
print('Creating collections and indexes...');

// Commitments collection
db.createCollection('commitments');
db.commitments.createIndex({ requestId: 1 }, { unique: true });
db.commitments.createIndex({ createdAt: 1 });
db.commitments.createIndex({ processedAt: 1 });
db.commitments.createIndex({ processedAt: 1, createdAt: 1 });

// Aggregator records collection
db.createCollection('aggregator_records');
db.aggregator_records.createIndex({ requestId: 1 }, { unique: true });
db.aggregator_records.createIndex({ blockNumber: 1 });
db.aggregator_records.createIndex({ leafIndex: 1 });
db.aggregator_records.createIndex({ finalizedAt: -1 });
db.aggregator_records.createIndex({ blockNumber: 1, leafIndex: 1 });

// Blocks collection
db.createCollection('blocks');
db.blocks.createIndex({ index: 1 }, { unique: true });
db.blocks.createIndex({ createdAt: -1 });
db.blocks.createIndex({ chainId: 1 });

// SMT nodes collection
db.createCollection('smt_nodes');
db.smt_nodes.createIndex({ key: 1 }, { unique: true });
db.smt_nodes.createIndex({ hash: 1 });
db.smt_nodes.createIndex({ createdAt: -1 });

// Block records collection
db.createCollection('block_records');
db.block_records.createIndex({ blockNumber: 1 }, { unique: true });
db.block_records.createIndex({ requestIds: 1 });
db.block_records.createIndex({ createdAt: -1 });

// Leadership collection
db.createCollection('leadership');
db.leadership.createIndex({ serverId: 1 });
db.leadership.createIndex({ expiresAt: 1 });
db.leadership.createIndex({ updatedAt: -1 });

print('Database initialization completed successfully!');
print('Collections created: commitments, aggregator_records, blocks, smt_nodes, block_records, leadership');
print('All indexes have been created for optimal query performance.');

// Insert a test document to verify everything is working
db.test.insertOne({ 
    message: "Unicity Aggregator database initialized successfully",
    timestamp: new Date(),
    version: "1.0.0"
});

print('Test document inserted. Database is ready for use.');