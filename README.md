# ZigScan Indexer

A comprehensive ZigChain blockchain indexer that extracts and stores detailed blockchain data including blocks, transactions, events, and attributes with robust error handling and parallel processing.

## 📁 Project Structure

```
zigscan-indexer/
├── src/                          # Main application source code
│   ├── core/                     # Core application logic
│   │   ├── orchestrator.js       # Main coordinator (manages workers & queues)
│   │   ├── worker.js             # Comprehensive blockchain data processor
│   │   └── indexer.js            # Legacy single-threaded indexer
│   ├── database/                 # Database-related modules
│   │   ├── db.js                 # Connection pooling & query utilities
│   │   ├── models.js             # Comprehensive database schema & table creation
│   │   └── partition.js          # PostgreSQL table partitioning utilities
│   ├── api/                      # API endpoints (if applicable)
│   └── tools/                    # Development/debug tools
│       └── check_msgs.js         # Transaction message type analyzer
├── scripts/                      # Operational scripts
│   └── retry_failed.js           # Retry failed transactions with backoff
├── .env.example                  # Environment template
├── .gitignore                    # Git ignore rules
├── Dockerfile                    # Container configuration
├── docker-compose.yml            # Service orchestration
└── package.json                  # Dependencies and scripts
```

## 🚀 Quick Start

### Using Docker (Recommended)

```bash
# Start the indexer
docker-compose up --build

# View logs
docker-compose logs -f zigscan-indexer

# Stop services
docker-compose down
```

### Local Development

```bash
# Install dependencies
npm install

# Copy environment template
cp .env.example .env
# Edit .env with your configuration

# Start the orchestrator
npm start

# Or run individual components
npm run worker      # Start a worker process
npm run indexer     # Run legacy indexer
npm run retry       # Retry failed transactions
npm run check-msgs  # Analyze message types
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file based on `.env.example`:

```bash
# Database Configuration
PGHOST=localhost
PGUSER=postgres
PGPASSWORD=your_password_here
PGDATABASE=zigindexerlocal
PGPORT=5432

# Connection Pool Configuration
ORCH_POOL_MAX=8      # Orchestrator connections
WORKER_POOL_MAX=5    # Worker connections per worker

# Application Configuration
NUM_WORKERS=10       # Number of worker processes
START_HEIGHT=1       # Starting block height
MAX_QUEUE_ITEMS=100  # Work queue size
BATCH_SIZE=5         # Blocks per batch within worker

# RPC Configuration
RPC_ENDPOINTS="http://5.189.171.155:26657,http://5.189.162.146:26657,http://167.86.79.37:26657,http://82.208.20.12:26657"
```

### Docker Configuration

The Docker setup includes:

- **Network Mode**: Host networking for direct database access
- **Resource Limits**: 8GB memory limit, 2GB reserved
- **Connection Pools**: Optimized for high throughput

## 🏗️ Architecture

### Core Components

1. **Orchestrator** (`src/core/orchestrator.js`)

   - Manages 10 parallel worker processes
   - Distributes work in 1000-block batches
   - Handles RPC failover across multiple endpoints
   - Tracks indexing progress and recovers stuck workers

2. **Worker** (`src/core/worker.js`)

   - Processes blocks in internal batches of 5
   - Comprehensive blockchain data extraction
   - Robust error handling with retry logic
   - Database connection management with exponential backoff

3. **Database Layer** (`src/database/`)
   - **db.js**: Optimized connection pooling
   - **models.js**: Comprehensive schema with 15+ tables
   - **partition.js**: Automatic table partitioning utilities

### Database Schema

#### Core Blockchain Tables

- **`blocks`**: Block metadata (height, app_hash, transaction counts)
- **`txs`**: Transaction details (hash, gas usage, codes, logs)
- **`tx_events`**: Event metadata per transaction
- **`tx_event_attrs_json`**: JSONB event attributes with dual format
- **`block_events`**: Finalize-block events
- **`block_event_attrs`**: Block event attributes

#### Type-Specific Tables

- **`type_wasm`** / **`type_wasm_attrs`**: WASM event processing
- **`type_message`** / **`type_message_attrs`**: Message event processing

#### Management Tables

- **`index_state`**: Tracks indexing progress per component
- **`work_queue`**: Job distribution and worker coordination
- **`failed_txs`** / **`failed_blocks`**: Error handling and recovery

#### Advanced Features

- **PostgreSQL Functions**: Automatic type table population
- **Triggers**: Real-time event processing
- **JSONB Storage**: Flexible attribute storage with indexing
- **Foreign Key Relationships**: Data integrity across tables

## 🛠️ Development

### Available Scripts

```bash
npm start           # Run orchestrator
npm run worker      # Run worker process
npm run indexer     # Run legacy indexer
npm run retry       # Retry failed transactions
npm run check-msgs  # Debug message types

# Docker commands
npm run docker:build  # Build Docker image
npm run docker:up     # Start all services
npm run docker:down   # Stop all services
npm run docker:logs   # View application logs
```

### Data Processing Flow

1. **Block Fetching**: Parallel RPC calls with automatic failover
2. **Data Extraction**:
   - Block metadata → `blocks` table
   - Transaction details → `txs` table
   - Event hierarchy → `tx_events` table
   - Attributes → `tx_event_attrs_json` (JSONB)
3. **Type Processing**: Automatic population via PostgreSQL triggers
4. **Error Handling**: Categorized error types with specific recovery

### Error Handling Categories

- 🔌 **Database Connection Errors**: Retry with exponential backoff
- 🌐 **RPC Connectivity Failures**: Automatic endpoint rotation
- ⏭️ **Empty Blocks**: Normal processing (not errors)
- 🔥 **General Processing Errors**: Detailed logging and recovery

## 🔧 Troubleshooting

### Common Issues

1. **"Too Many Clients" Error**:
   - Automatic retry with exponential backoff
   - Reduce `NUM_WORKERS` if persistent
2. **RPC Endpoint Failures**:

   - Automatic failover to backup endpoints
   - Check endpoint availability in logs

3. **Memory Issues**:
   - Increase Docker memory limits
   - Reduce `BATCH_SIZE` for workers

### Monitoring

```bash
# View real-time logs with error categorization
docker-compose logs -f zigscan-indexer

# Check database connections
docker exec -it postgres psql -U postgres -c "SELECT * FROM pg_stat_activity;"

# Monitor indexing progress
docker exec -it postgres psql -U postgres -d zigindexerlocal -c "SELECT * FROM index_state;"

# Check work queue status
docker exec -it postgres psql -U postgres -d zigindexerlocal -c "SELECT status, COUNT(*) FROM work_queue GROUP BY status;"
```

## 📊 Performance

### Current Optimizations

- ✅ **Parallel Processing**: 10 workers processing 1000-block batches
- ✅ **Connection Pooling**: Optimized pools per component
- ✅ **Batch Processing**: Internal 5-block batches for efficiency
- ✅ **Error Recovery**: Automatic retries with backoff
- ✅ **RPC Failover**: Multiple endpoint support
- ✅ **Database Schema**: Comprehensive with triggers and functions

### Expected Performance

- **Throughput**: ~1000-2000 blocks/minute (depending on transaction volume)
- **Memory Usage**: ~6-8GB total (with 8GB limit)
- **Connection Usage**: ~60-80 connections peak
- **Error Recovery**: Automatic handling of 95%+ error scenarios

### Performance Monitoring

Workers provide detailed metrics:

- Blocks/second processing rate
- Transaction throughput
- Empty block detection
- Batch completion summaries
- Error categorization and recovery

## 🔒 Security & Best Practices

- ✅ Environment variables properly gitignored
- ✅ Database credentials externalized
- ✅ Connection pooling prevents exhaustion
- ✅ Resource limits prevent system overload
- ✅ Comprehensive error handling prevents crashes
- ✅ Foreign key constraints ensure data integrity

## 🤝 Contributing

1. Follow the established folder structure
2. Use the comprehensive database schema in `models.js`
3. Add proper error handling with categorization
4. Test with Docker Compose before committing
5. Update documentation for new features
6. Maintain backward compatibility with existing tables

## 📝 License

[Add your license information here]
