# Collaborative Session Worker

A scalable WebSocket-based collaborative coding session worker with MinIO persistence, CRDT synchronization, and auto-commit functionality.

## Features

- **Real-time Collaboration**: Multiple users can work on the same session simultaneously
- **Conflict-Free Synchronization**: Uses Yjs CRDT for automatic conflict resolution
- **Auto-Commit**: Automatically commits changes after cooldown period
- **Persistent Storage**: MinIO-backed session persistence
- **Ephemeral Workers**: Stateless workers that can be scaled horizontally
- **Docker Swarm**: Production-ready orchestration with load balancing
- **Append-Only Logs**: Complete audit trail of all operations

## Quick Start

### Development

```bash
# Install dependencies
npm install

# Start development server
npm run dev

# Build for production
npm run build
npm start
```

### Docker Swarm Deployment

```bash
# Deploy the stack
docker stack deploy -c swarm.yml collaborative-session

# Check status
docker stack services collaborative-session

# View logs
docker service logs collaborative-session_collaborative-worker
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Load Balancer                         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌────────────────────────────────────────┐
        │    Collaborative Worker (5 replicas)    │
        │  - WebSocket Server                     │
        │  - Collaboration Manager (Yjs CRDT)     │
        │  - Auto-Commit                          │
        └────────────────────────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │      MinIO       │
                    │ (Session Storage) │
                    └──────────────────┘
```

## WebSocket Protocol

### Connect to Session

```javascript
const ws = new WebSocket('ws://localhost:8080');

ws.send(JSON.stringify({
  type: 'join',
  sessionId: 'your-session-id',
  userId: 'your-user-id'
}));
```

### File Operations

```javascript
// Create/Update file
ws.send(JSON.stringify({
  type: 'fileOperation',
  data: {
    type: 'create',
    path: 'src/index.ts',
    content: 'console.log("Hello, world!");',
    userId: 'user-1',
    timestamp: new Date().toISOString()
  }
}));

// Delete file
ws.send(JSON.stringify({
  type: 'fileOperation',
  data: {
    type: 'delete',
    path: 'src/old-file.ts',
    userId: 'user-1',
    timestamp: new Date().toISOString()
  }
}));
```

### CRDT Synchronization

```javascript
// Apply Yjs update
ws.send(JSON.stringify({
  type: 'yjsUpdate',
  data: {
    docId: 'document-id',
    update: Array.from(updateUint8Array)
  }
}));
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PORT` | `8080` | WebSocket server port |
| `WORKSPACE_DIR` | `/workspace` | Base directory for workspaces |
| `COOLDOWN_MS` | `300000` | Auto-commit cooldown (5 minutes) |
| `MINIO_ENDPOINT` | - | MinIO server endpoint |
| `MINIO_PORT` | `9000` | MinIO server port |
| `MINIO_BUCKET` | `sessions` | MinIO bucket name |
| `MINIO_ACCESS_KEY` | - | MinIO access key |
| `MINIO_SECRET_KEY` | - | MinIO secret key |

## How It Works

### 1. Session Lifecycle

1. Client connects via WebSocket and sends a `join` message
2. Worker downloads session from MinIO (if it exists)
3. Multiple clients can join the same session and collaborate
4. All changes are synchronized using Yjs CRDT
5. After cooldown period with no activity, changes are auto-committed
6. When all clients disconnect, session is uploaded to MinIO
7. Worker cleans up inactive sessions automatically

### 2. Collaboration

- **File Operations**: Create, update, delete, rename files
- **CRDT Sync**: Real-time synchronization without conflicts
- **Append-Only Log**: All operations logged for audit trail

### 3. Auto-Commit

**For Git Repositories:**
- Automatically detects git repositories
- Commits changes after cooldown period
- Excludes `.collaboration` directory
- User-attributed commit messages

**For Non-Git Workspaces:**
- Maintains collaboration logs only
- No git operations performed
- Session persists in MinIO

### 4. Ephemeral Workers

Workers are designed to be ephemeral:
- No local state (everything in MinIO)
- Can be scaled up/down dynamically
- Automatic cleanup and session upload
- Graceful shutdown handling

## Production Deployment

### Resource Requirements

**Per Worker:**
- CPU: 1-2 cores
- Memory: 2-4 GB
- Replicas: 5 (configurable)

**MinIO:**
- CPU: 0.5-1 core
- Memory: 512MB-2GB
- Storage: Based on session data

### Scaling

Horizontal scaling is supported out of the box:

```bash
# Scale workers
docker service scale collaborative-session_collaborative-worker=10

# Update configuration
docker service update --env-add COOLDOWN_MS=600000 collaborative-session_collaborative-worker
```

### Monitoring

```bash
# View service status
docker service ps collaborative-session_collaborative-worker

# Stream logs
docker service logs -f collaborative-session_collaborative-worker

# Check MinIO
docker exec -it $(docker ps -q -f name=collaborative-session_minio) mc ls /data/sessions/
```

## Development Guide

### Project Structure

```
collaborative-session-worker/
├── src/
│   ├── server.ts                  # WebSocket server
│   ├── collaborationManager.ts    # CRDT synchronization
│   ├── autoCommit.ts              # Auto-commit logic
│   └── storage/
│       └── sessionStorage.ts      # MinIO integration
├── Dockerfile
├── swarm.yml                      # Docker Swarm config
├── package.json
├── tsconfig.json
└── CLAUDE.md                      # Detailed documentation
```

### Testing

Create a test client:

```javascript
const WebSocket = require('ws');

const ws = new WebSocket('ws://localhost:8080');

ws.on('open', () => {
  ws.send(JSON.stringify({
    type: 'join',
    sessionId: 'test-session',
    userId: 'test-user'
  }));
});

ws.on('message', (data) => {
  const message = JSON.parse(data);
  console.log('Received:', message);
});
```

## Comparison with AI Coding Worker

| Feature | AI Coding Worker | Collaborative Session Worker |
|---------|------------------|----------------------------|
| **Protocol** | HTTP + SSE | WebSocket |
| **Purpose** | AI-assisted coding | Real-time collaboration |
| **Concurrency** | One task per worker | Multiple users per session |
| **Persistence** | MinIO | MinIO |
| **CRDT** | No | Yes (Yjs) |
| **Auto-Commit** | Manual | Automatic |
| **Ephemeral** | Yes | Yes |

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with Docker Swarm
5. Submit a pull request

## License

MIT

## Related Projects

- [AI Coding Worker](https://github.com/webedt/ai-coding-worker) - AI-assisted coding with multiple providers
- [Yjs](https://github.com/yjs/yjs) - CRDT framework for real-time collaboration

## Support

For issues and questions:
- GitHub Issues: [Report a bug](https://github.com/webedt/collaborative-session-worker/issues)
- Documentation: See [CLAUDE.md](./CLAUDE.md) for detailed documentation
