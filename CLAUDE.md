# Collaborative Session Worker

A WebSocket-based collaborative session worker with MinIO persistence, CRDT synchronization, and auto-commit functionality.

## Architecture Overview

The Collaborative Session Worker provides real-time collaboration capabilities for coding sessions with the following components:

### Core Components

1. **WebSocket Server** (`src/server.ts`)
   - Handles client connections and message routing
   - Manages session lifecycle
   - Coordinates between collaboration manager and auto-commit

2. **Session Storage** (`src/storage/sessionStorage.ts`)
   - MinIO integration for persistent session storage
   - Session metadata management
   - Collaboration log persistence

3. **Collaboration Manager** (`src/collaborationManager.ts`)
   - CRDT-based conflict-free synchronization using Yjs
   - File operation handling (create, update, delete, rename)
   - Real-time change propagation

4. **Auto-Commit** (`src/autoCommit.ts`)
   - Automatic git commits after cooldown period
   - Git repository detection
   - User-attributed commits

## How It Works

### Session Lifecycle

1. **Client Connection**: Client connects via WebSocket and joins a session
2. **Session Download**: Worker downloads session from MinIO (if exists)
3. **Collaboration**: Multiple users can edit files simultaneously
4. **Auto-Commit**: After cooldown period with no activity, changes are committed
5. **Session Upload**: On disconnect or cleanup, session is uploaded to MinIO
6. **Ephemeral Cleanup**: Worker cleans up inactive sessions automatically

### Workspace Structure

Each session gets an isolated workspace:

```
/workspace/session-{uuid}/
├── .collaboration/
│   └── operations.log        # Append-only change log
├── metadata.json             # Session metadata
└── [user files]              # Actual workspace files
```

### Message Types

#### Client → Server

- `join`: Join a session
  ```json
  {
    "type": "join",
    "sessionId": "uuid",
    "userId": "user-id"
  }
  ```

- `fileOperation`: Perform file operation
  ```json
  {
    "type": "fileOperation",
    "data": {
      "type": "create|update|delete|rename",
      "path": "file/path",
      "content": "file content",
      "userId": "user-id",
      "timestamp": "ISO8601"
    }
  }
  ```

- `yjsUpdate`: Yjs CRDT update
  ```json
  {
    "type": "yjsUpdate",
    "data": {
      "docId": "document-id",
      "update": [uint8array]
    }
  }
  ```

- `getFiles`: List files in workspace
  ```json
  {
    "type": "getFiles",
    "data": {
      "path": "optional/path"
    }
  }
  ```

- `getFile`: Get file content
  ```json
  {
    "type": "getFile",
    "data": {
      "path": "file/path"
    }
  }
  ```

#### Server → Client

- `joined`: Successfully joined session
- `userJoined`: Another user joined
- `userLeft`: User left session
- `fileOperation`: File operation from another user
- `yjsUpdate`: CRDT update from another user
- `files`: File list response
- `fileContent`: File content response
- `error`: Error message

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | WebSocket server port |
| `WORKSPACE_DIR` | `/workspace` | Base directory for workspaces |
| `COOLDOWN_MS` | `300000` | Auto-commit cooldown (5 minutes) |
| `MINIO_ENDPOINT` | - | MinIO server endpoint |
| `MINIO_PORT` | `9000` | MinIO server port |
| `MINIO_BUCKET` | `sessions` | MinIO bucket name |
| `MINIO_ACCESS_KEY` | - | MinIO access key |
| `MINIO_SECRET_KEY` | - | MinIO secret key |

## Deployment

### Docker Swarm

The service is designed to run in Docker Swarm with:

- **5 worker replicas** for load balancing
- **MinIO** for persistent session storage
- **Overlay network** for inter-service communication
- **Rolling updates** with automatic rollback

Deploy the stack:

```bash
docker stack deploy -c swarm.yml collaborative-session
```

### Resource Allocation

**Per Worker:**
- CPU: 1-2 cores
- Memory: 2-4 GB

**MinIO:**
- CPU: 0.5-1 core
- Memory: 512MB-2GB

## Features

### Conflict-Free Collaboration

Uses Yjs CRDT for conflict-free synchronization:
- Multiple users can edit the same document
- Changes are automatically merged
- No conflict resolution needed

### Append-Only Log

All operations are logged in an append-only format:
- No delete operations (deletions are logged as operations)
- Complete audit trail
- Enables replay and debugging

### Auto-Commit Behavior

**Git Repository:**
- Detects if workspace is a git repository
- Commits changes after cooldown period
- Excludes `.collaboration` directory from commits
- User-attributed commit messages

**Non-Git Workspace:**
- Maintains collaboration logs only
- No git operations performed
- Session state persists in MinIO

### Ephemeral Workers

Workers are stateless and ephemeral:
- Session state stored in MinIO
- Workers can be scaled up/down
- Automatic cleanup of inactive sessions
- Graceful shutdown with session upload

## Development

### Local Development

1. Install dependencies:
   ```bash
   npm install
   ```

2. Start development server:
   ```bash
   npm run dev
   ```

3. Build for production:
   ```bash
   npm run build
   npm start
   ```

### Testing with WebSocket Client

```javascript
const WebSocket = require('ws');

const ws = new WebSocket('ws://localhost:8080');

ws.on('open', () => {
  // Join session
  ws.send(JSON.stringify({
    type: 'join',
    sessionId: 'test-session',
    userId: 'user-1'
  }));

  // Create file
  ws.send(JSON.stringify({
    type: 'fileOperation',
    data: {
      type: 'create',
      path: 'test.txt',
      content: 'Hello, world!',
      userId: 'user-1',
      timestamp: new Date().toISOString()
    }
  }));
});

ws.on('message', (data) => {
  console.log('Received:', JSON.parse(data));
});
```

## Monitoring

### Health Checks

The server logs:
- Session creation/cleanup
- User connections/disconnections
- File operations
- Auto-commits
- Errors and warnings

### Session Management

List active sessions via MinIO or local workspace directory:

```bash
# Local sessions
ls /workspace/

# MinIO sessions (if configured)
mc ls minio/sessions/
```

## Security Considerations

1. **User Authentication**: Not implemented - should be added at gateway/proxy level
2. **Authorization**: No built-in access control
3. **Data Validation**: Basic validation of message structure
4. **Git Operations**: Runs with worker user permissions
5. **Network Isolation**: Uses overlay network in Swarm

## Troubleshooting

### Sessions Not Persisting

- Check MinIO configuration and connectivity
- Verify bucket exists and is accessible
- Check worker logs for upload/download errors

### Auto-Commit Not Working

- Verify workspace is a git repository
- Check git configuration in workspace
- Ensure cooldown period has elapsed
- Check worker logs for git errors

### High Memory Usage

- Reduce number of concurrent sessions per worker
- Lower worker replicas
- Increase cleanup frequency
- Monitor Yjs document sizes

## License

MIT
