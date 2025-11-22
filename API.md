# WebSocket API Documentation

## Connection

Connect to the WebSocket server:

```javascript
const ws = new WebSocket('ws://localhost:8080');
```

## Message Format

All messages are JSON-encoded strings:

```javascript
ws.send(JSON.stringify(message));
```

## Client → Server Messages

### 1. Join Session

Join a collaborative session.

**Request:**
```json
{
  "type": "join",
  "sessionId": "uuid-string",
  "userId": "user-identifier"
}
```

**Response:**
```json
{
  "type": "joined",
  "sessionId": "uuid-string",
  "userId": "user-identifier",
  "containerId": "container-hostname"
}
```

**Broadcast to Other Clients:**
```json
{
  "type": "userJoined",
  "userId": "user-identifier"
}
```

---

### 2. File Operation

Perform a file operation (create, update, delete, rename).

**Create/Update File:**
```json
{
  "type": "fileOperation",
  "data": {
    "type": "create",
    "path": "src/index.ts",
    "content": "console.log('Hello, world!');",
    "userId": "user-identifier",
    "timestamp": "2025-11-22T12:00:00.000Z"
  }
}
```

**Delete File:**
```json
{
  "type": "fileOperation",
  "data": {
    "type": "delete",
    "path": "src/old-file.ts",
    "userId": "user-identifier",
    "timestamp": "2025-11-22T12:00:00.000Z"
  }
}
```

**Rename File:**
```json
{
  "type": "fileOperation",
  "data": {
    "type": "rename",
    "oldPath": "src/old-name.ts",
    "newPath": "src/new-name.ts",
    "userId": "user-identifier",
    "timestamp": "2025-11-22T12:00:00.000Z"
  }
}
```

**Response:**
```json
{
  "type": "fileOperationSuccess"
}
```

**Broadcast to Other Clients:**
```json
{
  "type": "fileOperation",
  "userId": "user-identifier",
  "data": {
    "type": "create|update|delete|rename",
    "path": "...",
    "content": "...",
    ...
  }
}
```

---

### 3. Yjs CRDT Update

Send a Yjs CRDT update for conflict-free synchronization.

**Request:**
```json
{
  "type": "yjsUpdate",
  "data": {
    "docId": "document-identifier",
    "update": [1, 2, 3, ...]
  }
}
```

**Broadcast to Other Clients:**
```json
{
  "type": "yjsUpdate",
  "userId": "user-identifier",
  "data": {
    "docId": "document-identifier",
    "update": [1, 2, 3, ...]
  }
}
```

---

### 4. Get Files

List all files in the workspace.

**Request:**
```json
{
  "type": "getFiles",
  "data": {
    "path": "optional/subdirectory"
  }
}
```

**Response:**
```json
{
  "type": "files",
  "data": [
    "src/index.ts",
    "src/server.ts",
    "package.json",
    ...
  ]
}
```

---

### 5. Get File

Get the content of a specific file.

**Request:**
```json
{
  "type": "getFile",
  "data": {
    "path": "src/index.ts"
  }
}
```

**Response:**
```json
{
  "type": "fileContent",
  "data": {
    "path": "src/index.ts",
    "content": "console.log('Hello, world!');"
  }
}
```

---

### 6. Ping

Send a ping to keep the connection alive.

**Request:**
```json
{
  "type": "ping"
}
```

**Response:**
```json
{
  "type": "pong"
}
```

---

## Server → Client Messages

### User Joined

Broadcast when a new user joins the session.

```json
{
  "type": "userJoined",
  "userId": "user-identifier"
}
```

---

### User Left

Broadcast when a user leaves the session.

```json
{
  "type": "userLeft",
  "userId": "user-identifier"
}
```

---

### Error

Sent when an error occurs.

```json
{
  "type": "error",
  "error": "Error message"
}
```

---

## Complete Example

```javascript
const WebSocket = require('ws');

// Connect to server
const ws = new WebSocket('ws://localhost:8080');

ws.on('open', () => {
  console.log('Connected to server');

  // Join session
  ws.send(JSON.stringify({
    type: 'join',
    sessionId: 'my-session-id',
    userId: 'user-123'
  }));

  // Create a file
  setTimeout(() => {
    ws.send(JSON.stringify({
      type: 'fileOperation',
      data: {
        type: 'create',
        path: 'hello.txt',
        content: 'Hello, collaborative world!',
        userId: 'user-123',
        timestamp: new Date().toISOString()
      }
    }));
  }, 1000);

  // List files
  setTimeout(() => {
    ws.send(JSON.stringify({
      type: 'getFiles',
      data: {}
    }));
  }, 2000);
});

ws.on('message', (data) => {
  const message = JSON.parse(data.toString());
  console.log('Received:', message);

  switch (message.type) {
    case 'joined':
      console.log('Successfully joined session:', message.sessionId);
      break;

    case 'userJoined':
      console.log('User joined:', message.userId);
      break;

    case 'userLeft':
      console.log('User left:', message.userId);
      break;

    case 'fileOperation':
      console.log('File operation from', message.userId, ':', message.data);
      break;

    case 'files':
      console.log('Files in workspace:', message.data);
      break;

    case 'fileContent':
      console.log('File content:', message.data.path, '->', message.data.content);
      break;

    case 'error':
      console.error('Error:', message.error);
      break;
  }
});

ws.on('close', () => {
  console.log('Disconnected from server');
});

ws.on('error', (error) => {
  console.error('WebSocket error:', error);
});
```

---

## Error Codes

### Common Errors

- `"Missing sessionId or userId"` - Join request is missing required fields
- `"Not joined to a session"` - Operation attempted before joining
- `"Session not found"` - Session does not exist
- `"Invalid message format"` - Malformed JSON
- `"Unknown message type"` - Unsupported message type
- `"Failed to apply file operation"` - File operation failed
- `"Failed to apply update"` - Yjs update failed
- `"Failed to list files"` - File listing failed
- `"Failed to get file"` - File read failed

---

## Best Practices

1. **Join First**: Always send a `join` message before any other operations
2. **Handle Disconnects**: Implement reconnection logic for network failures
3. **Ping Regularly**: Send periodic pings to keep connection alive
4. **Validate Responses**: Check for error messages and handle them gracefully
5. **Clean Shutdown**: Close WebSocket connection when done
6. **User IDs**: Use consistent, unique user identifiers
7. **Session IDs**: Use UUIDs for session identifiers
8. **Timestamps**: Always include ISO8601 timestamps in operations

---

## Session Persistence

Sessions are automatically persisted to MinIO:

- **Upload**: When all users disconnect or after inactivity
- **Download**: When first user joins a session
- **Cleanup**: Inactive sessions are cleaned up automatically

The session state includes:
- Workspace files
- Collaboration logs
- Session metadata
- User information

---

## Auto-Commit

If the workspace is a Git repository:

1. Changes are automatically staged
2. After cooldown period (default: 5 minutes) with no activity
3. Changes are committed with user-attributed message
4. `.collaboration` directory is excluded from commits

---

## Rate Limiting

Currently not implemented. Consider adding:

- Connection rate limiting
- Message rate limiting
- File operation size limits
- Session count per user

---

## Security

**Important Security Considerations:**

1. **No Built-in Authentication**: Implement authentication at gateway/proxy level
2. **No Authorization**: All users in a session have full access
3. **Path Traversal**: File paths are validated to prevent directory traversal
4. **Resource Limits**: Configure Docker resource limits to prevent abuse
5. **Network Isolation**: Use Docker overlay networks for service isolation

---

## Monitoring

Monitor these metrics:

- Active WebSocket connections
- Sessions per worker
- File operations per second
- MinIO upload/download rates
- Auto-commit frequency
- Error rates

Access logs via:

```bash
docker service logs -f collaborative-session_collaborative-worker
```
