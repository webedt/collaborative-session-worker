import WebSocket from 'ws';
import { v4 as uuidv4 } from 'uuid';
import { StorageClient } from './storage/storageClient';
import { CollaborationManager } from './collaborationManager';
import { AutoCommit } from './autoCommit';
import * as os from 'os';

const PORT = parseInt(process.env.PORT || '8080', 10);
const WORKSPACE_DIR = process.env.WORKSPACE_DIR || '/workspace';
const COOLDOWN_MS = parseInt(process.env.COOLDOWN_MS || '300000', 10);
const CONTAINER_ID = os.hostname();

interface ClientConnection {
  ws: WebSocket;
  userId: string;
  sessionId: string;
  alive: boolean;
}

interface Message {
  type: 'join' | 'fileOperation' | 'yjsUpdate' | 'getFiles' | 'getFile' | 'ping';
  sessionId?: string;
  userId?: string;
  data?: any;
}

class CollaborativeSessionServer {
  private wss: WebSocket.Server;
  private sessionStorage: StorageClient;
  private sessions: Map<string, {
    manager: CollaborationManager;
    autoCommit: AutoCommit;
    clients: Set<ClientConnection>;
  }> = new Map();
  private cleanupInterval: NodeJS.Timeout;

  constructor() {
    this.sessionStorage = new StorageClient(WORKSPACE_DIR);
    this.wss = new WebSocket.Server({ port: PORT });

    this.wss.on('connection', this.handleConnection.bind(this));

    this.cleanupInterval = setInterval(() => {
      this.cleanupInactiveSessions();
    }, 60000);

    console.log(`Collaborative Session Worker started on port ${PORT}`);
    console.log(`Container ID: ${CONTAINER_ID}`);
    console.log(`Workspace directory: ${WORKSPACE_DIR}`);
    console.log(`Auto-commit cooldown: ${COOLDOWN_MS}ms`);
  }

  private handleConnection(ws: WebSocket): void {
    console.log('New WebSocket connection');

    const client: Partial<ClientConnection> = {
      ws,
      alive: true,
    };

    ws.on('message', async (data: WebSocket.Data) => {
      try {
        const message: Message = JSON.parse(data.toString());
        await this.handleMessage(client as ClientConnection, message);
      } catch (error) {
        console.error('Error handling message:', error);
        this.sendError(ws, 'Invalid message format');
      }
    });

    ws.on('pong', () => {
      client.alive = true;
    });

    ws.on('close', () => {
      this.handleDisconnect(client as ClientConnection);
    });

    ws.on('error', (error) => {
      console.error('WebSocket error:', error);
      this.handleDisconnect(client as ClientConnection);
    });
  }

  private async handleMessage(client: ClientConnection, message: Message): Promise<void> {
    switch (message.type) {
      case 'join':
        await this.handleJoin(client, message);
        break;

      case 'fileOperation':
        await this.handleFileOperation(client, message);
        break;

      case 'yjsUpdate':
        await this.handleYjsUpdate(client, message);
        break;

      case 'getFiles':
        await this.handleGetFiles(client, message);
        break;

      case 'getFile':
        await this.handleGetFile(client, message);
        break;

      case 'ping':
        this.handlePing(client);
        break;

      default:
        this.sendError(client.ws, 'Unknown message type');
    }
  }

  private async handleJoin(client: ClientConnection, message: Message): Promise<void> {
    if (!message.sessionId || !message.userId) {
      this.sendError(client.ws, 'Missing sessionId or userId');
      return;
    }

    client.sessionId = message.sessionId;
    client.userId = message.userId;

    let session = this.sessions.get(message.sessionId);

    if (!session) {
      await this.sessionStorage.downloadSession(message.sessionId);

      const manager = new CollaborationManager(message.sessionId, this.sessionStorage);
      const autoCommit = new AutoCommit(message.sessionId, this.sessionStorage, COOLDOWN_MS);
      await autoCommit.initialize();

      session = {
        manager,
        autoCommit,
        clients: new Set(),
      };

      this.sessions.set(message.sessionId, session);
      console.log(`Created new session: ${message.sessionId}`);
    }

    session.clients.add(client);

    this.send(client.ws, {
      type: 'joined',
      sessionId: message.sessionId,
      userId: message.userId,
      containerId: CONTAINER_ID,
    });

    this.broadcastToSession(message.sessionId, {
      type: 'userJoined',
      userId: message.userId,
    }, client);

    console.log(`User ${message.userId} joined session ${message.sessionId}`);
  }

  private async handleFileOperation(client: ClientConnection, message: Message): Promise<void> {
    if (!client.sessionId) {
      this.sendError(client.ws, 'Not joined to a session');
      return;
    }

    const session = this.sessions.get(client.sessionId);
    if (!session) {
      this.sendError(client.ws, 'Session not found');
      return;
    }

    try {
      await session.manager.applyFileOperation(client.userId, message.data);

      this.broadcastToSession(client.sessionId, {
        type: 'fileOperation',
        userId: client.userId,
        data: message.data,
      }, client);

      session.autoCommit.scheduleCommit(client.userId);

      this.send(client.ws, {
        type: 'fileOperationSuccess',
      });
    } catch (error) {
      console.error('Error applying file operation:', error);
      this.sendError(client.ws, 'Failed to apply file operation');
    }
  }

  private async handleYjsUpdate(client: ClientConnection, message: Message): Promise<void> {
    if (!client.sessionId) {
      this.sendError(client.ws, 'Not joined to a session');
      return;
    }

    const session = this.sessions.get(client.sessionId);
    if (!session) {
      this.sendError(client.ws, 'Session not found');
      return;
    }

    try {
      const update = new Uint8Array(message.data.update);
      session.manager.applyYjsUpdate(message.data.docId, update);

      this.broadcastToSession(client.sessionId, {
        type: 'yjsUpdate',
        userId: client.userId,
        data: message.data,
      }, client);

      session.autoCommit.scheduleCommit(client.userId);
    } catch (error) {
      console.error('Error applying Yjs update:', error);
      this.sendError(client.ws, 'Failed to apply update');
    }
  }

  private async handleGetFiles(client: ClientConnection, message: Message): Promise<void> {
    if (!client.sessionId) {
      this.sendError(client.ws, 'Not joined to a session');
      return;
    }

    const session = this.sessions.get(client.sessionId);
    if (!session) {
      this.sendError(client.ws, 'Session not found');
      return;
    }

    try {
      const files = await session.manager.listFiles(message.data?.path || '');
      this.send(client.ws, {
        type: 'files',
        data: files,
      });
    } catch (error) {
      console.error('Error listing files:', error);
      this.sendError(client.ws, 'Failed to list files');
    }
  }

  private async handleGetFile(client: ClientConnection, message: Message): Promise<void> {
    if (!client.sessionId) {
      this.sendError(client.ws, 'Not joined to a session');
      return;
    }

    const session = this.sessions.get(client.sessionId);
    if (!session) {
      this.sendError(client.ws, 'Session not found');
      return;
    }

    try {
      const content = await session.manager.getFileContent(message.data.path);
      this.send(client.ws, {
        type: 'fileContent',
        data: {
          path: message.data.path,
          content,
        },
      });
    } catch (error) {
      console.error('Error getting file:', error);
      this.sendError(client.ws, 'Failed to get file');
    }
  }

  private handlePing(client: ClientConnection): void {
    this.send(client.ws, { type: 'pong' });
  }

  private handleDisconnect(client: ClientConnection): void {
    if (!client.sessionId) return;

    const session = this.sessions.get(client.sessionId);
    if (!session) return;

    session.clients.delete(client);

    this.broadcastToSession(client.sessionId, {
      type: 'userLeft',
      userId: client.userId,
    });

    console.log(`User ${client.userId} left session ${client.sessionId}`);

    if (session.clients.size === 0) {
      this.scheduleSessionCleanup(client.sessionId);
    }
  }

  private scheduleSessionCleanup(sessionId: string): void {
    setTimeout(async () => {
      const session = this.sessions.get(sessionId);
      if (session && session.clients.size === 0) {
        await this.cleanupSession(sessionId);
      }
    }, 30000);
  }

  private async cleanupSession(sessionId: string): Promise<void> {
    const session = this.sessions.get(sessionId);
    if (!session) return;

    console.log(`Cleaning up session: ${sessionId}`);

    session.manager.cleanup();
    session.autoCommit.cleanup();

    await this.sessionStorage.uploadSession(sessionId);

    this.sessions.delete(sessionId);
    console.log(`Session ${sessionId} cleaned up and uploaded to MinIO`);
  }

  private async cleanupInactiveSessions(): Promise<void> {
    const now = Date.now();

    for (const [sessionId, session] of this.sessions.entries()) {
      if (session.clients.size === 0 && !session.manager.isActive(COOLDOWN_MS)) {
        await this.cleanupSession(sessionId);
      }
    }
  }

  private broadcastToSession(sessionId: string, message: any, exclude?: ClientConnection): void {
    const session = this.sessions.get(sessionId);
    if (!session) return;

    const messageStr = JSON.stringify(message);

    for (const client of session.clients) {
      if (client !== exclude && client.ws.readyState === WebSocket.OPEN) {
        client.ws.send(messageStr);
      }
    }
  }

  private send(ws: WebSocket, message: any): void {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
  }

  private sendError(ws: WebSocket, error: string): void {
    this.send(ws, {
      type: 'error',
      error,
    });
  }

  public async shutdown(): Promise<void> {
    console.log('Shutting down server...');

    clearInterval(this.cleanupInterval);

    for (const [sessionId] of this.sessions) {
      await this.cleanupSession(sessionId);
    }

    this.wss.close();
    console.log('Server shut down');
  }
}

const server = new CollaborativeSessionServer();

process.on('SIGTERM', async () => {
  console.log('Received SIGTERM signal');
  await server.shutdown();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('Received SIGINT signal');
  await server.shutdown();
  process.exit(0);
});
