import * as Y from 'yjs';
import * as fs from 'fs';
import * as path from 'path';
import { StorageClient } from './storage/storageClient';

interface FileOperation {
  type: 'create' | 'update' | 'delete' | 'rename';
  path: string;
  userId: string;
  timestamp: string;
  content?: string;
  oldPath?: string;
  newPath?: string;
}

export class CollaborationManager {
  private docs: Map<string, Y.Doc> = new Map();
  private sessionStorage: StorageClient;
  private sessionId: string;
  private lastActivityTime: number = Date.now();
  private activityTimeouts: Map<string, NodeJS.Timeout> = new Map();

  constructor(sessionId: string, sessionStorage: StorageClient) {
    this.sessionId = sessionId;
    this.sessionStorage = sessionStorage;
  }

  getOrCreateDoc(docId: string): Y.Doc {
    if (!this.docs.has(docId)) {
      const doc = new Y.Doc();
      this.docs.set(docId, doc);
    }
    return this.docs.get(docId)!;
  }

  async applyFileOperation(userId: string, operation: FileOperation): Promise<void> {
    this.updateActivity();

    const sessionDir = this.sessionStorage.getSessionDir(this.sessionId);
    const fullPath = path.join(sessionDir, operation.path);

    await this.sessionStorage.appendCollaborationLog(this.sessionId, userId, operation);

    switch (operation.type) {
      case 'create':
      case 'update':
        if (operation.content !== undefined) {
          await fs.promises.mkdir(path.dirname(fullPath), { recursive: true });
          await fs.promises.writeFile(fullPath, operation.content, 'utf-8');
        }
        break;

      case 'delete':
        try {
          await fs.promises.unlink(fullPath);
        } catch (error: any) {
          if (error.code !== 'ENOENT') throw error;
        }
        break;

      case 'rename':
        if (operation.oldPath && operation.newPath) {
          const oldFullPath = path.join(sessionDir, operation.oldPath);
          const newFullPath = path.join(sessionDir, operation.newPath);
          await fs.promises.mkdir(path.dirname(newFullPath), { recursive: true });
          await fs.promises.rename(oldFullPath, newFullPath);
        }
        break;
    }

    const metadata = await this.sessionStorage.getMetadata(this.sessionId);
    if (metadata) {
      if (!metadata.users.includes(userId)) {
        metadata.users.push(userId);
      }
      await this.sessionStorage.saveMetadata(this.sessionId, metadata);
    }
  }

  async getFileContent(filePath: string): Promise<string | null> {
    const sessionDir = this.sessionStorage.getSessionDir(this.sessionId);
    const fullPath = path.join(sessionDir, filePath);

    try {
      return await fs.promises.readFile(fullPath, 'utf-8');
    } catch {
      return null;
    }
  }

  async listFiles(dirPath: string = ''): Promise<string[]> {
    const sessionDir = this.sessionStorage.getSessionDir(this.sessionId);
    const fullPath = path.join(sessionDir, dirPath);

    try {
      const files = await fs.promises.readdir(fullPath, { withFileTypes: true });
      const result: string[] = [];

      for (const file of files) {
        if (file.name.startsWith('.collaboration')) continue;

        const relativePath = path.join(dirPath, file.name);
        if (file.isDirectory()) {
          const subFiles = await this.listFiles(relativePath);
          result.push(...subFiles);
        } else {
          result.push(relativePath);
        }
      }

      return result;
    } catch {
      return [];
    }
  }

  getYjsUpdate(docId: string): Uint8Array {
    const doc = this.getOrCreateDoc(docId);
    return Y.encodeStateAsUpdate(doc);
  }

  applyYjsUpdate(docId: string, update: Uint8Array): void {
    const doc = this.getOrCreateDoc(docId);
    Y.applyUpdate(doc, update);
    this.updateActivity();
  }

  updateActivity(): void {
    this.lastActivityTime = Date.now();
  }

  getLastActivityTime(): number {
    return this.lastActivityTime;
  }

  isActive(cooldownMs: number = 300000): boolean {
    return Date.now() - this.lastActivityTime < cooldownMs;
  }

  cleanup(): void {
    this.docs.forEach(doc => doc.destroy());
    this.docs.clear();
    this.activityTimeouts.forEach(timeout => clearTimeout(timeout));
    this.activityTimeouts.clear();
  }
}
