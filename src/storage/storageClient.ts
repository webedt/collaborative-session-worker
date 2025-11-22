import * as fs from 'fs';
import * as path from 'path';
import * as tar from 'tar';
import * as https from 'https';
import * as http from 'http';

interface SessionMetadata {
  sessionId: string;
  createdAt: string;
  lastModified: string;
  users: string[];
  isGitRepo: boolean;
}

/**
 * Storage client for communicating with storage-worker service
 * Replaces direct MinIO integration in collaborative-session-worker
 */
export class StorageClient {
  private baseUrl: string;
  private timeout: number;
  private workspaceDir: string;
  private enabled: boolean;

  constructor(workspaceDir: string = '/workspace') {
    this.workspaceDir = workspaceDir;

    const storageUrl = process.env.STORAGE_WORKER_URL;
    this.enabled = !!storageUrl;

    if (!this.enabled) {
      console.warn('Storage worker not configured. Running in local-only mode.');
      this.baseUrl = '';
      this.timeout = 60000;
      return;
    }

    this.baseUrl = storageUrl!.replace(/\/$/, '');
    this.timeout = parseInt(process.env.STORAGE_WORKER_TIMEOUT || '60000', 10);

    console.log(`Storage worker client initialized: ${this.baseUrl}`);
  }

  getSessionDir(sessionId: string): string {
    return path.join(this.workspaceDir, `session-${sessionId}`);
  }

  getCollaborationDir(sessionId: string): string {
    return path.join(this.getSessionDir(sessionId), '.collaboration');
  }

  async downloadSession(sessionId: string): Promise<boolean> {
    if (!this.enabled) {
      await this.createLocalSession(sessionId);
      return false;
    }

    const sessionDir = this.getSessionDir(sessionId);
    const tarFile = path.join('/tmp', `${sessionId}.tar.gz`);
    const url = `${this.baseUrl}/api/storage-worker/sessions/${sessionId}/download`;

    try {
      const downloaded = await this.downloadFile(url, tarFile);

      if (!downloaded) {
        console.log(`Session ${sessionId} not found in storage, creating new session`);
        await this.createLocalSession(sessionId);
        return false;
      }

      await fs.promises.mkdir(sessionDir, { recursive: true });
      await tar.x({
        file: tarFile,
        cwd: sessionDir,
      });

      fs.unlinkSync(tarFile);
      console.log(`Downloaded and extracted session ${sessionId}`);
      return true;
    } catch (error: any) {
      console.error(`Error downloading session ${sessionId}:`, error);
      throw error;
    }
  }

  private async createLocalSession(sessionId: string): Promise<void> {
    const sessionDir = this.getSessionDir(sessionId);
    const collaborationDir = this.getCollaborationDir(sessionId);

    await fs.promises.mkdir(sessionDir, { recursive: true });
    await fs.promises.mkdir(collaborationDir, { recursive: true });

    const metadata: SessionMetadata = {
      sessionId,
      createdAt: new Date().toISOString(),
      lastModified: new Date().toISOString(),
      users: [],
      isGitRepo: false,
    };

    await this.saveMetadata(sessionId, metadata);
  }

  async uploadSession(sessionId: string): Promise<void> {
    if (!this.enabled) {
      console.log('Storage worker not configured, skipping upload');
      return;
    }

    const sessionDir = this.getSessionDir(sessionId);
    const tarFile = path.join('/tmp', `${sessionId}.tar.gz`);

    try {
      await tar.c(
        {
          gzip: true,
          file: tarFile,
          cwd: sessionDir,
        },
        ['.']
      );

      const url = `${this.baseUrl}/api/storage-worker/sessions/${sessionId}/upload`;
      await this.uploadFile(url, tarFile);

      fs.unlinkSync(tarFile);
      console.log(`Uploaded session ${sessionId} to storage worker`);
    } catch (error) {
      console.error(`Failed to upload session ${sessionId}:`, error);
      throw error;
    }
  }

  async getMetadata(sessionId: string): Promise<SessionMetadata | null> {
    const metadataPath = path.join(this.getSessionDir(sessionId), 'metadata.json');

    try {
      await fs.promises.access(metadataPath, fs.constants.R_OK);
      const data = await fs.promises.readFile(metadataPath, 'utf-8');
      return JSON.parse(data);
    } catch {
      return null;
    }
  }

  async saveMetadata(sessionId: string, metadata: SessionMetadata): Promise<void> {
    const metadataPath = path.join(this.getSessionDir(sessionId), 'metadata.json');
    metadata.lastModified = new Date().toISOString();
    await fs.promises.writeFile(metadataPath, JSON.stringify(metadata, null, 2));
  }

  async appendCollaborationLog(sessionId: string, userId: string, operation: any): Promise<void> {
    const collaborationDir = this.getCollaborationDir(sessionId);
    const logFile = path.join(collaborationDir, 'operations.log');

    const logEntry = {
      timestamp: new Date().toISOString(),
      userId,
      operation,
    };

    await fs.promises.appendFile(logFile, JSON.stringify(logEntry) + '\n');
  }

  async listSessions(): Promise<string[]> {
    if (!this.enabled) {
      // List local sessions
      try {
        const files = await fs.promises.readdir(this.workspaceDir);
        return files
          .filter(f => f.startsWith('session-'))
          .map(f => f.replace('session-', ''));
      } catch {
        return [];
      }
    }

    const url = `${this.baseUrl}/api/storage-worker/sessions`;

    try {
      const response = await this.makeRequest(url, 'GET');
      const data = JSON.parse(response);
      return (data.sessions || []).map((s: any) => s.sessionId);
    } catch (error) {
      console.error('Failed to list sessions:', error);
      return [];
    }
  }

  async deleteSession(sessionId: string): Promise<void> {
    if (this.enabled) {
      try {
        const url = `${this.baseUrl}/api/storage-worker/sessions/${sessionId}`;
        await this.makeRequest(url, 'DELETE');
      } catch (error) {
        console.error(`Failed to delete session ${sessionId} from storage:`, error);
      }
    }

    const sessionDir = this.getSessionDir(sessionId);
    try {
      await fs.promises.rm(sessionDir, { recursive: true, force: true });
    } catch (error) {
      console.error(`Failed to delete local session ${sessionId}:`, error);
    }
  }

  // Helper methods

  private async uploadFile(url: string, filePath: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const fileStream = fs.createReadStream(filePath);
      const stats = fs.statSync(filePath);
      const protocol = url.startsWith('https') ? https : http;
      const urlObj = new URL(url);

      const req = protocol.request(
        {
          hostname: urlObj.hostname,
          port: urlObj.port,
          path: urlObj.pathname,
          method: 'POST',
          headers: {
            'Content-Type': 'application/gzip',
            'Content-Length': stats.size,
          },
          timeout: this.timeout,
        },
        (res) => {
          let data = '';
          res.on('data', (chunk) => (data += chunk));
          res.on('end', () => {
            if (res.statusCode === 200) {
              resolve();
            } else {
              reject(new Error(`Upload failed: ${res.statusCode} ${data}`));
            }
          });
        }
      );

      req.on('error', reject);
      req.on('timeout', () => {
        req.destroy();
        reject(new Error('Upload timeout'));
      });

      fileStream.pipe(req);
      fileStream.on('error', reject);
    });
  }

  private async downloadFile(url: string, destPath: string): Promise<boolean> {
    return new Promise((resolve, reject) => {
      const protocol = url.startsWith('https') ? https : http;
      const urlObj = new URL(url);

      const req = protocol.request(
        {
          hostname: urlObj.hostname,
          port: urlObj.port,
          path: urlObj.pathname,
          method: 'GET',
          timeout: this.timeout,
        },
        (res) => {
          if (res.statusCode === 404) {
            resolve(false);
            return;
          }

          if (res.statusCode !== 200) {
            reject(new Error(`Download failed: ${res.statusCode}`));
            return;
          }

          const fileStream = fs.createWriteStream(destPath);
          res.pipe(fileStream);

          fileStream.on('finish', () => {
            fileStream.close();
            resolve(true);
          });

          fileStream.on('error', (err) => {
            fs.unlinkSync(destPath);
            reject(err);
          });
        }
      );

      req.on('error', reject);
      req.on('timeout', () => {
        req.destroy();
        reject(new Error('Download timeout'));
      });

      req.end();
    });
  }

  private async makeRequest(url: string, method: string): Promise<string> {
    return new Promise((resolve, reject) => {
      const protocol = url.startsWith('https') ? https : http;
      const urlObj = new URL(url);

      const req = protocol.request(
        {
          hostname: urlObj.hostname,
          port: urlObj.port,
          path: urlObj.pathname,
          method,
          timeout: this.timeout,
        },
        (res) => {
          let data = '';
          res.on('data', (chunk) => (data += chunk));
          res.on('end', () => {
            if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
              resolve(data);
            } else {
              reject(new Error(`Request failed: ${res.statusCode}`));
            }
          });
        }
      );

      req.on('error', reject);
      req.on('timeout', () => {
        req.destroy();
        reject(new Error('Request timeout'));
      });

      req.end();
    });
  }
}
