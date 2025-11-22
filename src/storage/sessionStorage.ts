import * as Minio from 'minio';
import * as fs from 'fs';
import * as path from 'path';
import * as tar from 'tar';
import { promisify } from 'util';

const mkdir = promisify(fs.mkdir);
const writeFile = promisify(fs.writeFile);
const readFile = promisify(fs.readFile);
const access = promisify(fs.access);

interface SessionMetadata {
  sessionId: string;
  createdAt: string;
  lastModified: string;
  users: string[];
  isGitRepo: boolean;
}

export class SessionStorage {
  private minioClient: Minio.Client | null = null;
  private bucket: string;
  private workspaceDir: string;

  constructor(workspaceDir: string = '/workspace') {
    this.workspaceDir = workspaceDir;
    this.bucket = process.env.MINIO_BUCKET || 'sessions';

    const endpoint = process.env.MINIO_ENDPOINT;
    const port = parseInt(process.env.MINIO_PORT || '9000', 10);
    const accessKey = process.env.MINIO_ACCESS_KEY;
    const secretKey = process.env.MINIO_SECRET_KEY;

    if (endpoint && accessKey && secretKey) {
      this.minioClient = new Minio.Client({
        endPoint: endpoint,
        port: port,
        useSSL: false,
        accessKey: accessKey,
        secretKey: secretKey,
      });
      this.initializeBucket();
    } else {
      console.warn('MinIO not configured. Running in local-only mode.');
    }
  }

  private async initializeBucket(): Promise<void> {
    if (!this.minioClient) return;

    try {
      const exists = await this.minioClient.bucketExists(this.bucket);
      if (!exists) {
        await this.minioClient.makeBucket(this.bucket, 'us-east-1');
        console.log(`Created MinIO bucket: ${this.bucket}`);
      }
    } catch (error) {
      console.error('Failed to initialize MinIO bucket:', error);
    }
  }

  getSessionDir(sessionId: string): string {
    return path.join(this.workspaceDir, `session-${sessionId}`);
  }

  getCollaborationDir(sessionId: string): string {
    return path.join(this.getSessionDir(sessionId), '.collaboration');
  }

  async downloadSession(sessionId: string): Promise<boolean> {
    if (!this.minioClient) {
      await this.createLocalSession(sessionId);
      return false;
    }

    const sessionDir = this.getSessionDir(sessionId);
    const tarFile = path.join('/tmp', `${sessionId}.tar.gz`);

    try {
      await this.minioClient.fGetObject(this.bucket, `${sessionId}.tar.gz`, tarFile);

      await mkdir(sessionDir, { recursive: true });
      await tar.x({
        file: tarFile,
        cwd: sessionDir,
      });

      fs.unlinkSync(tarFile);
      console.log(`Downloaded and extracted session ${sessionId}`);
      return true;
    } catch (error: any) {
      if (error.code === 'NotFound' || error.code === 'NoSuchKey') {
        console.log(`Session ${sessionId} not found in MinIO, creating new session`);
        await this.createLocalSession(sessionId);
        return false;
      }
      throw error;
    }
  }

  private async createLocalSession(sessionId: string): Promise<void> {
    const sessionDir = this.getSessionDir(sessionId);
    const collaborationDir = this.getCollaborationDir(sessionId);

    await mkdir(sessionDir, { recursive: true });
    await mkdir(collaborationDir, { recursive: true });

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
    if (!this.minioClient) {
      console.log('MinIO not configured, skipping upload');
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

      await this.minioClient.fPutObject(this.bucket, `${sessionId}.tar.gz`, tarFile);
      fs.unlinkSync(tarFile);
      console.log(`Uploaded session ${sessionId} to MinIO`);
    } catch (error) {
      console.error(`Failed to upload session ${sessionId}:`, error);
      throw error;
    }
  }

  async getMetadata(sessionId: string): Promise<SessionMetadata | null> {
    const metadataPath = path.join(this.getSessionDir(sessionId), 'metadata.json');

    try {
      await access(metadataPath, fs.constants.R_OK);
      const data = await readFile(metadataPath, 'utf-8');
      return JSON.parse(data);
    } catch {
      return null;
    }
  }

  async saveMetadata(sessionId: string, metadata: SessionMetadata): Promise<void> {
    const metadataPath = path.join(this.getSessionDir(sessionId), 'metadata.json');
    metadata.lastModified = new Date().toISOString();
    await writeFile(metadataPath, JSON.stringify(metadata, null, 2));
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
    if (!this.minioClient) {
      const sessions: string[] = [];
      try {
        const files = await fs.promises.readdir(this.workspaceDir);
        return files
          .filter(f => f.startsWith('session-'))
          .map(f => f.replace('session-', ''));
      } catch {
        return [];
      }
    }

    const sessions: string[] = [];
    const stream = this.minioClient.listObjects(this.bucket, '', true);

    return new Promise((resolve, reject) => {
      stream.on('data', (obj) => {
        if (obj.name && obj.name.endsWith('.tar.gz')) {
          sessions.push(obj.name.replace('.tar.gz', ''));
        }
      });
      stream.on('end', () => resolve(sessions));
      stream.on('error', reject);
    });
  }

  async deleteSession(sessionId: string): Promise<void> {
    if (this.minioClient) {
      try {
        await this.minioClient.removeObject(this.bucket, `${sessionId}.tar.gz`);
      } catch (error) {
        console.error(`Failed to delete session ${sessionId} from MinIO:`, error);
      }
    }

    const sessionDir = this.getSessionDir(sessionId);
    try {
      await fs.promises.rm(sessionDir, { recursive: true, force: true });
    } catch (error) {
      console.error(`Failed to delete local session ${sessionId}:`, error);
    }
  }
}
