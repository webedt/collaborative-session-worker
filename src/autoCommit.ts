import simpleGit, { SimpleGit, CheckRepoActions } from 'simple-git';
import * as fs from 'fs';
import * as path from 'path';
import { StorageClient } from './storage/storageClient';

export class AutoCommit {
  private sessionStorage: StorageClient;
  private sessionId: string;
  private git: SimpleGit;
  private cooldownMs: number;
  private commitTimeout: NodeJS.Timeout | null = null;
  private isGitRepo: boolean = false;

  constructor(sessionId: string, sessionStorage: StorageClient, cooldownMs: number = 300000) {
    this.sessionId = sessionId;
    this.sessionStorage = sessionStorage;
    this.cooldownMs = cooldownMs;

    const sessionDir = this.sessionStorage.getSessionDir(sessionId);
    this.git = simpleGit(sessionDir);
  }

  async initialize(): Promise<void> {
    const sessionDir = this.sessionStorage.getSessionDir(this.sessionId);

    try {
      this.isGitRepo = await this.git.checkIsRepo(CheckRepoActions.IS_REPO_ROOT);

      if (!this.isGitRepo) {
        this.isGitRepo = await this.git.checkIsRepo();
      }

      if (this.isGitRepo) {
        console.log(`Session ${this.sessionId} is a git repository`);

        try {
          await this.git.addConfig('user.name', 'Collaborative Worker', false, 'local');
          await this.git.addConfig('user.email', 'worker@collaborative-session.local', false, 'local');
        } catch (error) {
          console.warn('Failed to set git config:', error);
        }
      } else {
        console.log(`Session ${this.sessionId} is not a git repository - skipping git operations`);
      }

      const metadata = await this.sessionStorage.getMetadata(this.sessionId);
      if (metadata) {
        metadata.isGitRepo = this.isGitRepo;
        await this.sessionStorage.saveMetadata(this.sessionId, metadata);
      }
    } catch (error) {
      console.error('Failed to check git repository status:', error);
      this.isGitRepo = false;
    }
  }

  scheduleCommit(userId: string): void {
    if (this.commitTimeout) {
      clearTimeout(this.commitTimeout);
    }

    this.commitTimeout = setTimeout(async () => {
      await this.commitChanges(userId);
    }, this.cooldownMs);
  }

  async commitChanges(userId: string): Promise<boolean> {
    if (!this.isGitRepo) {
      console.log('Not a git repository, skipping commit');
      return false;
    }

    try {
      const status = await this.git.status();

      if (!status.isClean()) {
        const collaborationDir = path.relative(
          this.sessionStorage.getSessionDir(this.sessionId),
          this.sessionStorage.getCollaborationDir(this.sessionId)
        );

        await this.git.add('./*');

        try {
          await this.git.reset([collaborationDir]);
        } catch {
          // Ignore if .collaboration is not tracked
        }

        const changedFiles = [
          ...status.modified,
          ...status.created,
          ...status.deleted,
          ...status.renamed.map(r => r.to || r.from)
        ];

        const filesList = changedFiles.slice(0, 5).join(', ');
        const moreFiles = changedFiles.length > 5 ? ` and ${changedFiles.length - 5} more` : '';

        const commitMessage = `Auto-commit by user ${userId}\n\nChanges: ${filesList}${moreFiles}`;

        await this.git.commit(commitMessage);
        console.log(`Auto-committed changes for user ${userId} in session ${this.sessionId}`);
        return true;
      } else {
        console.log('No changes to commit');
        return false;
      }
    } catch (error) {
      console.error('Failed to commit changes:', error);
      return false;
    }
  }

  async getRecentCommits(limit: number = 10): Promise<any[]> {
    if (!this.isGitRepo) {
      return [];
    }

    try {
      const log = await this.git.log({ maxCount: limit });
      return [...log.all];
    } catch (error) {
      console.error('Failed to get commits:', error);
      return [];
    }
  }

  cancelScheduledCommit(): void {
    if (this.commitTimeout) {
      clearTimeout(this.commitTimeout);
      this.commitTimeout = null;
    }
  }

  cleanup(): void {
    this.cancelScheduledCommit();
  }
}
