FROM node:20-slim

# Install git for repository operations
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1001 worker

# Create workspace directory
RUN mkdir -p /workspace && chown worker:worker /workspace

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./
COPY tsconfig.json ./

# Install dependencies
RUN npm install

# Copy source code
COPY src ./src

# Build TypeScript
RUN npm run build

# Configure git for the worker user
RUN git config --global user.name "Collaborative Worker" && \
    git config --global user.email "worker@collaborative-session.local"

# Set environment variables
ENV PORT=8080
ENV WORKSPACE_DIR=/workspace
ENV NODE_ENV=production
ENV COOLDOWN_MS=300000

# Expose WebSocket port
EXPOSE 8080

# Switch to non-root user
USER worker

# Start the server
CMD ["npm", "start"]
