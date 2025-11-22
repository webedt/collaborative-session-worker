#!/bin/bash

set -e

echo "Deploying Collaborative Session Worker to Docker Swarm..."

# Check if swarm is initialized
if ! docker info | grep -q "Swarm: active"; then
    echo "Docker Swarm is not initialized. Initializing..."
    docker swarm init
fi

# Deploy the stack
echo "Deploying stack..."
docker stack deploy -c swarm.yml collaborative-session

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 10

# Show status
echo ""
echo "Stack deployed successfully!"
echo ""
echo "Services:"
docker stack services collaborative-session

echo ""
echo "MinIO Console: http://localhost:9001"
echo "MinIO API: http://localhost:9000"
echo "WebSocket Server: ws://localhost:8080"
echo ""
echo "To view logs:"
echo "  docker service logs -f collaborative-session_collaborative-worker"
echo "  docker service logs -f collaborative-session_minio"
echo ""
echo "To remove stack:"
echo "  docker stack rm collaborative-session"
