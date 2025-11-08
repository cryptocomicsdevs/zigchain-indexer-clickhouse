# Dockerfile
FROM node:18

# Create app directory
WORKDIR /app/zigscan-indexer

# Copy package files first for better Docker layer caching
COPY package*.json ./

# Install production dependencies only
RUN npm ci --production

# Copy source code
COPY src/ ./src/
COPY scripts/ ./scripts/

# Expose port (not actually used by this app, but good practice)
EXPOSE 3000

# Default command - runs orchestrator from new location
CMD ["node", "src/core/orchestrator.js"]
