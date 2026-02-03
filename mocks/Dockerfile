# Use a lightweight Node.js image
FROM node:18-alpine

# Install a simple static server
RUN npm install -g http-server

# Create a placeholder directory
WORKDIR /app

# Expose the port used in your docker-compose
EXPOSE 8000

# Start the server
CMD ["http-server", "-p", "8000"]