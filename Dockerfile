# Build stage
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN make build

# Final stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates wget

# Create non-root user
RUN addgroup -g 1001 -S aggregator && \
    adduser -u 1001 -S aggregator -G aggregator

# Set working directory
WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/bin/aggregator /app/aggregator

# Change ownership to non-root user
RUN chown -R aggregator:aggregator /app

# Switch to non-root user
USER aggregator

# Expose port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:3000/health || exit 1

# Run the application
CMD ["./aggregator"]