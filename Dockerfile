ARG GO_IMAGE=golang:1.25.0-alpine3.22
ARG RUNTIME_IMAGE=alpine:3.22

FROM ${GO_IMAGE} AS rocksdb-builder

ARG ROCKSDB_VERSION=8.10.0
ARG ROCKSDB_SHA256=2dc107551cc864dbcf7908fdee96f2318cbb680df2b3fe1f85b0d545c2b5673b
ARG ROCKSDB_BUILD_JOBS=4
ARG ROCKSDB_PORTABLE=0

RUN apk add --no-cache \
    bash \
    build-base \
    bzip2-dev \
    curl \
    linux-headers \
    lz4-dev \
    perl \
    snappy-dev \
    tar \
    zlib-dev \
    zstd-dev

WORKDIR /tmp/rocksdb-src

RUN curl -fsSL "https://github.com/facebook/rocksdb/archive/refs/tags/v${ROCKSDB_VERSION}.tar.gz" -o /tmp/rocksdb.tar.gz && \
    echo "${ROCKSDB_SHA256}  /tmp/rocksdb.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/rocksdb.tar.gz --strip-components=1 -C /tmp/rocksdb-src

RUN PORTABLE="${ROCKSDB_PORTABLE}" USE_RTTI=1 DEBUG_LEVEL=0 DISABLE_WARNING_AS_ERROR=1 EXTRA_CXXFLAGS="-include cstdint" make -j"${ROCKSDB_BUILD_JOBS}" static_lib && \
    mkdir -p /opt/rocksdb/include /opt/rocksdb/lib && \
    cp -R include/rocksdb /opt/rocksdb/include/ && \
    cp librocksdb.a /opt/rocksdb/lib/

# Build stage
FROM ${GO_IMAGE} AS builder

ARG BUILD_TAGS=rocksdb

RUN apk add --no-cache \
    build-base \
    bzip2-dev \
    git \
    linux-headers \
    lz4-dev \
    snappy-dev \
    zlib-dev \
    zstd-dev

# Set working directory
WORKDIR /app

COPY --from=rocksdb-builder /opt/rocksdb /opt/rocksdb

ENV CGO_ENABLED=1
ENV CGO_CFLAGS="-I/opt/rocksdb/include"
ENV CGO_LDFLAGS="-L/opt/rocksdb/lib"

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN if [ -n "$BUILD_TAGS" ]; then \
      go build -tags "$BUILD_TAGS" -o /app/bin/aggregator ./cmd/aggregator; \
    else \
      go build -o /app/bin/aggregator ./cmd/aggregator; \
    fi

FROM builder AS test

RUN if [ -n "$BUILD_TAGS" ]; then \
      go test -tags "$BUILD_TAGS" ./internal/smt/disk/rocksstore ./cmd/disk-smt-benchmark ./internal/smt/disk/...; \
    else \
      go test ./internal/smt/disk/...; \
    fi

# Final stage
FROM ${RUNTIME_IMAGE}

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    libbz2 \
    libstdc++ \
    lz4-libs \
    snappy \
    wget \
    zlib \
    zstd-libs

# Create non-root user
RUN addgroup -g 1001 -S aggregator && \
    adduser -u 1001 -S aggregator -G aggregator

# Set working directory
WORKDIR /app

# Copy binary from builder stage. RocksDB is statically linked into the Go
# binary; only compression/runtime libraries remain dynamic.
COPY --from=builder /app/bin/aggregator /app/aggregator

RUN ldd /app/aggregator | tee /tmp/aggregator-ldd.txt && \
    ! grep -q "not found" /tmp/aggregator-ldd.txt && \
    rm /tmp/aggregator-ldd.txt

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
