FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install necessary build tools
RUN apk add --no-cache curl git make

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build
RUN make build

FROM quay.io/prometheus/busybox:latest

COPY --from=builder /app/stackdriver_exporter /bin/

USER       nobody
ENTRYPOINT ["/bin/stackdriver_exporter"]
EXPOSE     9255
