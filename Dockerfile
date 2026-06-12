FROM golang:1.24-alpine AS builder

WORKDIR /src

# Download deps first for layer caching.
COPY go.mod go.sum ./
RUN GOTOOLCHAIN=auto go mod download

COPY . .
RUN GOTOOLCHAIN=auto go build -o /sgpd ./cmd/sgpd

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /sgpd /sgpd
ENTRYPOINT ["/sgpd"]
