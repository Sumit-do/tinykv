# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/tinykv ./cmd/tinykv

FROM alpine:3.20
RUN addgroup -S tinykv && adduser -S -G tinykv -H tinykv \
    && mkdir -p /data && chown tinykv:tinykv /data
COPY --from=build /out/tinykv /usr/local/bin/tinykv
USER tinykv
VOLUME ["/data"]
EXPOSE 7700
HEALTHCHECK --interval=10s --timeout=2s --retries=3 \
    CMD wget -qO- http://127.0.0.1:7700/healthz >/dev/null || exit 1
ENTRYPOINT ["tinykv"]
CMD ["serve", "--dir", "/data", "--addr", ":7700", "--fsync", "always"]
