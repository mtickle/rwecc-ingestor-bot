FROM golang:1.27-alpine AS builder

WORKDIR /build

COPY . .

RUN go mod download
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o rwecc-ingestor main.go


FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/rwecc-ingestor /rwecc-ingestor

CMD ["/rwecc-ingestor"]