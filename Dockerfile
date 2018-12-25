FROM golang:1.11-alpine as builder

RUN apk add --no-cache gcc git linux-headers make musl-dev

ADD . /gyee
RUN cd /gyee/gyee && go build .

# Pull gyee from builder to deploy container
FROM alpine:latest

RUN apk add --no-cache ca-certificates
COPY --from=builder /gyee/gyee/gyee /usr/local/bin

ENTRYPOINT ["gyee"]
