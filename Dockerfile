FROM golang:1.12-alpine as builder

RUN apk add --no-cache gcc git linux-headers make musl-dev

ADD . /gyee
RUN cd /gyee && make all

# Pull gyee from builder to deploy container
FROM alpine:latest

RUN apk add --no-cache ca-certificates
COPY --from=builder /gyee/build/bin/* /usr/local/bin/

CMD ["gyee"]
