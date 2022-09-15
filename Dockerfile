
FROM ghcr.io/drogue-iot/builder:0.2.3 as builder

RUN mkdir /build
ADD . /build
WORKDIR /build

RUN cargo build --release

FROM registry.access.redhat.com/ubi9-minimal:latest

LABEL org.opencontainers.image.source="https://github.com/drogue-iot/twin-operator"

COPY --from=builder /build/target/release/twin-operator /

ENTRYPOINT [ "/twin-operator" ]
