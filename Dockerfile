FROM golang:1.25.6 AS benzhi-build
ARG TARGETOS=linux
ARG TARGETARCH
ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}
ENV GOTOOLCHAIN=local
WORKDIR /src
COPY go.mod go.sum ./
RUN mkdir -p /go/pkg/mod && go mod download
COPY . .
RUN go build ./...
RUN mkdir -p /out && CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -trimpath -o /out/benzhi-app ./cmd/server

FROM golang:1.25.6 AS benzhi-runtime
LABEL io.benzhi.delivery-template="backend-v2"
ENV GOTOOLCHAIN=local
WORKDIR /app
COPY --from=benzhi-build /go/pkg/mod /go/pkg/mod
COPY --from=benzhi-build /src /app
COPY --from=benzhi-build /out/benzhi-app /usr/local/bin/benzhi-app
CMD ["/usr/local/bin/benzhi-app"]
