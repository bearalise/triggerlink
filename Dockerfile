# TriggerLink 平台镜像：单二进制 + SQLite，数据落 /data。
# 构建上下文需要 internal/dashboard/dist（已提交进仓库，go:embed 打进二进制）。
FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/triggerlink ./cmd/triggerlink

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/triggerlink /usr/local/bin/triggerlink
EXPOSE 8288
VOLUME /data
# 密钥必须显式传入（-event-key/-signing-key 或对应环境注入后由命令行引用）
ENTRYPOINT ["triggerlink", "start", "-db", "/data/triggerlink.db"]
