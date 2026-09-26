# syntax=docker/dockerfile:1
# 同步辐射真空阀联锁 · 因果一致性服务
# 目标：app（运行时镜像）/ verify（验收镜像，执行后退出并以退出码报告结果）

# ---- 构建阶段：编译全部组件 ----
FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -buildvcs=false -o /out/app ./cmd/app \
 && CGO_ENABLED=0 go build -buildvcs=false -o /out/verify ./cmd/verify

# ---- 运行镜像：静态二进制，健康入口 /healthz ----
# 状态经 WAL 持久化到 DATA_DIR（默认 /data，可挂卷）；重启后自动恢复，
# 截断或损坏的日志尾部回退到最后一致状态。
FROM scratch AS app
COPY --from=build /out/app /app
ENV LISTEN_ADDR=:8080
ENV DATA_DIR=/data
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["/app"]

# ---- 验收镜像：构建检查 + 代码测试 + 因果场景 HTTP 冒烟 + 崩溃恢复冒烟 ----
FROM golang:1.23-bookworm AS verify
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
COPY scripts/verify-entry.sh /usr/local/bin/verify-entry
# 恢复冒烟在同一数据目录上反复 SIGKILL/重启 app 子进程。
COPY --from=build /out/app /usr/local/bin/app
RUN chmod +x /usr/local/bin/verify-entry \
 && go build -buildvcs=false -o /usr/local/bin/verify-smoke ./cmd/verify
ENV APP_URL=http://app:8080
ENV APP_BIN=/usr/local/bin/app
ENTRYPOINT ["verify-entry"]
