#!/bin/sh
# verify 入口：构建检查 → 代码测试 → 因果场景 HTTP 冒烟 → 崩溃恢复冒烟。
# 任一步骤失败即非零退出；全部通过则验收通过。
set -eu

# 服务以纯静态二进制构建，检查保持一致，避免依赖 C 工具链。
export CGO_ENABLED=0

echo "[verify] 构建检查: go build ./..."
go build -buildvcs=false ./...

echo "[verify] 静态检查: go vet ./..."
go vet -buildvcs=false ./...

echo "[verify] 代码测试: go test ./..."
go test -buildvcs=false ./...

echo "[verify] HTTP 冒烟: APP_URL=${APP_URL:-http://localhost:8080}"
verify-smoke

echo "[verify] 崩溃恢复冒烟: APP_BIN=${APP_BIN:-/usr/local/bin/app} RESTART_DATA_DIR=${RESTART_DATA_DIR:-/tmp/interlock-restart-data}"
# 在可配置数据目录上启动应用，SIGKILL 模拟进程退出/容器重建，
# 重启后经 HTTP 重放等待项、前沿、裁决与幂等场景，并验证损坏尾部回退。
verify-smoke -restart

echo "[verify] 全部验收通过"
