#!/bin/sh
# verify 入口：构建检查 → 代码测试 → 因果场景 HTTP 冒烟。
# 任一步骤失败即非零退出；全部通过则冒烟程序的退出码决定验收结果。
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
exec verify-smoke
