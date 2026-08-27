基于 Go 实现的数字档案长期保存 Web 项目，一款后端服务，实现多副本完整性核验、损坏隔离、可信修复与恢复裁定。

# archival-replica-integrity-recovery

本 Git 项目来自模型完成任务后的 workspace，不包含嵌套 .git 记录或本地构建产物。

## 本地构建与测试

```bash
go mod download
go build ./...
go test ./...
./run_benzhi_smoke.sh
```

## Docker 构建与运行

```bash
docker build --platform linux/amd64 -t archival-replica-integrity-recovery:latest .
./build_benzhi_docker.sh archival-replica-integrity-recovery linux/arm64
docker run --rm -it --platform linux/arm64 archival-replica-integrity-recovery:latest
./build_benzhi_docker.sh archival-replica-integrity-recovery linux/amd64
docker run --rm -it --platform linux/amd64 archival-replica-integrity-recovery:latest
```

构建脚本第二个参数为目标平台，必须分别完成 linux/arm64 和 linux/amd64 构建与容器验证；未提供时按照规范默认使用 linux/amd64。系统 backend-v2 模板通过 Go 原生交叉编译生成目标架构的 /usr/local/bin/benzhi-app，镜像默认直接运行该入口。
