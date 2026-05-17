# BusyPipe 使用说明

BusyPipe 是本仓库新增的 GOST transport，用来把原本外置的 BusyPipe 包装层直接下沉到 GOST 的连接层。它对上层仍然表现为普通 `net.Conn`，因此可以和现有的 `relay`、`http`、转发链、认证、TLS、MPTCP 等 GOST 能力组合使用。

## Transport 类型

当前提供两个 transport：

- `bp`：TCP + BusyPipe，适合本地调试或不需要 TLS 的测试环境。
- `bptls`：TCP + TLS + BusyPipe，推荐用于公网部署。

常见组合是：

- 服务端：`relay+bptls`
- 客户端转发链：`connector: relay` + `dialer: bptls`

`bptls` 的顺序是 TCP -> TLS -> BusyPipe。也就是说，BusyPipe 帧运行在 TLS 里面，公网侧看到的是持续 TLS 流。

## 快速启动

以下示例假设：

- 服务端公网地址：`server.example.com`
- 服务端监听端口：`33317`
- 客户端本地 HTTP 代理端口：`127.0.0.1:18080`
- 服务端证书：`/etc/letsencrypt/live/server.example.com/fullchain.pem`
- 服务端私钥：`/etc/letsencrypt/live/server.example.com/privkey.pem`

### 服务端

保存为 `server.yaml`：

```yaml
services:
- name: relay-bptls-server
  addr: :33317
  handler:
    type: relay
  listener:
    type: bptls
    tls:
      certFile: /etc/letsencrypt/live/server.example.com/fullchain.pem
      keyFile: /etc/letsencrypt/live/server.example.com/privkey.pem
    metadata:
      mptcp: true
      bp.minBps: 8000
```

启动：

```bash
./gost-bp -C server.yaml
```

### 客户端

保存为 `client.yaml`：

```yaml
services:
- name: proxy
  addr: 127.0.0.1:18080
  handler:
    type: http
    chain: bptls-chain
  listener:
    type: tcp

chains:
- name: bptls-chain
  hops:
  - name: hop-0
    nodes:
    - name: relay-node
      addr: server.example.com:33317
      connector:
        type: relay
      dialer:
        type: bptls
        tls:
          secure: true
          serverName: server.example.com
        metadata:
          bp.minBps: 8000
```

启动：

```bash
./gost-bp -C client.yaml
```

测试：

```bash
curl -x http://127.0.0.1:18080 http://example.com/
```

如果使用自签证书测试，把客户端 `tls.secure` 临时设为 `false`：

```yaml
dialer:
  type: bptls
  tls:
    secure: false
```

## URL 命令行写法

简单场景也可以直接使用 `-L` 和 `-F`：

```bash
# server
./gost-bp -L "relay+bptls://:33317?mptcp=true&certFile=/path/fullchain.pem&keyFile=/path/privkey.pem&bp.minBps=8000"

# client
./gost-bp -L "http://127.0.0.1:18080" -F "relay+bptls://server.example.com:33317?secure=true&serverName=server.example.com&bp.minBps=8000"
```

复杂配置建议使用 YAML，尤其是 TLS、认证、多级链路或多服务部署。

## 参数

BusyPipe 参数放在 listener 或 dialer 的 `metadata` 中。服务端和客户端都会发送自己的配置，握手后取双方更保守的组合：最低速率、tick 和 jitter 取较大值，最大帧和空闲超时取较小值。

| 参数 | 默认值 | 说明 |
| --- | ---: | --- |
| `bp.minBps` | `8000` | 每条 BusyPipe 连接维持的最低发送速率，单位 bit/s。 |
| `bp.tickMs` | `250` | padding 调度周期，单位 ms。 |
| `bp.maxFrameSize` | `1400` | BusyPipe 单帧最大大小，含 16 字节帧头。 |
| `bp.idleTimeoutMs` | `15000` | 底层连接无任何 BusyPipe 帧时的空闲超时。 |
| `bp.minJitterBytes` | `8` | MIXED payload 内真实数据偏移的最小抖动距离。 |
| `bp.warmupMs` | `3000` | 握手完成后仅发送随机 `PAD` 的窗口时长，单位 ms，`0` 表示禁用。 |

也兼容不带 `bp.` 前缀的写法，例如 `minBps`、`tickMs`，但新配置建议统一使用 `bp.` 前缀。

## MPTCP

服务端 listener 支持：

```yaml
metadata:
  mptcp: true
```

这会在监听 socket 上启用 Go 的 `net.ListenConfig.SetMultipathTCP(true)`。客户端侧是否使用 MPTCP 取决于系统内核、路由、GOST 底层 dialer 和对端支持情况。

Linux 上可用以下命令观察：

```bash
ss -tin | grep -A2 -B1 ':33317'
```

如果输出中出现 `olia` 等 MPTCP 拥塞控制信息，说明该连接正在走 MPTCP 栈。

## 生产部署建议

- 公网部署优先使用 `bptls`，不要裸用 `bp`。
- `bp.minBps` 不宜设置过高，否则空闲 padding 会消耗带宽，并可能与真实业务流量竞争。
- 推荐先从 `8000` 开始，确认稳定后再根据链路特征调整。
- 服务端使用正式证书时，客户端应设置 `tls.secure: true` 和正确的 `serverName`。
- 测试端口验证完后，及时停止临时服务，避免留下未受控的 relay 入口。

## 排障

确认二进制包含 BusyPipe transport：

```bash
./gost-bp -V
```

确认服务监听：

```bash
ss -tln | grep ':33317'
```

确认客户端代理可用：

```bash
curl -v -x http://127.0.0.1:18080 http://example.com/
```

查看日志时重点确认：

- 服务端日志中出现 `listener":"bptls"` 和 `listening on ...:33317/tcp`。
- 客户端日志中出现 `handler":"http"` 和对目标地址的转发记录。
- 服务端日志中出现 `handler":"relay"` 和对应客户端来源地址。

如果连接建立后很快断开，优先检查：

- 两端是否都使用 `bptls`，不要一端 `tls` 一端 `bptls`。
- TLS 证书路径、权限和 `serverName` 是否正确。
- `bp.maxFrameSize` 是否被设置得过小。
- 防火墙或安全组是否放行监听端口。
