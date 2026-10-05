# sen-privacy-job-registry

把多方隐私计算任务的参与方、计算类型、输入数据引用、阶段与结果引用记录成可查询的服务，支持按参与方与阶段查询任务并追溯完整的任务阶段。

## 运行要求

- Go 1.26 或以上
- SQLite（本服务自带存储，不需要外部数据库）

## 构建、测试与启动

```bash
go build ./...
go test ./...
go run .
```

服务默认监听 `127.0.0.1:8080`。可用环境变量覆盖：

| 变量 | 默认值 | 用途 |
|---|---|---|
| `ADDR` | `127.0.0.1:8080` | HTTP 监听地址 |
| `DB_PATH` | `sen-privacy-job-registry.db` | SQLite 数据库文件路径 |

## 已公开的入口

### `GET /healthz`

返回服务与存储状态。正常时 HTTP 200：

```json
{"status":"ok","database":"ok"}
```

存储不可用时 HTTP 503：

```json
{"error":{"code":"storage_unavailable","message":"database is not available"}}
```

### `POST /jobs`

登记单个计算任务。请求体必须是恰好包含以下四个字段的单个 JSON 对象：

| 字段 | 类型 | 要求 |
|---|---|---|
| `id` | 字符串 | 非空白，任务编号 |
| `computation_type` | 字符串 | 非空白，不限定枚举 |
| `participants` | 字符串数组 | 至少一个元素，每个元素非空白，顺序按原样保存 |
| `input_refs` | 字符串数组 | 至少一个元素，每个元素非空白，顺序按原样保存 |

服务只记录引用文本，不读取引用指向的数据，也不接受原始数据或客户端指定的阶段。

首次登记返回 HTTP 201：

```json
{"id":"job-1","computation_type":"mpc","participants":["alice","bob"],"input_refs":["r1","r2"],"stage":"registered","history":[{"stage":"registered"}]}
```

用相同编号再次登记时：

- 四个字段的原值与数组顺序完全一致：HTTP 200，返回已有记录，不追加历史；
- 任一字段不同（含数组顺序不同）：HTTP 409，`job_conflict`，已有内容不变。

### `GET /jobs`

分页查询任务，HTTP 200：

```json
{"items":[/* 与登记响应一致的记录 */],"total":1,"page":1,"page_size":20}
```

查询参数：

| 参数 | 默认 | 规则 |
|---|---|---|
| `page` | `1` | 十进制正整数 |
| `page_size` | `20` | 1 至 100 的十进制整数 |
| `participant` | 无 | 可选，按参与方精确匹配筛选；空白值视为非法请求 |

结果按编号的 UTF-8 字节升序排列，`total` 为筛选后的总数，越界页返回空 `items`。

## 错误约定

所有错误响应都是单个顶层 `error` 对象，包含 `code` 与 `message` 两个字符串字段；`message` 不包含 SQL、堆栈或文件路径。

| HTTP | `code` | 场景 |
|---|---|---|
| 400 | `invalid_request` | 缺字段、类型不符、空白字符串、空数组、额外字段、请求体不是单个 JSON 对象、分页参数非法 |
| 409 | `job_conflict` | 相同编号以不同字段或不同数组顺序重复登记 |
| 503 | `storage_unavailable` | 存储不可用（`message` 与 `GET /healthz` 一致） |
| 404 | `route_not_found` | 未知路由 |
