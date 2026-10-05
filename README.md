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

登记一个任务。请求体是单个 JSON 对象，恰好包含四个必填字段：`id`、`computation_type`（均为非空白字符串），以及 `participants`、`input_refs`（均为至少含一个非空白字符串的数组，元素顺序原样保存）。`input_refs` 只记录引用文本，服务不读取引用指向的数据。计算类型不限定枚举，不接受客户端指定阶段。

首次登记返回 HTTP 201，记录附带值为 `registered` 的 `stage` 和仅含初始阶段的 `history`：

```json
{
  "id": "job-1",
  "computation_type": "mpc-sum",
  "participants": ["alice", "bob"],
  "input_refs": ["s3://inputs/a"],
  "stage": "registered",
  "history": [{"stage": "registered"}]
}
```

以相同编号再次登记时，按四个字段的原值与数组顺序比较：完全相同返回 HTTP 200 与已有记录，不追加历史；任一不同返回 HTTP 409：

```json
{"error":{"code":"job_conflict","message":"a different job with this id is already registered"}}
```

登记具备单写者事务语义：记录与初始历史一起成功或一起失败，重叠请求也只留下单条记录，失败不会覆盖已有内容。

### `GET /jobs`

分页查询已登记任务，HTTP 200：

```json
{
  "items": [/* 与登记响应一致的记录 */],
  "total": 3,
  "page": 1,
  "page_size": 20
}
```

查询参数：

| 参数 | 默认值 | 约束 |
|---|---|---|
| `page` | `1` | 十进制正整数 |
| `page_size` | `20` | 1 至 100 的十进制整数 |
| `participant` | 无 | 精确筛选参与方，空白值报错 |

结果按编号的 UTF-8 字节升序排列，`total` 为筛选后的总数，越界页返回空数组。非法参数返回 HTTP 400 与 `invalid_request`。

## 错误约定

所有错误响应都是单个顶层 `error` 对象，包含 `code` 与 `message` 两个字符串字段；`message` 不包含 SQL、堆栈或文件路径。
