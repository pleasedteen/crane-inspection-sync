# 岸桥巡检单三供应商同步程序（crane_sync）

汇总三家供应商的岸桥巡检单，核对每张单的最新修订。程序**自带三个本地模拟 HTTP
数据源**（一个进程、三个 URL 前缀），分别演示偏移、游标、时间窗三种分页方式，
并完整实现共享网关额度/令牌、游标过期重读、边界去重、单位归一、原子快照发布与
中止续跑。

## 一、环境要求

- Python 3.11+，核心功能**仅使用标准库**，无需安装第三方包。
- 仅当快照选用 Parquet 格式时需要 `pyarrow`（可选）：
  `python3 -m pip install pyarrow`。不安装也能正常使用默认的 JSON Lines。

## 二、一条命令跑自动化测试（80 秒内出结果）

在项目根目录（`crane_sync/` 的上一级）执行：

```bash
python3 run_tests.py
```

实测约 22 秒完成 14 项测试，覆盖：毫米/微米归一、三源端到端发布、共享额度与
令牌只续期一次、游标清单过期重读且旧快照可读、时间窗边界去重、强制中止后续跑、
并发公平性、Parquet（有 pyarrow 时）以及真实子进程的 serve→sync→read 链路。

也可用标准 unittest：

```bash
python3 -m unittest discover -s tests -v
```

## 三、启动、同步、读取快照（三步）

### 1) 启动自带的三个本地模拟数据源

```bash
python3 -m crane_sync serve --config-out mock.json --state-dir ./state --port 8731
```

启动后监听一个端口，暴露：

| 来源 | 接口 | 分页方式 | 鉴权/额度 |
|------|------|----------|-----------|
| 偏移源 | `GET /offset/orders?limit=&offset=` | 偏移 | 共享网关令牌+额度 |
| 游标源 | `GET /cursor/manifest`、`GET /cursor/orders?cursor=` | 清单游标 | 共享网关令牌+额度 |
| 时间窗源 | `GET /window/orders?from=&to=` | 时间窗 | 独立额度，无令牌 |
| 令牌 | `POST /gateway/token` | — | 续期同样占额度 |

`--config-out` 会把连接参数写成 `mock.json`，供下一步使用。该命令常驻，
另开终端执行后续步骤；按 `Ctrl+C` 停止。

### 2) 执行一轮同步并发布快照

```bash
python3 -m crane_sync sync --config mock.json
# 可选：--format parquet（需 pyarrow）、--state-dir 覆盖目录、-v 详细日志
```

每个来源读完整轮清单后**独立发布**一次原子快照，结尾打印各来源快照 ID、格式、
记录数与文件路径。

### 3) 读取已发布快照

```bash
# 跨来源合并：每个巡检单号只保留最高修订，间隙统一毫米
python3 -m crane_sync read --state-dir ./state --merged

# 只读某个来源的最新已发布快照
python3 -m crane_sync read --state-dir ./state --source offset
```

未发布过快照时 `read` 会明确报错并以非零码退出，绝不会读到半成品。

## 四、关键设计与需求对应

### 数据正确性
- **单位归一**：同一页可能混用毫米（`mm`）与微米（`um/μm`），入库前统一换算为
  毫米（1 μm = 0.001 mm），见 `models.py`。
- **只保留最高修订**：供应商共用巡检单号与修订号。
  - 暂存表保留某来源拉到的全部 `(单号, 修订号)` 用于处理与去重；
  - **发布的快照（单源与合并）中，每张巡检单号只保留最高修订**——单源发布用
    `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY revision DESC)` 折叠，
    跨源合并再按单号取最高修订（`storage.read_merged`）。
- **时间窗去重**：相邻时间窗用闭区间，边界记录会被两个窗口重复返回，且同一时刻
  允许多张单；暂存表以 `(source, order_id, revision)` 为主键，重复记录自然合并。

### 三种分页
- **偏移源**：进度就是下一页 `offset`，每页落盘。
- **游标源**：游标绑定清单版次。清单过期后旧游标返回 HTTP 410，客户端丢弃旧
  进度、清空该源旧暂存、从新版清单开头重读；新版清单的记录集合与顺序都可能变化，
  快照按清单行号（`position` 列）保序。**重读期间上一版已发布快照仍可正常读取**。
- **时间窗源**：进度记录最后一个窗口的 `to`，重跑从此处继续。

### 额度、冷却与令牌（`limits.py`）
- 偏移源与游标源**共用**同一网关的滑动窗口额度和同一访问令牌；数据请求与令牌
  续期都消耗额度；续期后旧令牌立即失效。
- 滑动窗口按“来源”轮询发放（round-robin），额度允许时每个来源都能持续获得采集
  机会，一个来源等待响应或冷却时其他来源照常推进。
- 收到 HTTP 429 按 `Retry-After` 进入冷却；冷却门只阻塞对应来源。
- **同一失效令牌只续期一次**：按令牌世代记录是否已续期，并发的两个来源同时 401
  时只触发一次续期，另一个直接复用新令牌。
- 时间窗源使用独立额度，不与网关互相阻塞。

### 快照与可恢复性（`storage.py`）
- 状态存于 `state/sync_state.db`（SQLite，WAL 模式）：分页进度、暂存记录、已发布
  指针与快照表。
- **原子发布**：先把完整快照写到不可见临时文件并 `os.replace` 改名，再在一个 DB
  事务里切换“已发布”指针；维保人员只能读到完整发布后的快照。发布成功后旧快照表
  与旧文件才被删除。
- 快照支持 **JSON Lines（默认，零依赖）** 与 **Parquet（需 pyarrow）**。
- **强制中止可重跑**：每读完一页进度即落盘，重跑时仍有效的分页从已保存进度续读；
  游标若已过期则按上面的 410 规则重读，已发布快照全程可读。

## 五、目录结构

```
crane_sync/
  models.py       数据模型与 mm/μm 归一
  limits.py       滑动窗口额度、冷却门、共享网关令牌（401/429/410 处理）
  http.py         标准库 JSON HTTP 客户端（线程池非阻塞）
  mock_server.py  三个自带本地模拟 HTTP 数据源 + 网关额度/令牌/游标轮换
  fetchers.py     偏移/游标/时间窗三个采集器（分页、进度、发布）
  storage.py      SQLite 进度/暂存/原子发布，JSONL/Parquet 快照，合并读取
  engine.py       异步并发编排三个来源
  cli.py          serve / sync / read 命令行
tests/
  test_sync.py    14 项自动化测试（含真实子进程链路）
run_tests.py      一键测试入口
config.example.json
requirements.txt
```

## 六、模拟源的测试辅助接口（仅 mock 提供）

- `POST /mock/admin/expire-token`：强制当前令牌立即失效，用于验证 401 续期。
- `POST /mock/admin/rotate-cursor`：把游标清单轮换到新版，用于验证 410 重读。
- `GET  /mock/admin/stats`：查看网关请求数、续期次数、429 次数等。
- 启动参数 `--cursor-rotate-after N`：第 N 次游标调用后自动轮换清单。
