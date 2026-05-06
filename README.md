# PangFlow v0.2.6

> **算法工程师视角的零摩擦工作流编排框架**

PangFlow 是一个面向算法工程师的 OPS 工作流管理工具。它让工程师只写纯 Python 函数，用 `@node` 标记、用 `>>` 连接，即可自动编译为 Prefect 工作流和 HTTP 服务。工程师无需了解 Prefect API，也无需编写部署脚本。

---

## 核心特性

- **算法侧极简**：只写纯函数，`@node` 标记，`>>` 连接编排，零 Prefect API 接触
- **Prefect 侧透明**：自动生成最优 Prefect Flow，工程师不感知 Prefect 存在
- **配置驱动**：TOML 声明所有 OPS 行为，CLI 仅触发与查询
- **默认 SQLite，保留扩展**：元数据、日志、模型、特征默认 SQLite，通过 StorageBackend 接口支持 PostgreSQL / S3 / MongoDB
- **Conda 即环境**：仅 Conda 管理 Python 依赖，不引入 Docker / K8s
- **HTTP 即服务**：仅通过 HTTP 暴露推理/优化服务，FastAPI 驱动
- **监控零成本**：节点级日志、监控、血缘自动采集，默认接入 Prefect UI
- **分层解耦**：五层架构（编排层、配置层、任务层、执行层、观察者层）+ 三子系统（环境、存储、服务）
- **WebUI 仪表盘**：TypeScript + Tailwind CSS 构建的现代化 Web 界面

---

## 安装

PangFlow 要求 **Python ≥ 3.13.5**。

```bash
# 使用 uv（推荐）
uv pip install pangflow==0.2.6

# 或使用 pip
pip install pangflow==0.2.6
```

CLI 入口：`pangflowctl`

---

## 快速开始

### 1. 初始化工作空间

```bash
pangflowctl init my_project
cd my_project
```

目录结构：
```
my_project/
├── pangflow.toml
├── pangflow.db
├── workflows/
├── logs/
├── data/
├── temp/
├── models/
└── features/
```

### 2. 编写算法节点

`nodes.py`:

```python
import pangflow as pf
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor

@pf.node(name="load-data", log=True)
def load_data(source: str) -> pd.DataFrame:
    """加载历史用电数据"""
    pf.log(f"从 {source} 加载数据", level="INFO")
    return pd.read_csv(source)

@pf.node(name="train-model", log=True, artifact="model", stage="development")
def train_model(data: pd.DataFrame) -> GradientBoostingRegressor:
    """训练预测模型"""
    pf.log("开始训练模型", level="INFO")
    model = GradientBoostingRegressor(n_estimators=100)
    model.fit(data.drop("target", axis=1), data["target"])
    pf.log_metric("train_score", model.score(data.drop("target", axis=1), data["target"]))
    return model

@pf.node(name="generate-forecast", log=True, feature="forecast")
def generate_forecast(model: GradientBoostingRegressor, data: pd.DataFrame) -> pd.DataFrame:
    """生成未来24小时预测"""
    pf.log("生成预测", level="INFO")
    forecast = model.predict(data)
    return pd.DataFrame({"timestamp": pd.date_range("now", periods=24, freq="H"), "value": forecast})
```

### 3. 编排工作流

`workflow.py`:

```python
import pangflow as pf
from .nodes import load_data, train_model, generate_forecast

@pf.workflow(name="energy-forecast", schedule="0 * * * *")
def main_workflow():
    # >> 定义数据流
    data = load_data(source="data/history.csv")
    model = train_model(data)
    forecast = generate_forecast(model, data)
    return forecast
```

### 4. 暴露 HTTP 服务

`service.py`:

```python
import pangflow as pf
from pydantic import BaseModel

class PredictRequest(BaseModel):
    features: list[float]

class PredictResponse(BaseModel):
    forecast: list[float]
    trace_id: str

@pf.serve(endpoint="/api/v1/predict", method="POST")
def predict_service(request: PredictRequest) -> PredictResponse:
    """实时预测服务"""
    model = pf.load_model("energy-forecast", stage="production")
    forecast = model.predict([request.features])
    return PredictResponse(forecast=forecast[0], trace_id=pf.get_trace_id())
```

### 5. TOML 配置

`workflow.toml`:

```toml
[workflow]
name = "energy-forecast"
version = "1.0.0"
description = "户用储能需求预测工作流"

[workflow.schedule]
type = "cron"
expression = "0 * * * *"

[workflow.env]
name = "energy-forecast"
python = "3.13"

[workflow.env.conda]
channels = ["conda-forge"]
dependencies = ["pandas", "scikit-learn"]

[workflow.env.pip]
dependencies = ["requests"]

[workflow.storage]
default_backend = "local"

[workflow.storage.local]
base_path = "~/.pangflow/data"

[workflow.storage.models]
versioning = "semantic"
stage_policy = "manual"

[workflow.storage.features]
ttl = "24h"
partition_by = "date"
format = "parquet"

[workflow.log]
level = "INFO"
backend = "sqlite"
format = "structured"

[workflow.serve]
enabled = true
port = 8000
host = "127.0.0.1"

[[workflow.serve.endpoints]]
name = "predict"
path = "/api/v1/predict"
method = "POST"
handler = "predict_service"

[workflow.nodes.train-model]
timeout = 300
retries = 2
log_level = "INFO"
```

### 6. Prefect 环境预配置

PangFlow 编排后端基于 Prefect，部署前需先启动 Prefect Server 和 Worker。

**terminal-1（Prefect Server）:**
```bash
# 指定 Prefect API 地址
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

# 启动 Prefect Server
prefect server start
```

**terminal-2（Prefect Worker）:**
```bash
# 创建 process work pool
prefect work-pool create --type process default-process

# 启动 process worker
prefect worker start -p "default-process"
```

浏览器访问 `http://127.0.0.1:4200` 确认 Prefect UI 正常。

### 7. CLI 完整流程

```bash
# 工作流管理
pangflowctl workflow list                      # 列出所有工作流
pangflowctl workflow get energy-forecast       # 查看工作流详情
pangflowctl workflow delete energy-forecast    # 删除工作流

# 环境管理（从 TOML 读取配置，独立 conda 环境）
pangflowctl env create --workflow energy-forecast
pangflowctl env list

# 注册与部署
pangflowctl register workflow.toml
pangflowctl deploy energy-forecast --cron "0 * * * *"

# Prefect serve 管理（部署后自动启动）
pangflowctl deployment status                  # 查看所有部署状态
pangflowctl deployment stop energy-forecast    # 停止 serve
pangflowctl deployment serve energy-forecast   # 手动启动 serve

# HTTP 推理服务
pangflowctl serve start energy-forecast
pangflowctl serve stop energy-forecast

# 触发与查询
pangflowctl trigger energy-forecast --wait
pangflowctl logs energy-forecast --node train-model --limit 50
pangflowctl metrics energy-forecast
pangflowctl lineage energy-forecast
pangflowctl model list
pangflowctl model promote v1.0.1 --to production
pangflowctl status
```

### 7. WebUI

启动服务后，打开浏览器访问：

```
http://localhost:8000/ui
```

WebUI 提供：
- **Dashboard**：工作流状态概览、最近执行、系统健康
- **Workflows**：工作流列表与 DAG 可视化
- **Executions**：执行历史与日志查看
- **Models**：模型版本管理与晋级/回滚
- **Lineage**：数据血缘关系图
- **Settings**：环境与存储配置

---

## 架构概览

PangFlow v0.2.6 采用 **五层 + 三子系统** 架构：

```
┌─────────────────────────────────────────┐
│  算法工程师视角                            │
│  @node / @workflow / @serve / >>        │
├─────────────────────────────────────────┤
│  编排层 (Orchestration Layer)            │
│  NodeRegistry → DAGBuilder → FlowCompiler│
│                               ServeCompiler│
├─────────────────────────────────────────┤
│  核心处理层                               │
│  配置层 (Config Layer)                    │
│  任务层 (Task Layer)                      │
│  观察者层 (Observer Layer)                │
├─────────────────────────────────────────┤
│  执行层 (Execution Layer)                 │
│  LocalStrategy / CondaStrategy            │
│  HTTPServiceStrategy / StorageStrategy    │
├─────────────────────────────────────────┤
│  子系统                                   │
│  环境子系统 (EnvSubsystem)                │
│  存储子系统 (StorageSubsystem)            │
│  服务子系统 (ServeSubsystem)              │
└─────────────────────────────────────────┘
```

---

## CLI 命令参考

| 命令 | 说明 |
|------|------|
| `pangflowctl init [path]` | 初始化工作空间 |
| `pangflowctl workflow list` | 列出所有工作流 |
| `pangflowctl workflow get <name>` | 查看工作流详情 |
| `pangflowctl workflow delete <name>` | 删除工作流 |
| `pangflowctl env create --workflow <name>` | 创建 Conda 环境（读取 TOML） |
| `pangflowctl env update --workflow <name>` | 更新 Conda 依赖 |
| `pangflowctl env remove --workflow <name>` | 删除 Conda 环境 |
| `pangflowctl env list` | 列出所有环境 |
| `pangflowctl register <workflow.toml>` | 解析 TOML 并注册工作流 |
| `pangflowctl deploy <workflow> [--cron]` | 编译并部署到 Prefect（自动启动 serve） |
| `pangflowctl deployment serve <workflow>` | 启动 Prefect serve |
| `pangflowctl deployment stop <workflow>` | 停止 Prefect serve |
| `pangflowctl deployment status` | 查看 Prefect 部署状态 |
| `pangflowctl serve start <workflow>` | 启动 HTTP 推理服务 |
| `pangflowctl serve stop <workflow>` | 停止 HTTP 推理服务 |
| `pangflowctl serve status` | 查看 HTTP 服务状态 |
| `pangflowctl trigger <workflow> [--wait]` | 手动触发工作流 |
| `pangflowctl logs <workflow> [--node]` | 查询节点日志 |
| `pangflowctl metrics <workflow>` | 查询业务指标 |
| `pangflowctl lineage <workflow>` | 查询数据血缘 |
| `pangflowctl model list` | 列出模型版本 |
| `pangflowctl model promote <ver> --to <stage>` | 晋级模型阶段 |
| `pangflowctl model rollback <ver>` | 回滚模型版本 |
| `pangflowctl status` | 查看系统状态 |

---

## 技术栈

- **Python**: 3.13.5+
- **工作流引擎**: Prefect 3.6.25
- **Web 框架**: FastAPI 0.135.3
- **CLI**: Typer + Rich
- **数据库**: SQLAlchemy + SQLite（默认）
- **配置**: TOML + Pydantic
- **前端**: TypeScript 6.0.2 + Tailwind CSS + Vite
- **打包**: uv + pyproject.toml + build.py

---

## 开发指南

```bash
# 克隆仓库
git clone https://github.com/shihua/pangflow.git
cd pangflow

# 安装开发依赖
uv pip install -e ".[dev]"

# 构建前端
cd webui
npm install
npm run build
cd ..
python build.py

# 运行测试
pytest tests/
```

---

## 许可证

Apache-2.0 © 2026 The PangFlow Authors
