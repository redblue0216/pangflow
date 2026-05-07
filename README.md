# PangFlow v0.2.8

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
- **WebUI 仪表盘**：TypeScript + Tailwind CSS 构建的现代化 Web 界面，支持 Executions DAG 节点状态可视化
- **模型工件闭环**：`pf.save_model()` / `pf.load_model()` 支持模型注册、晋级、回滚

---

## 安装

PangFlow 要求 **Python ≥ 3.13.5**。

```bash
# 使用 uv（推荐）
uv pip install pangflow==0.2.7

# 或使用 pip
pip install pangflow==0.2.7
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
├── pangflow.toml              # 工作空间配置
├── pangflow.db                # SQLite 数据库
├── workflows/                 # 工作流 TOML 目录
├── logs/
├── data/                      # 数据文件
├── temp/
├── models/                    # 模型工件
├── features/
├── nodes.py                   # 标准示例：算法节点
├── workflow.py                # 标准示例：工作流编排
├── service.py                 # 标准示例：HTTP 推理服务
├── workflow.toml              # 标准示例：TOML 配置
├── sklearn_nodes.py           # sklearn 示例：算法节点
├── sklearn_workflow.py        # sklearn 示例：工作流编排
├── sklearn_service.py         # sklearn 示例：HTTP 推理服务
└── sklearn_workflow.toml      # sklearn 示例：TOML 配置
```

> **v0.2.7 改进**：`init` 自动生成两套完全独立的示例（标准 + sklearn），各配独立 TOML，无需 `cp` 覆盖。

---

## 场景一：标准 Workflow（零外部依赖）

标准示例仅使用 Python 标准库，无需 conda 额外依赖。

### 1.1 查看示例代码

**`nodes.py`** — 定义算法节点，调用 `pf.save_model()` 保存模型工件：

```python
import pangflow as pf

@pf.node(name="load-data", log=True)
def load_data(n: int = 5) -> list:
    """生成模拟数据"""
    pf.log(f"生成 {n} 个数据点", level="INFO")
    data = list(range(1, n + 1))
    pf.log_metric("data_count", float(n))
    return data

@pf.node(name="process-data", log=True)
def process_data(data: list) -> list:
    """对数据做平方处理"""
    pf.log(f"处理数据: {data}", level="INFO")
    result = [x ** 2 for x in data]
    pf.log_metric("sum_squares", float(sum(result)))
    return result

@pf.node(name="save-result", log=True, artifact="demo_result")
def save_result(processed: list, label: str = "demo") -> dict:
    """保存处理结果为制品，并注册到模型仓库"""
    pf.log(f"保存结果: label={label}, values={processed}", level="INFO")
    result = {"label": label, "values": processed, "sum": sum(processed)}
    # 显式保存为模型工件，支持后续的晋级/回滚/推理
    pf.save_model("demo_result", result, metadata={"label": label})
    pf.log("模型工件已保存: demo_result", level="INFO")
    return result
```

**`workflow.py`** — 编排 DAG，含 `if __name__ == "__main__"` 入口：

```python
import os
import pangflow as pf
from nodes import load_data, process_data, save_result

@pf.workflow(name="demo-workflow", schedule="0 * * * *")
def main_workflow():
    raw = load_data(n=5)
    processed = process_data(raw)
    result = save_result(processed, label="demo-run")
    return result

if __name__ == "__main__":
    import uuid
    os.environ.setdefault("PANGFLOW_RUN_ID", str(uuid.uuid4()))
    compiled = main_workflow()
    result = compiled()
    print(f"Workflow result: {result}")
```

**`service.py`** — 推理服务，调用 `pf.load_model()` 加载工件：

```python
from pydantic import BaseModel
import pangflow as pf

class PredictRequest(BaseModel):
    index: int

class PredictResponse(BaseModel):
    value: int
    label: str
    trace_id: str

@pf.serve(endpoint="/api/v1/predict", method="POST")
def demo_predict_service(request: PredictRequest) -> PredictResponse:
    """基于已保存结果的推理服务"""
    artifact = pf.load_model("demo_result")
    values = artifact["values"]
    label = artifact.get("label", "unknown")
    idx = request.index % len(values)
    return PredictResponse(
        value=values[idx],
        label=label,
        trace_id=pf.get_trace_id(),
    )
```

**`workflow.toml`** — 仅含标准示例配置：

```toml
[workflow]
name = "demo-workflow"
version = "1.0.0"
description = "PangFlow v0.2.8 标准演示工作流 —— 纯 Python 标准库"

[workflow.schedule]
type = "cron"
expression = "0 * * * *"

[workflow.env]
name = "demo-workflow"
python = "3.13"

[workflow.env.conda]
channels = ["conda-forge"]
dependencies = []

[workflow.env.pip]
dependencies = []

[workflow.storage]
default_backend = "local"

[workflow.storage.local]
base_path = "~/.pangflow/data"

[workflow.log]
level = "INFO"
backend = "sqlite"
format = "structured"

[workflow.serve]
enabled = true
port = 8000
host = "127.0.0.1"

[[workflow.serve.endpoints]]
name = "demo_predict"
path = "/api/v1/predict"
method = "POST"
handler = "demo_predict_service"

[workflow.nodes.process-data]
timeout = 60
retries = 1
log_level = "INFO"
```

### 1.2 注册与触发

> **v0.2.7 改进**：`register` 自动推断 `command = "python {stem}.py"`，无需手动填写。

```bash
# 注册（command 自动推断为 python workflow.py）
pangflowctl register workflow.toml

# 触发工作流
pangflowctl trigger demo-workflow
```

输出示例：
```
Triggering demo-workflow  Run ID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
Workflow completed successfully
Workflow result: {'label': 'demo-run', 'values': [1, 4, 9, 16, 25], 'sum': 55}
```

### 1.3 模型工件晋级与回滚

```bash
# 查看模型工件
pangflowctl model list
# 输出：demo_result | 1.0.0 | development

# 晋级到 production
ARTIFACT_ID=$(sqlite3 pangflow.db "SELECT artifact_id FROM artifacts WHERE name='demo_result';")
pangflowctl model promote "$ARTIFACT_ID" --to production

# 查看晋级历史
sqlite3 pangflow.db "SELECT stage, promotion_note FROM artifact_versions;"

# 回滚到上一阶段
pangflowctl model rollback "$ARTIFACT_ID"
```

### 1.4 启动推理服务

```bash
pangflowctl serve start demo-workflow --port 8000
```

测试：
```bash
curl -X POST http://127.0.0.1:8000/api/v1/predict \
  -H "Content-Type: application/json" \
  -d '{"index": 2}'
# {"value": 9, "label": "demo-run", "trace_id": "t-xxxxxx"}
```

---

## 场景二：sklearn Iris Workflow（独立 conda 环境）

sklearn 示例演示在独立 conda 环境中运行 sklearn 算法，与标准示例完全隔离。

### 2.1 创建 sklearn 算法环境

```bash
pangflowctl env create --workflow sklearn-iris-workflow --file sklearn_workflow.toml
```

### 2.2 查看示例代码

**`sklearn_nodes.py`** — 训练并保存 Iris 分类器：

```python
import pangflow as pf
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

@pf.node(name="sklearn-load-data", log=True)
def load_iris_data() -> dict:
    """加载 sklearn Iris 数据集"""
    pf.log("加载 Iris 数据集", level="INFO")
    iris = load_iris()
    data = {
        "X": iris.data.tolist(),
        "y": iris.target.tolist(),
        "feature_names": iris.feature_names,
        "target_names": iris.target_names.tolist(),
    }
    pf.log_metric("total_samples", float(len(data["y"])))
    return data

@pf.node(name="sklearn-train", log=True, artifact="iris_model")
def train_model(data: dict, n_estimators: int = 100) -> dict:
    """训练 RandomForest 分类器并保存为模型工件"""
    pf.log(f"训练 RandomForest (n_estimators={n_estimators})", level="INFO")
    X = data["X"]
    y = data["y"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    clf = RandomForestClassifier(n_estimators=n_estimators, random_state=42)
    clf.fit(X_train, y_train)
    accuracy = accuracy_score(y_test, clf.predict(X_test))
    pf.log_metric("accuracy", float(accuracy))
    # 显式保存模型工件
    pf.save_model(
        "iris_model",
        clf,
        metadata={"accuracy": accuracy, "n_estimators": n_estimators},
    )
    return {"accuracy": accuracy}
```

**`sklearn_service.py`** — 加载模型进行推理：

```python
from pydantic import BaseModel
from typing import List
import pangflow as pf

class IrisPredictRequest(BaseModel):
    features: List[float]

class IrisPredictResponse(BaseModel):
    predicted_class: str
    predicted_index: int
    probabilities: List[float]
    trace_id: str

@pf.serve(endpoint="/api/v1/predict", method="POST")
def iris_predict_service(request: IrisPredictRequest) -> IrisPredictResponse:
    """Iris 分类器推理服务"""
    clf = pf.load_model("iris_model")
    proba = clf.predict_proba([request.features])[0]
    pred_idx = int(clf.predict([request.features])[0])
    target_names = ["setosa", "versicolor", "virginica"]
    return IrisPredictResponse(
        predicted_class=target_names[pred_idx],
        predicted_index=pred_idx,
        probabilities=proba.tolist(),
        trace_id=pf.get_trace_id(),
    )
```

### 2.3 注册、触发与模型管理

```bash
# 注册（command 自动推断为 python sklearn_workflow.py）
pangflowctl register sklearn_workflow.toml

# 触发
pangflowctl trigger sklearn-iris-workflow

# 查看模型工件
pangflowctl model list
# 输出：iris_model | 1.0.0 | development

# 晋级
ARTIFACT_ID=$(sqlite3 pangflow.db "SELECT artifact_id FROM artifacts WHERE name='iris_model';")
pangflowctl model promote "$ARTIFACT_ID" --to production
pangflowctl model rollback "$ARTIFACT_ID"
```

### 2.4 启动推理服务

```bash
pangflowctl serve start sklearn-iris-workflow --port 8001
```

测试：
```bash
curl -X POST http://127.0.0.1:8001/api/v1/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [5.1, 3.5, 1.4, 0.2]}'
# {"predicted_class": "setosa", "predicted_index": 0, "probabilities": [...], "trace_id": "t-xxxxxx"}
```

---

## WebUI 仪表盘

启动 WebUI：

```bash
pangflowctl webui start --port 8080
```

浏览器打开 `http://127.0.0.1:8080/`：

- **Dashboard**：工作流状态概览、最近执行、系统健康
- **Workflows**：工作流列表
- **Executions**：执行历史，点击每行右侧 **"View DAG"** 可展开查看该次执行的 DAG 节点状态变化（绿色=success、蓝色=running、红色=failed）及耗时
- **Models**：模型版本管理与晋级/回滚
- **Lineage**：数据血缘关系图
- **Settings**：环境与存储配置

> **v0.2.7 改进**：Executions 页面新增 **View DAG** 功能，展示每个节点的 `running` → `success`/`failed` 状态变化及 `duration_ms`。

---

## Prefect 部署（两个场景通用）

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

### 部署工作流

```bash
# 部署标准 workflow（自动创建 flow 文件并启动 serve）
pangflowctl deploy demo-workflow --cron "0 * * * *"

# 部署 sklearn workflow
pangflowctl deploy sklearn-iris-workflow --cron "0 2 * * *"

# 查看部署状态
pangflowctl deployment status

# Prefect serve 管理
pangflowctl deployment stop demo-workflow
pangflowctl deployment serve demo-workflow
```

---

## CLI 完整命令参考

```bash
# 工作空间
pangflowctl init [path]                        # 初始化工作空间（自动生成两套示例）

# 工作流管理
pangflowctl workflow list                      # 列出所有工作流
pangflowctl workflow get <name>                # 查看工作流详情
pangflowctl workflow delete <name>             # 删除工作流

# 环境管理（从 TOML 读取配置，独立 conda 环境）
pangflowctl env create --workflow <name> [--file <toml>]
pangflowctl env update --workflow <name> [--file <toml>]
pangflowctl env remove --workflow <name>
pangflowctl env list

# 注册与部署
pangflowctl register <workflow.toml>           # 解析 TOML 并注册（自动推断 command）
pangflowctl deploy <workflow> [--cron]         # 编译并部署到 Prefect（自动启动 serve）

# Prefect serve 管理（部署后自动启动）
pangflowctl deployment status                  # 查看所有部署状态
pangflowctl deployment stop <workflow>         # 停止 serve
pangflowctl deployment serve <workflow>        # 手动启动 serve

# HTTP 推理服务
pangflowctl serve start <workflow> [--port]
pangflowctl serve stop <workflow>
pangflowctl serve status

# 触发与查询
pangflowctl trigger <workflow> [--wait]
pangflowctl logs <workflow> [--node]           # 查询执行日志
pangflowctl metrics <workflow>                 # 查询业务指标
pangflowctl lineage <workflow>                 # 查询数据血缘
pangflowctl model list                         # 列出模型工件
pangflowctl model promote <artifact_id> --to <stage>   # 晋级模型
pangflowctl model rollback <artifact_id>       # 回滚模型
pangflowctl status                             # 查看系统状态

# WebUI
pangflowctl webui start [--port]
pangflowctl webui status
```

---

## 架构概览

PangFlow v0.2.7 采用 **五层 + 三子系统** 架构：

```
┌─────────────────────────────────────────┐
│  算法工程师视角                            │
│  @node / @workflow / @serve / >>        │
│  pf.save_model() / pf.load_model()      │
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

## 技术栈

- **Python**: 3.13.5+
- **工作流引擎**: Prefect 3.6.25
- **Web 框架**: FastAPI 0.135.3
- **CLI**: Typer + Rich
- **数据库**: SQLAlchemy + SQLite（默认），支持自动 schema 迁移
- **配置**: TOML + Pydantic
- **前端**: TypeScript + Tailwind CSS + Vite
- **打包**: uv + pyproject.toml

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

# 构建包
uv build

# 运行测试
pytest tests/
```

---

## 许可证

Apache-2.0 © 2026 The PangFlow Authors
