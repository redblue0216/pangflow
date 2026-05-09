# PangFlow

**PangFlow** 是一个面向算法工程师的 **Algorithm OPS 编排框架**，基于 [Prefect](https://www.prefect.io/) 构建，通过装饰器驱动的 DSL 让 DAG 编排像写普通 Python 函数一样简单。它提供从算法开发、训练调度、模型版本管理到在线推理服务的全生命周期支持，并内置了数据血缘追踪、Conda 环境隔离和 WebUI 仪表盘。

> **版本**: v0.3.1  
> **Python**: >= 3.13.5  
> **License**: Apache-2.0

---

## 目录

- [简介](#简介)
- [核心概念](#核心概念)
- [如何获得与快速入门（标准场景）](#如何获得与快速入门标准场景)
- [设计思路](#设计思路)
  - [总体设计](#总体设计)
  - [具体设计](#具体设计)
- [详细使用说明（sklearn 场景）](#详细使用说明sklearn-场景)
- [技术选型列表](#技术选型列表)

---

## 简介

在算法工程实践中，一个完整的算法上线流程通常包括：数据预处理 -> 模型训练 -> 评估验证 -> 模型晋级 -> 在线推理。这些步骤之间的依赖关系天然形成一张**有向无环图（DAG）**，但传统的脚本串联方式（Shell 脚本、Makefile、Airflow 的 Operator 堆砌）往往让算法工程师陷入" infra 细节 "的泥潭，而非专注于算法本身。

PangFlow 的核心理念是：**"用写 Python 函数的方式写工作流"**。

- 用 `@pf.node` 定义算法节点
- 用 `@pf.workflow` 编排 DAG（支持 `>>` 运算符或自然函数调用）
- 用 `@pf.serve` 暴露推理端点
- 用 `pangflowctl` 一键初始化、注册、触发、部署、 serve

PangFlow 在底层自动完成：
- DAG 构建、验证（环检测、类型检查、参数完整性检查）
- 拓扑排序与并行调度
- 执行日志与指标自动落库
- 模型工件版本管理与 stage 晋级（development -> staging -> production）
- 数据血缘自动追踪
- Conda 环境隔离执行
- FastAPI 推理服务编译与启动
- WebUI 可视化仪表盘

---

## 核心概念

### 1. 节点（Node）—— `@pf.node`

节点是算法工作流的最小执行单元，本质上就是一个被装饰的 Python 函数。PangFlow 通过装饰器捕获函数的签名（参数类型、返回值类型），用于后续 DAG 的自动边连接和类型校验。

```python
import pangflow as pf

@pf.node(name="load-data", log=True)
def load_data(n: int = 5) -> list:
    pf.log(f"生成 {n} 个数据点")
    return list(range(1, n + 1))
```

节点支持以下元数据：
- `name`: 节点名称，用于日志和 UI 展示
- `log=True`: 开启自动日志上下文注入，`pf.log()` 和 `pf.log_metric()` 会自动关联到当前节点
- `artifact="model_name"`: 标记该节点产出模型工件
- `feature=True`: 标记该节点产出特征工件

### 2. 工作流（Workflow）—— `@pf.workflow`

工作流是节点的编排容器。被 `@pf.workflow` 装饰的函数在执行时会进入**DAG 构建模式**：所有对 `@pf.node` 函数的调用都会被拦截，转化为 DAG 的节点和边，而非立即执行。

```python
@pf.workflow(name="demo-workflow", schedule="0 * * * *")
def main_workflow():
    raw = load_data(n=5)          # DAG 节点: load-data
    processed = process_data(raw) # DAG 节点: process-data，依赖 load-data
    result = save_result(processed) # DAG 节点: save-result，依赖 process-data
    return result
```

内部流程：
1. **DAG 构建**：`@workflow` 创建一个 `DAGBuilder`，拦截所有节点调用，记录节点和边
2. **DAG 验证**：环检测（DFS）、类型兼容性检查、必需参数覆盖检查
3. **拓扑排序**：Kahn 算法生成可并行化的执行层
4. **DAG 持久化**：将静态拓扑序列化到数据库 `workflows.dag_json`，供 WebUI 渲染
5. **编译为 Prefect Flow**：`FlowCompiler` 将 `DAGBuilder` 转换为 Prefect `@flow`，每个节点变为 `@task`
6. **执行**：Prefect 负责实际的调度和执行

### 3. 推理服务（Serve）—— `@pf.serve`

`@pf.serve` 将算法函数暴露为 HTTP 端点。PangFlow 的 `ServeCompiler` 会收集所有 `@serve` 装饰的函数，自动编译为一个 FastAPI 应用，支持 Pydantic 请求/响应模型、自动 OpenAPI 文档、Tracing 中间件。

```python
from pydantic import BaseModel
import pangflow as pf

class PredictRequest(BaseModel):
    index: int

class PredictResponse(BaseModel):
    value: int
    label: str

@pf.serve(endpoint="/api/v1/predict", method="POST")
def demo_predict_service(request: PredictRequest) -> PredictResponse:
    artifact = pf.load_model("demo_result")
    return PredictResponse(
        value=artifact["values"][request.index],
        label=artifact.get("label", "unknown"),
        trace_id=pf.get_trace_id(),
    )
```

### 4. 工作空间（Workspace）

PangFlow 采用**工作空间隔离**设计。每个项目是一个独立的工作空间，包含：

```
my_project/
├── pangflow.toml          # 工作空间配置
├── pangflow.db            # SQLite 数据库（工作流、执行记录、模型元数据）
├── workflows/             # TOML 工作流配置目录
├── data/
│   ├── models/            # 模型工件存储
│   └── features/          # 特征工件存储
├── logs/                  # 执行日志
└── temp/                  # 临时文件
```

工作空间发现优先级：
1. `PANGFLOW_WORKSPACE` 环境变量
2. 从当前目录向上遍历，寻找包含 `pangflow.toml` 的目录

### 5. 模型工件（Model Artifact）

PangFlow 的模型系统不是简单的文件保存，而是一个完整的**工件生命周期管理系统**：

- **`pf.save_model(name, model, metadata)`**: 序列化（pickle）-> 写入本地文件后端 -> 注册元数据到数据库
- **`pf.load_model(name, stage)`**: 从数据库查询元数据 -> 按 stage 过滤 -> 反序列化
- **Stage 晋级**: `pangflowctl model promote <id> --to production`
- **Stage 回滚**: `pangflowctl model rollback <id>`
- **版本管理**: 自动版本号（1.0.0 -> 1.0.1），支持历史版本列表

```
data/models/
├── iris_model/
│   ├── 1.0.0.pkl              # 模型二进制
│   └── 1.0.0.pkl.meta.json    # 元数据
└── iris_dataset/
    ├── 1.0.0.pkl
    └── 1.0.0.pkl.meta.json
```

### 6. 环境隔离（Conda Env）

PangFlow 支持为每个工作流绑定独立的 Conda 环境。训练节点在隔离的 conda 环境中通过 `cloudpickle` 序列化执行，推理服务也可以在 conda 环境中启动，彻底避免算法依赖与框架依赖的冲突。

```bash
pangflowctl env create --workflow sklearn-iris-workflow --file sklearn_workflow.toml
```

### 7. 数据血缘（Lineage）

PangFlow 自动追踪数据流向：
- 每个 `@node(artifact=...)` 产出的工件会被记录
- 下游节点与上游工件之间的依赖关系自动写入 `lineage_edges` 表
- WebUI 提供交互式血缘关系图

### 8. 观察者模式（Observer）

PangFlow 内部采用**发布-订阅**的事件总线：`get_subject().publish(event, data)`。内置观察者包括：
- **LogObserver**: 自动将 `pf.log()` 写入数据库 `node_logs` 表
- **MetricObserver**: 自动将 `pf.log_metric()` 写入 `metrics` 表
- **LineageObserver**: 自动追踪数据血缘
- **AlertObserver**: 执行失败时触发告警

---

## 如何获得与快速入门（标准场景）

标准场景使用纯 Python 标准库，零外部算法依赖，适合快速体验 PangFlow 的核心功能。

### 1. 安装

```bash
pip install pangflow==0.3.1
```

### 2. 初始化工作空间

```bash
pangflowctl init my_project
cd my_project
```

`init` 会复制示例文件（nodes.py、workflow.py、service.py、workflow.toml），并创建一个空的 SQLite 数据库。

### 3. 定义算法节点

编辑 `nodes.py`：

```python
import pangflow as pf

@pf.node(name="load-data", log=True)
def load_data(n: int = 5) -> list:
    pf.log(f"生成 {n} 个数据点", level="INFO")
    data = list(range(1, n + 1))
    pf.log_metric("data_count", float(n))
    return data

@pf.node(name="process-data", log=True)
def process_data(data: list) -> list:
    result = [x ** 2 for x in data]
    pf.log_metric("sum_squares", float(sum(result)))
    return result

@pf.node(name="save-result", log=True, artifact="demo_result")
def save_result(processed: list, label: str = "demo") -> dict:
    result = {"label": label, "values": processed, "sum": sum(processed)}
    pf.save_model("demo_result", result, metadata={"label": label})
    return result
```

### 4. 编排工作流

编辑 `workflow.py`：

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

### 5. 定义推理服务

编辑 `service.py`：

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
    artifact = pf.load_model("demo_result")
    values = artifact["values"]
    idx = request.index % len(values)
    return PredictResponse(
        value=values[idx],
        label=artifact.get("label", "unknown"),
        trace_id=pf.get_trace_id(),
    )
```

### 6. 注册工作流

```bash
pangflowctl register workflow.toml
```

### 7. 触发训练

```bash
pangflowctl trigger demo-workflow
```

输出示例：
```
Triggering demo-workflow  Run ID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
Workflow completed successfully
Workflow result: {'label': 'demo-run', 'values': [1, 4, 9, 16, 25], 'sum': 55}
```

### 8. 查看模型工件

```bash
pangflowctl model list
# demo_result | 1.0.0 | development
```

### 9. 模型晋级

```bash
ARTIFACT_ID=$(sqlite3 pangflow.db "SELECT artifact_id FROM artifacts WHERE name='demo_result';")
pangflowctl model promote "$ARTIFACT_ID" --to production
pangflowctl model list
# demo_result | 1.0.0 | production
```

### 10. 启动推理服务

```bash
pangflowctl serve start demo-workflow --port 8000
```

### 11. 测试推理

```bash
curl -X POST http://127.0.0.1:8000/api/v1/predict \
  -H "Content-Type: application/json" \
  -d '{"index": 2}'
# {"value": 9, "label": "demo-run", "trace_id": "t-xxxxxx"}
```

### 12. WebUI 仪表盘

```bash
pangflowctl webui start --port 8080
```

浏览器访问 `http://127.0.0.1:8080`：
- **Dashboard**: 工作流状态概览
- **Workflows**: 工作流列表与注册
- **Executions**: 执行历史，点击 "View DAG" 查看拓扑图
- **Models**: 模型版本管理与晋级/回滚
- **Lineage**: 数据血缘关系图

---

## 设计思路

### 总体设计

PangFlow 采用**分层架构**，从上到下分为五层：

```
+-------------------------------------------------------------+
|  用户层: @node / @workflow / @serve 装饰器 DSL              |
|  CLI: pangflowctl (init, register, trigger, serve, ...)     |
+-------------------------------------------------------------+
|  编排层: DAGBuilder -> DAG 验证 -> FlowCompiler              |
|  ServeCompiler -> FastAPI 应用                              |
+-------------------------------------------------------------+
|  执行层: Prefect @flow / @task                               |
|  策略模式: LocalExecutor / CondaExecutor / HTTPService       |
+-------------------------------------------------------------+
|  运行时层: 日志、指标、血缘、Tracing、告警 (Observer 模式)    |
+-------------------------------------------------------------+
|  持久化层: SQLite (ORM) + LocalFileBackend (pickle)         |
|  环境层: CondaEnv + EnvManager                               |
+-------------------------------------------------------------+
```

#### 设计哲学

1. **算法工程师优先**: 所有设计决策以降低算法工程师的心智负担为第一优先级。不需要学习 Airflow 的 Operator 概念，不需要写 YAML，不需要理解 Kubernetes。会写 Python 函数就会写工作流。

2. **渐进式复杂度**: 从最简单的 `@node` + `@workflow` 开始，需要时再引入 conda 隔离、模型晋级、血缘追踪。绝不强迫用户在 "Hello World" 阶段就理解整个系统。

3. **工作空间即边界**: 每个项目是一个独立的工作空间，数据库、模型文件、日志全部隔离。避免全局状态污染。

4. **Best-effort 容错**: 所有非核心路径（DAG 持久化、日志写入、血缘记录）都采用 try/except 静默跳过，确保即使数据库不可用，算法执行本身不会中断。

5. **装饰器即元数据**: `@node`、`@workflow`、`@serve` 不仅是语法糖，更是运行时元数据的注册入口。装饰器捕获的签名信息驱动了后续的 DAG 构建、类型检查、API 文档生成。

### 具体设计

#### DAG 构建机制

PangFlow 的 DAG 构建采用**调用拦截**模式：

1. `@node` 返回的不是原始函数，而是一个 `NodeProxy` 对象
2. `NodeProxy.__call__` 检查当前是否处于 DAG 构建上下文（通过 thread-local 的 `active_dag`）
3. 如果是构建上下文，则将当前调用记录为 DAG 的一个节点，并自动创建与上游节点的边
4. 如果不是构建上下文（如直接调用或 serve 中调用），则执行原始函数

```python
# 构建模式（在 @workflow 内部）
raw = load_data(n=5)          # -> DAGBuilder.add_node("load-data")
                              #   返回值是占位符，不是真实数据
processed = process_data(raw) # -> DAGBuilder.add_node("process-data")
                              #   DAGBuilder.add_edge("load-data" -> "process-data")

# 执行模式（在 serve 或直接调用）
result = load_data(n=5)       # -> 直接执行原始函数，返回 [1, 2, 3, 4, 5]
```

#### Prefect 集成

PangFlow 不重新发明轮子，而是站在 Prefect 的肩膀之上：
- `FlowCompiler.compile()` 将 `DAGBuilder` 转换为 Prefect `@flow`
- 每个节点转换为 Prefect `@task`，天然获得重试、超时、并发控制、状态管理
- Prefect Server 提供调度、监控、UI（`prefect server start`）
- 但 PangFlow 屏蔽了 Prefect 的复杂度，用户不需要写任何 Prefect 特有的代码

#### Conda 环境隔离

`_run_in_conda()` 是环境隔离的核心：
1. 通过 `cloudpickle` 序列化函数对象和参数（支持闭包、lambda、类实例等复杂对象）
2. 生成临时 Python 脚本，在脚本中：
   - 动态 `chdir` 到工作空间
   - 注入 PangFlow 包路径到 `sys.path`
   - 通过 `conda run -n <env>` 在隔离环境中执行
3. 反序列化执行结果并返回

这种设计的优势：
- 不需要在 conda 环境中安装 PangFlow（运行时注入 sys.path）
- 不需要在每个节点手动处理 conda 激活
- 支持任意复杂的 Python 对象作为节点参数和返回值

#### 模型工件系统

PangFlow 的模型系统区别于 MLflow 的重量级方案，采用**轻量级文件 + SQLite 元数据**的双层设计：

- **文件层**: `LocalFileBackend` 用 SHA-256 命名文件，避免重复存储
- **元数据层**: `MetaStore` 将 artifact 的 workflow_id、node_id、storage_key、checksum、tags 等写入 SQLite
- **版本层**: `ArtifactVersionModel` 记录每次晋级（promotion）操作，形成 stage 变更历史

Stage 机制：
- `development`: 默认 stage，训练产出
- `staging`: 预发布，用于 A/B 测试
- `production`: 线上服务加载的模型

serve 服务通过环境变量 `PANGFLOW_DEFAULT_STAGE=production` 自动过滤模型。

#### 数据血缘追踪

血缘追踪是**被动式**的：
1. 当节点调用 `pf.save_model()` 时，记录 `artifact_id` 到 log context
2. 下游节点执行时，通过 `_find_upstream_artifacts()` 递归查找最近的上游 artifact
3. 在 `lineage_edges` 表中写入 `(from_artifact_id, to_artifact_id)` 边

用户不需要手动声明血缘关系，系统自动从 DAG 拓扑和 artifact 产出推断。

#### WebUI 与 API

PangFlow 提供两套界面：
- **FastAPI REST API** (`pangflow/web/api.py`): 工作流 CRUD、执行查询、模型管理、血缘查询、DAG 可视化
- **静态 WebUI** (`pangflow/web/static/`): 基于前端构建的 SPA，通过 Fetch 调用 REST API

DAG 可视化采用**静态拓扑 + 运行时状态叠加**的策略：
1. 注册时从 `WorkflowModel.dag_json` 读取静态 DAG
2. 查询时从 `NodeLogModel` 读取各节点的执行状态
3. 前端将状态和拓扑合并渲染

这种设计避免了运行时重放 DAG 的不确定性，确保 UI 展示的拓扑与注册时完全一致。

---

## 详细使用说明（sklearn 场景）

sklearn 场景演示如何在**独立 Conda 环境**中使用 scikit-learn 训练模型，并暴露为在线推理服务。

### 1. 初始化工作空间

```bash
pangflowctl init my_project
cd my_project
```

确认 sklearn 示例文件已存在：
- `sklearn_nodes.py` — 算法节点
- `sklearn_workflow.py` — 工作流编排
- `sklearn_service.py` — 推理服务
- `sklearn_workflow.toml` — TOML 配置

### 2. 查看 sklearn 示例代码

**`sklearn_nodes.py`**:

```python
import pangflow as pf
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

@pf.node(name="sklearn-load-data", log=True)
def load_iris_data() -> dict:
    iris = load_iris()
    data = {
        "X": iris.data.tolist(),
        "y": iris.target.tolist(),
        "feature_names": iris.feature_names,
        "target_names": iris.target_names.tolist(),
    }
    pf.log_metric("total_samples", float(len(data["y"])))
    pf.save_model("iris_dataset", data, metadata={"stage": "raw"})
    return data

@pf.node(name="sklearn-train", log=True, artifact="iris_model")
def train_model(data: dict, n_estimators: int = 100) -> dict:
    X_train, X_test, y_train, y_test = train_test_split(
        data["X"], data["y"], test_size=0.2, random_state=42
    )
    clf = RandomForestClassifier(n_estimators=n_estimators, random_state=42)
    clf.fit(X_train, y_train)
    accuracy = accuracy_score(y_test, clf.predict(X_test))
    pf.log_metric("accuracy", float(accuracy))
    pf.save_model("iris_model", clf, metadata={"accuracy": accuracy})
    return {"accuracy": accuracy}

@pf.node(name="sklearn-evaluate", log=True)
def evaluate_model(model_info: dict) -> dict:
    pf.log(f"模型评估结果: accuracy={model_info['accuracy']:.4f}")
    return model_info
```

**`sklearn_workflow.py`**:

```python
import os
import pangflow as pf
from sklearn_nodes import load_iris_data, train_model, evaluate_model

@pf.workflow(name="sklearn-iris-workflow", schedule="0 2 * * *")
def iris_training_workflow():
    data = load_iris_data()
    model_info = train_model(data, n_estimators=100)
    result = evaluate_model(model_info)
    return result

if __name__ == "__main__":
    import uuid
    os.environ.setdefault("PANGFLOW_RUN_ID", str(uuid.uuid4()))
    compiled = iris_training_workflow()
    result = compiled()
    print(f"Workflow result: {result}")
```

**`sklearn_service.py`**:

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

### 3. 创建 Conda 环境

```bash
pangflowctl env create --workflow sklearn-iris-workflow --file sklearn_workflow.toml
```

`env create` 会：
1. 读取 TOML 中的 conda/pip 依赖配置
2. 执行 `conda create -n sklearn-iris-workflow python=3.13 numpy scikit-learn`
3. 在 conda 环境中 `pip install -e <pangflow_root>`（将 PangFlow 本身安装到环境中）
4. 将环境元数据写入数据库

### 4. 注册工作流

```bash
pangflowctl register sklearn_workflow.toml
```

### 5. 触发训练

```bash
pangflowctl trigger sklearn-iris-workflow
```

执行流程：
1. CLI 解析 `sklearn_workflow.toml`，找到入口文件 `sklearn_workflow.py`
2. 导入并执行 `iris_training_workflow()`，进入 DAG 构建模式
3. DAG 构建完成后，`FlowCompiler` 编译为 Prefect `@flow`
4. Prefect 调度执行：
   - 节点 `sklearn-load-data`：在本地或 conda 环境中执行（根据 env 配置）
   - 节点 `sklearn-train`：在 `sklearn-iris-workflow` conda 环境中执行（通过 `cloudpickle` + `conda run`）
   - 节点 `sklearn-evaluate`：同上
5. 每个节点执行时，`pf.log()` 和 `pf.log_metric()` 自动写入数据库
6. `pf.save_model()` 将模型序列化到 `data/models/iris_model/1.0.0.pkl`

输出示例：
```
Triggering sklearn-iris-workflow  Run ID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
Workflow completed successfully
Workflow result: {'accuracy': 1.0, 'feature_names': [...], 'target_names': [...]}
```

### 6. 查看模型工件

```bash
pangflowctl model list
```

输出：
```
iris_dataset | 1.0.0 | development
iris_model   | 1.0.0 | development
```

模型文件存储在：
```
my_project/data/models/
├── iris_dataset/
│   └── 1.0.0.pkl
└── iris_model/
    └── 1.0.0.pkl
```

### 7. 模型晋级

```bash
# 获取 artifact_id
ARTIFACT_ID=$(sqlite3 pangflow.db "SELECT artifact_id FROM artifacts WHERE name='iris_model';")

# 晋级到 production
pangflowctl model promote "$ARTIFACT_ID" --to production

# 查看晋级历史
pangflowctl model list
# iris_model | 1.0.0 | production

# 回滚到上一 stage
pangflowctl model rollback "$ARTIFACT_ID"
```

### 8. 启动推理服务

```bash
pangflowctl serve start sklearn-iris-workflow --port 8001
```

serve 启动流程：
1. 发现 service 文件：`sklearn_service.py`
2. 检测到有 conda 环境绑定，生成临时启动脚本
3. 脚本内容：
   - `chdir` 到工作空间
   - 注入 PangFlow 路径到 `sys.path`
   - 导入 `sklearn_service.py`，触发 `@pf.serve` 注册
   - `ServeCompiler.compile()` 将注册的 endpoints 编译为 FastAPI 应用
   - `uvicorn.run(app, host="127.0.0.1", port=8001)`
4. 通过 `conda run -n sklearn-iris-workflow python <temp_script>` 在隔离环境中启动
5. stdout/stderr 重定向到 `logs/sklearn-iris-workflow_serve.log`

输出：
```
Serve started sklearn-iris-workflow at http://127.0.0.1:8001 (conda: sklearn-iris-workflow)
Logs: /path/to/my_project/logs/sklearn-iris-workflow_serve.log
Press Ctrl+C to stop.
```

### 9. 测试推理

```bash
# setosa
curl -X POST http://127.0.0.1:8001/api/v1/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [5.1, 3.5, 1.4, 0.2]}'
# {"predicted_class": "setosa", "predicted_index": 0, "probabilities": [1.0, 0.0, 0.0], "trace_id": "..."}

# versicolor
curl -X POST http://127.0.0.1:8001/api/v1/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [6.2, 2.8, 4.8, 1.8]}'
# {"predicted_class": "versicolor", ...}

# virginica
curl -X POST http://127.0.0.1:8001/api/v1/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [7.2, 3.2, 6.0, 1.8]}'
# {"predicted_class": "virginica", ...}
```

### 10. 查看 serve 日志

如果推理返回异常，查看日志排查：

```bash
cat logs/sklearn-iris-workflow_serve.log
```

### 11. 完整命令速查表

```bash
# 工作空间
pangflowctl init <path>                        # 初始化工作空间

# 工作流
pangflowctl register <workflow.toml>           # 解析 TOML 并注册
pangflowctl workflow list                      # 列出所有工作流
pangflowctl trigger <workflow> [--wait]        # 触发训练
pangflowctl logs <workflow> [--node]           # 查询执行日志
pangflowctl metrics <workflow>                 # 查询业务指标
pangflowctl lineage <workflow>                 # 查询数据血缘

# 环境
pangflowctl env create --workflow <name>       # 创建 conda 环境
pangflowctl env list                           # 列出所有环境
pangflowctl env remove --workflow <name>       # 删除环境

# 模型
pangflowctl model list                         # 列出模型工件
pangflowctl model promote <id> --to <stage>    # 晋级模型
pangflowctl model rollback <id>                # 回滚模型

# 推理服务
pangflowctl serve start <workflow> [--port]    # 启动 serve
pangflowctl serve stop <workflow>              # 停止 serve
pangflowctl serve status                       # 查看 serve 状态

# WebUI
pangflowctl webui start [--port]               # 启动 WebUI
```

---

## 技术选型列表

| 层级 | 组件 | 选型 | 理由 |
|------|------|------|------|
| **编排引擎** | 工作流调度 | [Prefect](https://www.prefect.io/) v3.6.25 | 现代 Pythonic 工作流引擎，原生支持 `@flow`/`@task`，状态管理完善，社区活跃 |
| **API 框架** | HTTP 服务 | [FastAPI](https://fastapi.tiangolo.com/) v0.135.3 | 异步高性能，原生 Pydantic 集成，自动生成 OpenAPI 文档 |
| **数据验证** | 请求/响应模型 | [Pydantic](https://docs.pydantic.dev/) v2.12.5+ | Python 类型系统的最佳实践，运行时校验 + 序列化 |
| **ORM** | 数据库访问 | [SQLAlchemy](https://www.sqlalchemy.org/) v2.0.49+ | SQLAlchemy 2.0 的声明式模型与类型提示原生兼容 |
| **数据库** | 元数据存储 | SQLite | 零配置、单文件、足够支撑中小规模算法 OPS 场景 |
| **序列化** | 函数/模型序列化 | [cloudpickle](https://github.com/cloudpipe/cloudpickle) v3.1.2+ | 支持闭包、lambda、类实例等复杂对象的跨进程序列化 |
| **CLI** | 命令行界面 | [Typer](https://typer.tiangolo.com/) v0.24.1+ | 基于类型注解自动生成 CLI，开发体验极佳 |
| **终端输出** | 美化输出 | [Rich](https://github.com/Textualize/rich) v14.3.3+ | 表格、进度条、语法高亮，提升 CLI 体验 |
| **配置解析** | TOML 解析 | [tomli](https://github.com/hukkin/tomli) / [tomli-w](https://github.com/hukkin/tomli-w) | Python 3.11+ 标准库 tomli 的兼容方案 |
| **前端** | WebUI | Vite + React + TailwindCSS | 现代前端工具链，构建产物为纯静态文件，零后端依赖 |
| **环境隔离** | Conda 管理 | conda CLI | 算法领域的事实标准，支持 Python + 非 Python 依赖 |
| **模型存储** | 文件后端 | LocalFileBackend (pickle) | 轻量级，算法工程师最熟悉的序列化方式 |
| **事件系统** | 观察者模式 | 自定义 Subject/Observer | 解耦日志、指标、血缘、告警，便于扩展 |
