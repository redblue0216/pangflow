"""PangFlow v0.2.19 示例算法节点 —— 纯 Python 标准库，零外部依赖

这个文件演示了如何使用 @pf.node 装饰器定义算法节点，
以及如何使用 pf.save_model() 将结果保存为模型工件。
"""

from datetime import datetime
from pathlib import Path
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
    # Save intermediate artifact so lineage can trace the data flow
    pf.save_model("processed_data", result, metadata={"stage": "intermediate"})
    return result


@pf.node(name="save-result", log=True, artifact="demo_result")
def save_result(processed: list, label: str = "demo-run") -> dict:
    """保存处理结果为制品，并注册到模型仓库"""
    pf.log(f"保存结果: label={label}, values={processed}", level="INFO")
    result = {"label": label, "values": processed, "sum": sum(processed)}
    # 显式保存为模型工件，支持后续的晋级/回滚/推理
    pf.save_model("demo_result", result, metadata={"label": label})
    pf.log("模型工件已保存: demo_result", level="INFO")
    return result


@pf.node(name="write-to-file", log=True)
def write_to_file(result: dict, file_path: str = "demo_results.txt") -> str:
    """将结果追加写入文本文件，带时间戳以区分每次执行"""
    timestamp = datetime.now().isoformat()
    line = f"[{timestamp}] label={result.get('label')}, values={result.get('values')}, sum={result.get('sum')}\n"
    path = Path(file_path)
    # 追加模式写入，区分每次执行
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)
    pf.log(f"结果已追加到文件: {path.resolve()}", level="INFO")
    return str(path.resolve())
