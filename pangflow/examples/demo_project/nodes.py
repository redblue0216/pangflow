"""PangFlow v0.2.7 示例算法节点 —— 纯 Python 标准库，零外部依赖

这个文件演示了如何使用 @pf.node 装饰器定义算法节点，
以及如何使用 pf.save_model() 将结果保存为模型工件。
"""

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
