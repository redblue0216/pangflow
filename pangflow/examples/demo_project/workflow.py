"""PangFlow v0.2.7 示例工作流编排 —— 演示 >> 运算符

这个文件演示了如何使用 @pf.workflow 装饰器和 >> 运算符编排 DAG，
以及如何通过 if __name__ == "__main__" 入口直接运行工作流。
"""

import os
import pangflow as pf
from nodes import load_data, process_data, save_result


@pf.workflow(name="demo-workflow", schedule="0 * * * *")
def main_workflow():
    """演示串行工作流: 加载 -> 处理 -> 保存
    
    执行流程:
        1. load_data 生成 [1, 2, 3, 4, 5]
        2. process_data 计算平方 [1, 4, 9, 16, 25]
        3. save_result 保存为制品 {"label": "demo-run", "values": [...], "sum": 55}
           同时调用 pf.save_model() 注册到模型仓库
    """
    raw = load_data(n=5)
    processed = process_data(raw)
    result = save_result(processed, label="demo-run")
    return result


@pf.workflow(name="demo-parallel")
def parallel_workflow():
    """演示并行分支工作流
    
    执行流程:
        1. load_data 生成数据
        2. process_data 并行执行两次（同一输入，不同输出）
        3. 返回两个并行处理结果
    """
    raw = load_data(n=3)
    p1 = process_data(raw)
    p2 = process_data(raw)
    return p1, p2


if __name__ == "__main__":
    # 运行主工作流；Prefect / CLI trigger / API trigger 都会调用此入口
    import uuid
    os.environ.setdefault("PANGFLOW_RUN_ID", str(uuid.uuid4()))
    compiled = main_workflow()
    result = compiled()
    print(f"Workflow result: {result}")
