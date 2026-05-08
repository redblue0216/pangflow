"""PangFlow v0.2.19 sklearn 示例工作流编排

使用 sklearn Iris 数据集训练 RandomForest 分类器的完整工作流。
算法运行在独立的 conda 环境（sklearn 依赖安装在算法环境中）。
"""

import os
import pangflow as pf
from sklearn_nodes import load_iris_data, train_model, evaluate_model


@pf.workflow(name="sklearn-iris-workflow", schedule="0 2 * * *")
def iris_training_workflow():
    """Iris 分类器训练工作流: 加载数据 -> 训练模型 -> 评估"""
    data = load_iris_data()
    model_info = train_model(data, n_estimators=100)
    result = evaluate_model(model_info)
    return result


if __name__ == "__main__":
    # 运行主工作流；Prefect / CLI trigger / API trigger 都会调用此入口
    import uuid
    os.environ.setdefault("PANGFLOW_RUN_ID", str(uuid.uuid4()))
    compiled = iris_training_workflow()
    result = compiled()
    print(f"Workflow result: {result}")
