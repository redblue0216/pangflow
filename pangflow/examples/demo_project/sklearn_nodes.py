"""PangFlow v0.2.19 sklearn 示例算法节点 —— 使用 sklearn 自带 Iris 数据集

这个文件演示了如何在独立 conda 环境中使用 sklearn 进行训练和推理，
以及如何使用 pf.save_model() 将训练好的模型注册为工件。
"""

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
    pf.log_metric("n_features", float(len(data["feature_names"])))
    # Save raw dataset artifact so lineage can trace the data flow
    pf.save_model("iris_dataset", data, metadata={"stage": "raw"})
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
    y_pred = clf.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    pf.log_metric("accuracy", float(accuracy))
    pf.log_metric("n_estimators", float(n_estimators))
    pf.log(f"训练完成，准确率: {accuracy:.4f}", level="INFO")
    # 显式保存模型工件到 PangFlow 模型仓库
    pf.save_model(
        "iris_model",
        clf,
        metadata={
            "accuracy": accuracy,
            "n_estimators": n_estimators,
            "feature_names": data["feature_names"],
            "target_names": data["target_names"],
        },
    )
    pf.log("模型工件已保存: iris_model", level="INFO")
    return {
        "accuracy": accuracy,
        "feature_names": data["feature_names"],
        "target_names": data["target_names"],
    }


@pf.node(name="sklearn-evaluate", log=True)
def evaluate_model(model_info: dict) -> dict:
    """评估模型并输出结果"""
    pf.log(f"模型评估结果: accuracy={model_info['accuracy']:.4f}", level="INFO")
    return {
        "accuracy": model_info["accuracy"],
        "feature_names": model_info["feature_names"],
        "target_names": model_info["target_names"],
    }
