"""PangFlow v0.2.19 sklearn 示例 HTTP 推理服务

暴露已训练的 Iris 分类器作为 REST API。
推理服务通过 pf.load_model() 从模型仓库加载模型，接收特征向量返回预测结果。
"""

from pydantic import BaseModel
from typing import List
import pangflow as pf


class IrisPredictRequest(BaseModel):
    features: List[float]  # 4 个特征: sepal length, sepal width, petal length, petal width


class IrisPredictResponse(BaseModel):
    predicted_class: str
    predicted_index: int
    probabilities: List[float]
    trace_id: str


@pf.serve(endpoint="/api/v1/predict", method="POST")
def iris_predict_service(request: IrisPredictRequest) -> IrisPredictResponse:
    """Iris 分类器推理服务"""
    # 从模型仓库加载已训练的模型
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
