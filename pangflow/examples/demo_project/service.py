"""PangFlow v0.2.11 示例 HTTP 服务 —— 暴露推理端点

这个文件演示了如何使用 @pf.serve 装饰器将算法函数暴露为 HTTP API，
以及如何使用 pf.load_model() 加载已注册的模型工件进行推理。
"""

from pydantic import BaseModel
import pangflow as pf


class PredictRequest(BaseModel):
    """请求体模型"""
    index: int  # 要查询的结果索引


class PredictResponse(BaseModel):
    """响应体模型"""
    value: int
    label: str
    trace_id: str


@pf.serve(endpoint="/api/v1/predict", method="POST")
def demo_predict_service(request: PredictRequest) -> PredictResponse:
    """基于已保存结果的推理服务
    
    调用示例:
        curl -X POST http://localhost:8000/api/v1/predict \
            -H "Content-Type: application/json" \
            -d '{"index": 2}'
    
    响应:
        {"value": 9, "label": "demo-run", "trace_id": "t-xxxxxx"}
    """
    # 从模型仓库加载之前保存的工件
    try:
        artifact = pf.load_model("demo_result")
        values = artifact["values"]
        label = artifact.get("label", "unknown")
    except Exception as exc:
        pf.log(f"加载模型失败: {exc}", level="WARNING")
        # fallback: 直接计算
        values = [1, 4, 9, 16, 25]
        label = "fallback"

    idx = request.index % len(values)
    return PredictResponse(
        value=values[idx],
        label=label,
        trace_id=pf.get_trace_id(),
    )
