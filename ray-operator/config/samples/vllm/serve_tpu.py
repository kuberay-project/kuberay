import os

import json
import logging
from typing import Dict, List, Optional

import ray
from fastapi import FastAPI
from ray import serve
from starlette.requests import Request
from starlette.responses import Response

from vllm import LLM, SamplingParams

logger = logging.getLogger("ray.serve")

app = FastAPI()

@serve.deployment(name="VLLMDeployment")
@serve.ingress(app)
class VLLMDeployment:
    def __init__(
        self,
        model_id,
        num_tpu_chips,
        max_model_len,
        tokenizer_mode,
        dtype,
    ):
        self.llm = LLM(
            model=model_id,
            tensor_parallel_size=num_tpu_chips,
            max_model_len=max_model_len,
            dtype=dtype,
            download_dir=os.environ['VLLM_XLA_CACHE_PATH'],  # Error if not provided.
            tokenizer_mode=tokenizer_mode,
            enforce_eager=True,
        )

    @app.post("/v1/generate")
    async def generate(self, request: Request):
        request_dict = await request.json()
        prompts = request_dict.pop("prompt")
        max_toks = int(request_dict.pop("max_tokens"))
        print("Processing prompt ", prompts)
        sampling_params = SamplingParams(temperature=0.7,
                                         top_p=1.0,
                                         n=1,
                                         max_tokens=max_toks)

        outputs = self.llm.generate(prompts, sampling_params)
        for output in outputs:
            prompt = output.prompt
            generated_text = ""
            token_ids = []
            for completion_output in output.outputs:
                generated_text += completion_output.text
                token_ids.extend(list(completion_output.token_ids))

            print("Generated text: ", generated_text)
            ret = {
                "prompt": prompt,
                "text": generated_text,
                "token_ids": token_ids,
            }

        return Response(content=json.dumps(ret))

def get_num_tpu_chips() -> int:
    if "TPU" not in ray.cluster_resources():
        # Pass in TPU chips when the current Ray cluster resources can't be auto-detected (i.e for autoscaling).
        if os.environ.get('TPU_CHIPS') is not None:
            return int(os.environ.get('TPU_CHIPS'))
        return 0
    return int(ray.cluster_resources()["TPU"])

def get_max_model_len() -> Optional[int]:
    if 'MAX_MODEL_LEN' not in os.environ or os.environ['MAX_MODEL_LEN'] == "":
        return None
    return int(os.environ['MAX_MODEL_LEN'])

def get_tokenizer_mode() -> str:
    if 'TOKENIZER_MODE' not in os.environ or os.environ['TOKENIZER_MODE'] == "":
        return "auto"
    return os.environ['TOKENIZER_MODE']

def get_dtype() -> str:
    if 'DTYPE' not in os.environ or os.environ['DTYPE'] == "":
        return "auto"
    return os.environ['DTYPE']

def build_app(cli_args: Dict[str, str]) -> serve.Application:
    """Builds the Serve app based on CLI arguments."""
    ray.init(ignore_reinit_error=True, address="ray://localhost:10001")

    model_id = os.environ['MODEL_ID']

    num_tpu_chips = get_num_tpu_chips()
    pg_resources = []
    pg_resources.append({"CPU": 1})  # for the deployment replica
    for i in range(num_tpu_chips):
        pg_resources.append({"CPU": 1, "TPU": 1})  # for the vLLM actors

    # Use PACK strategy since the deployment may use more than one TPU node.
    return VLLMDeployment.options(
        placement_group_bundles=pg_resources,
        placement_group_strategy="PACK").bind(model_id, num_tpu_chips, get_max_model_len(), get_tokenizer_mode(), get_dtype())

model = build_app({})
