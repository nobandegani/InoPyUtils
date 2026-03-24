from .http_helper import InoHttpHelper
from .util_helper import ino_ok, ino_err, ino_is_err

class InoRunpodHelper:
    @staticmethod
    async def serverless_vllm_runsync(url: str, api_key: str, system_prompt: str, user_prompt: str, temperature: float = 0.7, max_tokens: int = 1024, image_url: str = None) -> dict:
        """Send a synchronous chat completion request to a RunPod serverless vLLM endpoint."""
        try:
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }

            if image_url:
                user_content = [
                    {"type": "text", "text": user_prompt},
                    {"type": "image_url", "image_url": {"url": image_url}}
                ]
            else:
                user_content = user_prompt

            payload = {
                "input": {
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_content}
                    ],
                    "sampling_params": {
                        "temperature": temperature,
                        "max_tokens": max_tokens
                    }
                }
            }

            async with InoHttpHelper() as http_client:
                response = await http_client.post(
                    url=url,
                    headers=headers,
                    json=payload,
                    json_response=True
                )

            if ino_is_err(response):
                return ino_err(response.get("msg", "request failed"),
                               status_code=response.get("status_code"))

            data = response.get("data", {})
            status = data.get("status")

            if status != "COMPLETED":
                return ino_err(f"runsync not completed: {status}",
                               error_code=data.get("error"),
                               status=status)

            output = data.get("output", [])
            first_output = output[0] if isinstance(output, list) and output else output

            choices = first_output.get("choices") if isinstance(first_output, dict) else None
            usage = first_output.get("usage") if isinstance(first_output, dict) else None

            return ino_ok("runsync complete",
                          id=data.get("id"),
                          status=status,
                          delay_time=data.get("delayTime"),
                          execution_time=data.get("executionTime"),
                          response="",
                          choices=choices,
                          usage=usage,
                          output = output,
                          )
        except Exception as e:
            return ino_err(f"runsync failed: {e}")
