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
                    json=payload
                )

            if ino_is_err(response):
                return response

            return ino_ok("runsync complete", data=response.get("data"))
        except Exception as e:
            return ino_err(f"runsync failed: {e}")
