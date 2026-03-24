import base64
from typing import Union

from .http_helper import InoHttpHelper
from .util_helper import ino_ok, ino_err, ino_is_err

class InoRunpodHelper:
    @staticmethod
    async def serverless_vllm_runsync(
            url: str,
            api_key: str,
            system_prompt: str,
            user_prompt: str,
            temperature: float = 0.7,
            max_tokens: int = 1024,
            image: Union[str, bytes, None] = None
    ) -> dict:
        """Send a synchronous chat completion request to a RunPod serverless vLLM endpoint.

        Args:
            image: Image as a URL string or raw bytes (JPEG/PNG). Bytes are base64-encoded as a data URI.
        """
        try:
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }

            if image is not None:
                if isinstance(image, bytes):
                    b64 = base64.b64encode(image).decode("ascii")
                    image_uri = f"data:image/jpeg;base64,{b64}"
                else:
                    image_uri = image
                user_content = [
                    {"type": "text", "text": user_prompt},
                    {"type": "image_url", "image_url": {"url": image_uri}}
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
            first_output = output[0] if isinstance(output, list) and output else {}

            choices = first_output.get("choices", []) if isinstance(first_output, dict) else []
            usage = first_output.get("usage") if isinstance(first_output, dict) else None

            # Extract first choice -> first token as the response text
            first_choice = choices[0] if choices else {}
            tokens = first_choice.get("tokens", []) if isinstance(first_choice, dict) else []
            response_text = tokens[0] if tokens else ""

            return ino_ok("runsync complete",
                          id=data.get("id"),
                          status=status,
                          delay_time=data.get("delayTime"),
                          execution_time=data.get("executionTime"),
                          response=response_text,
                          choices=choices,
                          usage=usage,
                          output=output,
                          )
        except Exception as e:
            return ino_err(f"runsync failed: {e}")
