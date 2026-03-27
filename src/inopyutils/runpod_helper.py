from typing import Optional

from .http_helper import InoHttpHelper
from .util_helper import ino_ok, ino_err, ino_is_err

class InoRunpodHelper:
    @staticmethod
    async def serverless_vllm_runsync(
            url: str,
            api_key: str,
            user_prompt: str,
            system_prompt: str = "",
            image: Optional[str] = None,
            temperature: float = 0.7,
            max_tokens: int = 1024,
    ) -> dict:
        """Send a synchronous chat completion request to a RunPod serverless vLLM endpoint.

        Args:
            url: RunPod runsync endpoint URL.
            api_key: RunPod API key.
            user_prompt: The user message string.
            system_prompt: Optional system message string.
            image: Optional image URL or base64 data URI (e.g. "https://..." or "data:image/jpeg;base64,...").
            temperature: Sampling temperature.
            max_tokens: Max tokens in the response.
        """
        try:
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }

            if image:
                user_content = [
                    {"type": "text", "text": user_prompt},
                    {"type": "image_url", "image_url": {"url": image}}
                ]
            else:
                user_content = user_prompt

            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": user_content})

            payload = {
                "input": {
                    "messages": messages,
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
