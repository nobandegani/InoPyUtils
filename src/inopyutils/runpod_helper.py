from typing import Optional

from .http_helper import InoHttpHelper
from .util_helper import ino_ok, ino_err, ino_is_err

class InoRunpodHelper:
    @staticmethod
    async def serverless_vllm_runsync(
            url: str,
            api_key: str,
            model: str,
            user_prompt: str,
            system_prompt: str = "",
            image: Optional[str] = None,
            temperature: float = 0.7,
            max_tokens: int = 1024,
    ) -> dict:
        """Send a synchronous chat completion request to a RunPod serverless vLLM endpoint.

        Uses the OpenAI-compatible route for proper vision/multimodal support.

        Args:
            url: RunPod runsync endpoint URL.
            api_key: RunPod API key.
            model: Model name served by vLLM.
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
                    "openai_route": "/v1/chat/completions",
                    "openai_input": {
                        "model": model,
                        "messages": messages,
                        "temperature": temperature,
                        "max_tokens": max_tokens,
                        "stream": False
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

            raw_output = data.get("output", [])
            output = raw_output[0] if isinstance(raw_output, list) and raw_output else raw_output if isinstance(raw_output, dict) else {}

            # OpenAI-compatible response format
            choices = output.get("choices", [])
            usage = output.get("usage")

            first_choice = choices[0] if choices else {}
            message = first_choice.get("message", {})
            content = message.get("content") or message.get("reasoning") or ""

            return ino_ok("runsync complete",
                          id=data.get("id"),
                          status=status,
                          delay_time=data.get("delayTime"),
                          execution_time=data.get("executionTime"),
                          response=content,
                          reasoning=message.get("reasoning"),
                          finish_reason=first_choice.get("finish_reason"),
                          choices=choices,
                          usage=usage,
                          output=output,
                          )
        except Exception as e:
            return ino_err(f"runsync failed: {e}")
