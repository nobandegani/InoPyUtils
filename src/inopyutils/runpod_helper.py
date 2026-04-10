import asyncio
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
            timeout: float = 300.0,
            max_polls: int = 24,
            poll_delay: float = 10.0,
            max_failed_retries: int = 5,
    ) -> dict:
        """Send a synchronous chat completion request to a RunPod serverless vLLM endpoint.

        Uses the OpenAI-compatible route for proper vision/multimodal support.
        Polls the status endpoint when the job is IN_QUEUE or IN_PROGRESS.
        Re-submits the job up to max_failed_retries times when status is FAILED.

        Args:
            url: RunPod endpoint URL. Accepts either the base endpoint
                (e.g. "https://api.runpod.ai/v2/<id>") or the full runsync URL
                (e.g. "https://api.runpod.ai/v2/<id>/runsync").
            api_key: RunPod API key.
            model: Model name served by vLLM.
            user_prompt: The user message string.
            system_prompt: Optional system message string.
            image: Optional image URL or base64 data URI (e.g. "https://..." or "data:image/jpeg;base64,...").
            temperature: Sampling temperature.
            max_tokens: Max tokens in the response.
            timeout: Total HTTP timeout in seconds (default 300s / 5 minutes).
            max_polls: Max number of status polls when job is IN_QUEUE/IN_PROGRESS (default 24 = ~4 min).
            poll_delay: Seconds to wait between status polls (default 10s).
            max_failed_retries: Max times to re-submit the entire job when status is FAILED (default 5).
        """
        try:
            # Normalize URL: accept either base endpoint or full /runsync URL
            base_url = url.rstrip("/")
            if base_url.endswith("/runsync"):
                base_url = base_url[: -len("/runsync")]
            runsync_url = f"{base_url}/runsync"

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

            async with InoHttpHelper(
                timeout_total=timeout,
                timeout_sock_read=timeout,
            ) as http_client:
                last_error = None

                for failed_attempt in range(1 + max_failed_retries):
                    if failed_attempt > 0:
                        await asyncio.sleep(poll_delay)

                    response = await http_client.post(
                        url=runsync_url,
                        headers=headers,
                        json=payload,
                        json_response=True
                    )

                    if ino_is_err(response):
                        return ino_err(response.get("msg", "request failed"),
                                       status_code=response.get("status_code"))

                    data = response.get("data", {})
                    status = data.get("status")

                    if status == "COMPLETED":
                        break

                    job_id = data.get("id")
                    if not job_id:
                        return ino_err(f"runsync not completed and no job id: {status}",
                                       error_code=data.get("error"), status=status)

                    # Poll status endpoint instead of re-posting
                    status_url = f"{base_url}/status/{job_id}"

                    poll_statuses = {"IN_QUEUE", "IN_PROGRESS"}
                    for _ in range(max_polls):
                        await asyncio.sleep(poll_delay)

                        status_resp = await http_client.get(
                            url=status_url,
                            headers=headers,
                            json=True
                        )

                        if ino_is_err(status_resp):
                            return ino_err(status_resp.get("msg", "status poll failed"),
                                           status_code=status_resp.get("status_code"))

                        data = status_resp.get("data", {})
                        status = data.get("status")

                        if status == "COMPLETED":
                            break

                        if status not in poll_statuses:
                            break
                    else:
                        return ino_err(f"runsync timed out after {max_polls} polls: {status}",
                                       id=job_id, status=status)

                    if status == "COMPLETED":
                        break

                    if status == "FAILED":
                        last_error = data.get("error")
                        if failed_attempt < max_failed_retries:
                            continue
                        return ino_err(f"runsync failed after {max_failed_retries} retries",
                                       error_code=last_error, status=status, id=job_id)

                    # Unknown non-retryable status
                    return ino_err(f"runsync not completed: {status}",
                                   error_code=data.get("error"), status=status)

            raw_output = data.get("output", [])
            output = raw_output[0] if isinstance(raw_output, list) and raw_output else raw_output if isinstance(raw_output, dict) else {}

            if not isinstance(output, dict) or not output:
                return ino_err("runsync completed but returned no output",
                               id=data.get("id"), status=status, output=raw_output)

            # OpenAI-compatible response format
            choices = output.get("choices", [])
            usage = output.get("usage")

            if not choices:
                return ino_err("runsync completed but returned no choices",
                               id=data.get("id"), status=status, output=output)

            first_choice = choices[0] if choices else {}
            message = first_choice.get("message", {})
            content = message.get("content") or message.get("reasoning") or ""

            if not content:
                return ino_err("runsync completed but returned no content",
                               id=data.get("id"), status=status, output=output)

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
