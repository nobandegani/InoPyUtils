from typing import Optional

from openai import AsyncOpenAI

from .util_helper import ino_ok, ino_err


class InoOpenAIHelper:
    @staticmethod
    async def chat_completions(
            api_key: str,
            base_url: str,
            model: str,
            user_prompt: str,
            system_prompt: str = "",
            image: Optional[str] = None,
            temperature: float = 0.7,
            max_tokens: int = 1024,
            **kwargs
    ) -> dict:
        """Send a chat completion request via the OpenAI-compatible API.

        Args:
            api_key: API key for the target endpoint (OpenAI, a self-hosted
                vLLM server, RunPod, Modal, etc.). For Modal auth, pass
                "wk-<key>:ws-<secret>" — it will be split into Modal-Key
                and Modal-Secret headers automatically.
            base_url: Base URL for the OpenAI-compatible API
                (e.g. "https://api.openai.com/v1", "http://localhost:8000/v1"
                for a local vLLM server, or a RunPod/Modal endpoint + "/v1").
            model: Model name to use.
            user_prompt: The user message string.
            system_prompt: Optional system message string.
            image: Optional image URL or base64 data URI (e.g. "https://..." or "data:image/jpeg;base64,...").
            temperature: Sampling temperature.
            max_tokens: Max tokens in the response.
            **kwargs: Additional args passed to chat.completions.create().
        """
        try:
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})

            if image:
                user_content = [
                    {"type": "text", "text": user_prompt},
                    {"type": "image_url", "image_url": {"url": image}}
                ]
            else:
                user_content = user_prompt
            messages.append({"role": "user", "content": user_content})

            # Modal auth: "wk-<key>:ws-<secret>" → Modal-Key / Modal-Secret headers
            if api_key.startswith("wk-") and ":" in api_key:
                modal_key, modal_secret = api_key.split(":", 1)
                client = AsyncOpenAI(
                    api_key="modal",
                    base_url=base_url,
                    default_headers={
                        "Modal-Key": modal_key,
                        "Modal-Secret": modal_secret,
                    },
                )
            else:
                client = AsyncOpenAI(api_key=api_key, base_url=base_url)
            response = await client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                **kwargs
            )

            choice = response.choices[0] if response.choices else None
            msg = choice.message if choice else None
            content = (msg.content or getattr(msg, "reasoning", None) or "") if msg else ""

            return ino_ok(
                "chat complete",
                response=content,
                reasoning=getattr(msg, "reasoning", None) if msg else None,
                finish_reason=choice.finish_reason if choice else None,
                usage={
                    "prompt_tokens": response.usage.prompt_tokens,
                    "completion_tokens": response.usage.completion_tokens,
                    "total_tokens": response.usage.total_tokens,
                } if response.usage else None,
                raw=response
            )
        except Exception as e:
            return ino_err(f"chat failed: {e}")
