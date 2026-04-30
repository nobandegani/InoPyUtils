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
            top_p: float = 1.0,
            enable_thinking: bool = True,
            repetition_penalty: Optional[float] = None,
            top_k: Optional[int] = None,
            min_p: Optional[float] = None,
            extra_body: Optional[dict] = None,
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
            top_p: Nucleus sampling probability (default 1.0).
            enable_thinking: For Qwen3-style thinking models on vLLM, toggles the
                thinking phase via the chat template (default True). Sent as
                chat_template_kwargs.enable_thinking in extra_body.
            repetition_penalty: vLLM-specific. Penalty for repeated tokens
                (typical: 1.0–1.1).
            top_k: vLLM-specific. Top-k sampling cutoff.
            min_p: vLLM-specific. Min-p sampling cutoff.
            extra_body: Additional vLLM/server-specific params merged into the
                request body. Overrides any keys set by the named args above.
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
            # Build extra_body from vLLM-specific params + chat template toggle.
            # User-provided extra_body wins on conflicts (deep-merged for
            # chat_template_kwargs).
            built_extra: dict = {}
            chat_template_kwargs: dict = {"enable_thinking": enable_thinking}

            if repetition_penalty is not None:
                built_extra["repetition_penalty"] = repetition_penalty
            if top_k is not None:
                built_extra["top_k"] = top_k
            if min_p is not None:
                built_extra["min_p"] = min_p

            if extra_body:
                user_ctk = extra_body.get("chat_template_kwargs")
                if isinstance(user_ctk, dict):
                    chat_template_kwargs.update(user_ctk)
                built_extra.update({k: v for k, v in extra_body.items() if k != "chat_template_kwargs"})

            built_extra["chat_template_kwargs"] = chat_template_kwargs

            response = await client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                top_p=top_p,
                extra_body=built_extra,
                **kwargs
            )

            choice = response.choices[0] if response.choices else None
            msg = choice.message if choice else None

            content = (msg.content if msg else "") or ""
            reasoning = getattr(msg, "reasoning", None) if msg else None
            tool_calls = getattr(msg, "tool_calls", None) if msg else None

            return ino_ok(
                "chat complete",
                response=content,
                reasoning=reasoning,
                tool_calls=tool_calls,
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
