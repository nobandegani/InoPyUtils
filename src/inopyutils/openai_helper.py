from openai import OpenAI

from .util_helper import ino_ok, ino_err

class InoOpenAIHelper:
    @staticmethod
    def chat(
            api_key: str,
            base_url: str,
            model: str,
            messages: list,
            temperature: float = 0.7,
            max_tokens: int = 1024,
            **kwargs
    ) -> dict:
        """Send a chat completion request via the OpenAI-compatible API.

        Args:
            api_key: API key (e.g. RUNPOD_API_KEY).
            base_url: Base URL for the API (e.g. RunPod endpoint + /openai/v1).
            model: Model name to use.
            messages: List of message dicts with 'role' and 'content'.
            temperature: Sampling temperature.
            max_tokens: Max tokens in the response.
            **kwargs: Additional args passed to chat.completions.create().
        """
        try:
            client = OpenAI(api_key=api_key, base_url=base_url)
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                **kwargs
            )

            choice = response.choices[0] if response.choices else None
            content = choice.message.content if choice else ""

            return ino_ok(
                "chat complete",
                response=content,
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
