from .http_helper import InoHttpHelper
from .util_helper import ino_ok, ino_err

class InoRunpodHelper:
    @staticmethod
    async def ServerlessVllmRunSync(url: str, api_key: str, system_prompt: str, user_prompt: str, attach_image: bool, image: str, temperature: float = 0.7, max_tokens: int = 1024) -> dict :
        http_client = InoHttpHelper()
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        prompt = {
            "input": {
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "sampling_params": {
                    "temperature": temperature,
                    "max_tokens": max_tokens
                }
            }
        }
        response = await http_client.post(
            url=url,
            headers=headers,
            json=prompt
        )
        return response