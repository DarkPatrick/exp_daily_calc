import requests
import pandas as pd
from typing import Any
from pydantic import BaseModel, Field
from json import dumps
import time



class Mb_Client(BaseModel):
    url: str
    username: str
    password: str = Field(repr=False)
    session_header: dict = Field(default_factory=dict, repr=False)


    def model_post_init(self, __context: Any) -> None:
        self.get_session()


    def get_session(self) -> None:
        credentials: dict = {
            "username": self.username,
            "password": self.password
        }
        
        response = requests.post(
            f"{self.url}/api/session",
            json=credentials
        )

        session_id = response.json()["id"]
        setattr(self, "session_header", {"X-Metabase-Session": session_id})


    def _extract_mb_error(self, resp: requests.Response) -> str:
        try:
            js = resp.json()
        except Exception:
            return f"HTTP {resp.status_code}: {resp.text[:1000]}"

        parts = []
        for k in ("message", "error", "cause"):
            if k in js and js[k]:
                parts.append(str(js[k]))
        if isinstance(js.get("errors"), dict) and js["errors"]:
            parts.append(dumps(js["errors"], ensure_ascii=False))
        if js.get("state") == "failed":
            parts.append(str(js.get("error_type", "state=failed")))
        if not parts:
            parts.append(str(js))
        return f"HTTP {resp.status_code}: " + " | ".join(parts)


# Unexpected response structure: {'database_id': 2, 'started_at': '2025-08-12T19:38:48.673207Z', 'via': [{'status': 'failed', 'class': 'class java.sql.SQLException', 'error': 'Code: 241. DB::Exception: Memory limit (for query) exceeded: would use 9.42 GiB (attempt to allocate chunk of 133366410 bytes), maximum: 9.31 GiB.:
    def post(self, api_endpoint: str, query: str) -> dict:
        payload: dict = {
            "database": 2,
            "type": "native",
            "format_rows": False,
            "pretty": False,
            "native": {
                "query": query
            }
        }
        for attempt in range(10):
            post = requests.post(
                f'{self.url}/api/{api_endpoint}',
                headers=self.session_header | {
                    "Content-Type": "application/json;charset=utf-8"
                },
                json=payload
            )
            if post.status_code >= 400:
                print(self._extract_mb_error(post))
                time.sleep(5)
            # elif post.status_code == 202:
            #     print("Metabase returned 202 Accepted (query still running). Retrying...")
            #     print(post.json())
            else:
                try:
                    json_res = post.json()
                    # if there are no json_res['data']['cols'] print json_res
                    if 'data' not in json_res or 'cols' not in json_res['data'] or json_res['data']['cols'] == [] or json_res['data']['cols'] is None:
                        print(f"Unexpected response structure: {json_res}")
                        print(f"erorr, reloading")
                        time.sleep(3)
                        continue
                except ValueError as e:
                    print(f"Invalid JSON in response: {post.text[:1000]}")
                if any(k in json_res for k in ("error", "message")) and "data" not in json_res:
                    print(str(json_res.get("error") or json_res.get("message")))
                else:
                    break
        # json_res = post.json()
        column_names = [col['display_name'] for col in json_res['data']['cols']]
        data_rows = json_res['data']['rows']
        df = pd.DataFrame(data_rows, columns=column_names)
        return df
