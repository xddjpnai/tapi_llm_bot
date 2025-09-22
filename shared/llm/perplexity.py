import os
import requests
import logging
from typing import List, Tuple, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Removed unused call_perplexity; using call_perplexity_with_citations only


def call_perplexity_with_citations(prompt: str) -> Tuple[str, List[Dict[str, Any]]]:
    sonar_api_key = os.getenv("SONAR_API_KEY")
    if not sonar_api_key:
        return "Perplexity API key not configured.", []
    url = "https://api.perplexity.ai/chat/completions"
    headers = {
        "Authorization": f"Bearer {sonar_api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "model": "sonar-pro",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 900,
        "return_citations": True,
    }
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=20)
        r.raise_for_status()
        j = r.json()
        content = ""
        citations: List[Dict[str, Any]] = []
        if "choices" in j and len(j["choices"]) > 0:
            msg = j["choices"][0].get("message", {})
            content = msg.get("content") or msg.get("text") or ""
            citations = msg.get("citations") or j.get("citations") or []
        return content, citations or []
    except Exception as e:
        logger.exception("Perplexity API error: %s", e)
        return f"Perplexity API error: {e}", []


