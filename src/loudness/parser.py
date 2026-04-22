"""HTML and GraphQL parsing for Yelp business data."""

import html as html_module
import re
from datetime import datetime

from parsel import Selector

from .models import BusinessResult, NoiseLevel


def extract_alias_from_url(url: str) -> str:
    """Extract business alias from Yelp URL."""
    from urllib.parse import urlparse

    parsed = urlparse(url)
    path = parsed.path
    if path.startswith("/biz/"):
        alias = path[5:].split("?")[0].split("/")[0]
        return alias
    return url


def extract_biz_id(html: str) -> str | None:
    """Extract encrypted business ID from HTML meta tag."""
    selector = Selector(text=html)
    biz_id = selector.css('meta[name="yelp-biz-id"]::attr(content)').get()
    return biz_id


def extract_business_name(html: str) -> str | None:
    """Extract business name from HTML."""
    selector = Selector(text=html)

    name = selector.css('meta[property="og:title"]::attr(content)').get()
    if name:
        name = name.replace(" - Yelp", "").strip()
        return name

    name = selector.css("h1::text").get()
    if name:
        return name.strip()

    return None


def extract_noise_level_from_html(html: str) -> str | None:
    """Extract noise level from embedded JSON/scripts in HTML.

    Yelp embeds business data in script tags, which we parse to find
    the NoiseLevel attribute without needing a GraphQL call.
    """
    selector = Selector(text=html)

    for script in selector.css("script::text").getall():
        if "NoiseLevel" not in script and "noise_level" not in script:
            continue

        noise = _extract_noise_from_script(script)
        if noise:
            return noise

    noise = _extract_from_attributes_section(selector)
    if noise:
        return noise

    return None


def _extract_noise_from_script(script: str) -> str | None:
    """Extract noise level from a script tag content."""
    # Decode HTML entities (Yelp encodes JSON in HTML attributes)
    decoded = html_module.unescape(script)
    
    patterns = [
        # Format: "displayText":"Loud","alias":"NoiseLevel"
        r'"displayText"\s*:\s*"([^"]+)"\s*,\s*"alias"\s*:\s*"NoiseLevel"',
        r'"alias"\s*:\s*"NoiseLevel"[^}]*"singleValue"\s*:\s*\{[^}]*"alias"\s*:\s*"([^"]+)"',
        r'"NoiseLevel"\s*:\s*"([^"]+)"',
        r'"noiseLevel"\s*:\s*"([^"]+)"',
        r'"noise_level"\s*:\s*"([^"]+)"',
        r'"displayText"\s*:\s*"Noise Level"[^}]*"shortDisplayText"\s*:\s*"([^"]+)"',
    ]

    for pattern in patterns:
        match = re.search(pattern, decoded, re.IGNORECASE)
        if match:
            value = match.group(1).lower().replace(" ", "_")
            return _normalize_noise_level(value)

    return None


def _extract_from_attributes_section(selector: Selector) -> str | None:
    """Extract noise level from rendered attributes section."""
    noise_text = selector.css(
        '[aria-label*="Noise Level"] span::text, '
        '[data-testid*="noise"] span::text, '
        'dt:contains("Noise Level") + dd::text'
    ).get()

    if noise_text:
        return _normalize_noise_level(noise_text.strip().lower().replace(" ", "_"))

    return None


def _normalize_noise_level(value: str) -> str | None:
    """Normalize noise level value to standard enum values."""
    value = value.lower().replace(" ", "_").replace("-", "_")

    mappings = {
        "quiet": "quiet",
        "average": "average",
        "moderate": "average",
        "moderate_noise": "average",
        "loud": "loud",
        "very_loud": "very_loud",
        "veryloud": "very_loud",
        "very loud": "very_loud",
    }

    return mappings.get(value)


def parse_graphql_response(response_data: dict | list) -> dict | None:
    """Parse GraphQL batch response to extract business attributes.

    Args:
        response_data: The JSON response from /gql/batch

    Returns:
        Dict with business data or None if parsing fails
    """
    if isinstance(response_data, list):
        for item in response_data:
            result = _extract_from_gql_item(item)
            if result:
                return result
    elif isinstance(response_data, dict):
        return _extract_from_gql_item(response_data)

    return None


def _extract_from_gql_item(item: dict) -> dict | None:
    """Extract business data from a single GraphQL response item."""
    if "data" not in item:
        return None

    data = item["data"]

    business = data.get("business") or data.get("getBusiness")
    if not business:
        for _key, value in data.items():
            if isinstance(value, dict) and "name" in value:
                business = value
                break

    if not business:
        return None

    result = {
        "name": business.get("name"),
        "alias": business.get("alias"),
        "enc_biz_id": business.get("encBizId") or business.get("id"),
        "noise_level": None,
    }

    attributes = (
        business.get("authoritativeAttributes")
        or business.get("attributes")
        or business.get("businessAttributes")
    )

    if attributes:
        result["noise_level"] = _extract_noise_from_attributes(attributes)

    return result


def _extract_noise_from_attributes(attributes: list | dict) -> str | None:
    """Extract noise level from business attributes."""
    if isinstance(attributes, dict):
        noise = attributes.get("noiseLevel") or attributes.get("noise_level")
        if noise:
            return _normalize_noise_level(str(noise))
        return None

    for attr in attributes:
        if not isinstance(attr, dict):
            continue

        alias = attr.get("alias", "").lower()
        if alias == "noiselevel" or "noise" in alias:
            single_value = attr.get("singleValue", {})
            if isinstance(single_value, dict):
                value = single_value.get("alias") or single_value.get("shortDisplayText")
                if value:
                    return _normalize_noise_level(value)
            elif single_value:
                return _normalize_noise_level(str(single_value))

    return None


def parse_business_page(
    html: str, url: str, enc_biz_id: str | None = None
) -> BusinessResult:
    """Parse a Yelp business page HTML to extract business data.

    Args:
        html: The HTML content of the business page
        url: The URL that was fetched
        enc_biz_id: Pre-extracted business ID (optional)

    Returns:
        BusinessResult with extracted data
    """
    alias = extract_alias_from_url(url)
    name = extract_business_name(html)
    biz_id = enc_biz_id or extract_biz_id(html)
    noise_level_str = extract_noise_level_from_html(html)

    noise_level = None
    if noise_level_str:
        try:
            noise_level = NoiseLevel(noise_level_str)
        except ValueError:
            pass

    return BusinessResult(
        alias=alias,
        enc_biz_id=biz_id,
        name=name,
        noise_level=noise_level,
        url=url,
        scraped_at=datetime.utcnow(),
    )
