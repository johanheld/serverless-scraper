BASE_URL = "https://www.vinted.se/"
API_URL = "https://api.vinted.se/svc-catalogue/items?page=1&per_page=96&time={time}&search_text={search_text}&order=newest_first&global_search_session_id={session_id}&attribute_ids%5Bcatalog%5D=&attribute_ids%5Bsize%5D=&attribute_ids%5Bbrand%5D=&attribute_ids%5Bstatus%5D=&attribute_ids%5Bcolor%5D=&attribute_ids%5Bmaterial%5D="

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36"
)

BASE_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "sv,en-US;q=0.9,en;q=0.8",
    "Origin": BASE_URL.rstrip("/"),
    "Referer": BASE_URL,
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "locale": "sv-SE",
    "platform": "web",
    "x-next-app": "marketplace-web",
}