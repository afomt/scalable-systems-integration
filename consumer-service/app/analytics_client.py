import aiohttp
from tenacity import retry, stop_after_attempt, wait_fixed

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def send_to_analytics(data):
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "http://analytics-mock:9000/analytics/data",
            json=data
        ) as response:
            response.raise_for_status()
