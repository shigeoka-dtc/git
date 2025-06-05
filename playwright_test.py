import asyncio
from playwright.async_api import async_playwright

async def run_test():
    async with async_playwright() as p:
        print("Launching Chromium...")
        browser = await p.chromium.launch()
        print("Creating a new page...")
        page = await browser.new_page()
        print("Navigating to example.com...")
        await page.goto("http://example.com")
        print(f"Page title: {await page.title()}")
        print("Closing browser...")
        await browser.close()
        print("Test finished successfully.")

if __name__ == "__main__":
    asyncio.run(run_test())