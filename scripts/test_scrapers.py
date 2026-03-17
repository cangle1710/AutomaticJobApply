import asyncio
from applypilot.discovery.orchestrator import run_scrapers

async def main():
    jobs = await run_scrapers(
        queries=[
            {"query": "backend engineer", "location": "remote", "days_old": 7},
            {"query": "software engineer", "location": "New York, NY"},
        ],
        sources=["indeed"],          # or ["indeed", "linkedin", "hiring_cafe"]
        # proxy="http://user:pass@host:port",  # optional
    )

    for job in jobs:
        print(f"[{job.source}] {job.title} @ {job.company} — {job.location}")
        print(f"  {job.url}\n")

    print(f"\nTotal: {len(jobs)} jobs")

asyncio.run(main())