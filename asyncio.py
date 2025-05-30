import asyncio
import time

async def get_snack():
    """Simulates Teddy going to get a snack."""
    print(f"[{time.perf_counter():.1f}s] Teddy: Heading out to get snack… 🍪")
    # pretend it takes 2 seconds to get the snack
    await asyncio.sleep(2)
    print(f"[{time.perf_counter():.1f}s] Teddy: Snack is here! 🍪")
    return "🍪"

async def play_blocks():
    """Simulates you playing with blocks while waiting."""
    print(f"[{time.perf_counter():.1f}s] You: Start building blocks… 🧱")
    # build tower in three steps
    for i in range(1, 4):
        await asyncio.sleep(0.7)   # each layer takes 0.7s
        print(f"[{time.perf_counter():.1f}s] You: Placed layer {i} 🏰")
    return "🏰"

async def main():
    start = time.perf_counter()
    # 1) Kick off both tasks simultaneously:
    snack_task  = asyncio.create_task(get_snack())
    blocks_task = asyncio.create_task(play_blocks())

    print(f"[{time.perf_counter()-start:.1f}s] Main: Tasks started, now doing other stuff…")

    # 2) Wait for both to finish (you could also await them one by one)
    done, pending = await asyncio.wait(
        {snack_task, blocks_task},
        return_when=asyncio.ALL_COMPLETED
    )

    # 3) Collect results
    snack, tower = [t.result() for t in done]
    elapsed = time.perf_counter() - start
    print(f"[{elapsed:.1f}s] Main: All done!  Got {snack} and {tower} 🎉")

if __name__ == "__main__":
    asyncio.run(main())
