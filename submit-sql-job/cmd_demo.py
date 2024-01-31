import asyncio
import os


async def test():
    os.system("nohup ping baidu.com &")

async def main():
    for i in range(100):
        print(i)
        if i == 10:
            await test()
    print("over!")

if __name__ == '__main__':
    # asyncio.run(main())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())