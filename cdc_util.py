import argparse
import asyncio
import collections
import datetime
import json
import os.path

from aiosfstream import SalesforceStreamingClient, ReplayMarker
from terminaltables import AsciiTable

args = {}

async def stream_events():
    path = os.path.expanduser(f"~/.private/{args.credentials}.json")
    creds_string = open(path).read()
    creds_js = json.loads(creds_string)
    streams = sorted(creds_js["streams"])

    replay_id = args.replay_id - 1 if args.replay_id > 0 else args.replay_id
    print(f"Using replay_id: {replay_id}")
    replay_marker = ReplayMarker(replay_id=replay_id, date="")
    replay = {}
    for stream in streams:
        replay[stream] = replay_marker
    starttime = datetime.datetime.now()

    async with SalesforceStreamingClient(
            consumer_key=creds_js["consumer_key"],
            consumer_secret=creds_js["consumer_secret"],
            username=creds_js["username"],
            password=creds_js["password"],
            replay=replay) as client:

        # subscribe to topics
        for stream in streams:
            print(f"Connecting to: {stream}")
            await client.subscribe(stream)
            print(f"Connected to: {stream}")

        # listen for incoming messages
        count = 0
        last_table_data = None
        period_start = datetime.datetime.now()
        period_count = 1
        changes = collections.defaultdict(int)
        async for message in client:
            if args.limit != -1 and count >= args.limit:
                break
            count += 1
            topic = message["channel"]
            data = message["data"]
            changes[topic] += 1
            if args.monitor:
                if count % args.number == 0:
                    period_count += 1
                    table_data = [["stream", "replay_id", "changes"]]
                    for stream in streams:
                        marker = replay[stream]
                        last_replay_id = last_table_data[len(table_data)][1] if last_table_data else -1
                        table_data.append([stream, marker.replay_id, changes[stream]])
                    table = AsciiTable(table_data)
                    last_table_data = table_data
                    print("\033[2J")
                    print("\033[0;0H")
                    period_end = datetime.datetime.now()
                    uptime = period_end - starttime
                    period_time = period_end - period_start
                    period_start = period_end
                    print(f"Uptime: {uptime}, Period #{period_count}, Period time: {period_time}")
                    print(table.table)
                    changes = collections.defaultdict(int)
            else:
                js = json.dumps(data, indent=4)
                print(f"{topic}: {js}")
                print(f"replay: {replay}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--credentials", type=str, help="name of file in ~/.private directory containin login info and list of streams to connect to")
    parser.add_argument("-m", "--monitor", help="monitor number of CDC events received", action="store_true")
    parser.add_argument("-r", "--replay_id", type=int, default=-1, help="first  replay id to fetch")
    parser.add_argument("-l", "--limit", type=int, default=-1, help="number of events to show (-1 is infinite)")
    parser.add_argument("-n", "--number", type=int, default=1, help="number of events before refreshing monitor table")
    parser.add_argument("-s", "--seconds", type=int, default=1, help="number of seconds before refreshing monitor table")
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(stream_events())

