from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys

from .client import DEFAULT_USER_AGENT, Blocked, Client
from .pipeline import build, scrape
from .storage import store_from_env


def _sigterm(*_: object) -> None:
    # Turn runner cancellation into an exception so the current week gets checkpointed.
    raise KeyboardInterrupt


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="leystop", description=__doc__)
    ap.add_argument("--local", metavar="DIR", help="write to a local directory instead of R2")
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("scrape", help="scrape missing weeks (newest first), then rebuild tables")
    sp.add_argument("--time-budget", type=float, default=330, help="minutes before stopping cleanly (default 330)")
    sp.add_argument("--weeks", help="comma-separated semana ids to restrict to, e.g. 199,198")
    sp.add_argument("--max-weeks", type=int, help="stop after this many weeks")
    sp.add_argument("--refresh", action="store_true", help="re-scrape weeks even if already stored")
    sp.add_argument("--min-interval", type=float, default=1.5, help="seconds between request starts")

    sub.add_parser("build", help="rebuild consolidated tables and manifest from weekly partitions")

    args = ap.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    signal.signal(signal.SIGTERM, _sigterm)

    store = store_from_env(args.local)
    public_url = os.environ.get("PUBLIC_BASE_URL")

    if args.cmd == "build":
        print(json.dumps(build(store, public_url), indent=2, ensure_ascii=False))
        return 0

    weeks = {int(w) for w in args.weeks.split(",")} if args.weeks else None
    ua = os.environ.get("LEYSTOP_USER_AGENT", DEFAULT_USER_AGENT)
    exit_code, completed = 0, 0
    with Client(min_interval=args.min_interval, user_agent=ua) as client:
        try:
            completed = scrape(
                client,
                store,
                time_budget_s=args.time_budget * 60,
                only_weeks=weeks,
                refresh=args.refresh,
                max_weeks=args.max_weeks,
            )
        except Blocked as e:
            logging.error("blocked by the site's WAF (%s); stopping. Progress is checkpointed.", e)
            exit_code = 2
        except KeyboardInterrupt:
            logging.warning("interrupted; progress is checkpointed")
            exit_code = 130
        finally:
            logging.info("%d HTTP requests made", client.requests)

    if completed:
        build(store, public_url)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
