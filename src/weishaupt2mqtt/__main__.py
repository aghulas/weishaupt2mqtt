"""CLI entry point for weishaupt2mqtt."""
from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from .bridge import run_forever
from .config import Config


def main() -> None:
    parser = argparse.ArgumentParser(description="Weishaupt eBUS to functional MQTT bridge")
    parser.add_argument("--options", type=Path, default=None, help="Path to JSON options file (e.g. /data/options.json)")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = Config.from_sources(args.options)
    try:
        asyncio.run(run_forever(config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
