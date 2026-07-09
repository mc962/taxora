"""Taxora CLI — administrative commands.

Usage:
    python -m taxora.cli create-key --name admin --superuser
    python -m taxora.cli create-schedule --countries IS VA --interval-hours 24
    python -m taxora.cli delete-schedule
"""

import argparse
import asyncio
import sys
from datetime import timedelta

DAILY_SCHEDULE_ID = "taxora-daily-refresh"


async def _create_key(name: str, is_superuser: bool) -> None:
    from taxora.data.database.connection import sessionmanager
    from taxora.data.repository.api_key_repository import ApiKeyRepository

    async with sessionmanager.session() as db:
        repo = ApiKeyRepository(db)
        _key, plaintext = await repo.create(name=name, is_superuser=is_superuser)
        await db.commit()

    role = "superuser" if is_superuser else "regular"
    print(f"\nCreated {role} API key '{name}':")
    print(f"  {plaintext}")
    print("\nCopy it now — it cannot be retrieved again.\n")


async def _create_schedule(country_codes: list[str], interval_hours: int) -> None:
    from temporalio.client import (
        Client,
        Schedule,
        ScheduleActionStartWorkflow,
        ScheduleIntervalSpec,
        ScheduleSpec,
        ScheduleState,
    )

    from taxora.configuration.settings import application_settings
    from taxora.workflows import BatchIngestParams, BatchIngestWorkflow

    client = await Client.connect(application_settings.temporal_address)
    await client.create_schedule(
        DAILY_SCHEDULE_ID,
        Schedule(
            action=ScheduleActionStartWorkflow(
                BatchIngestWorkflow.run,
                BatchIngestParams(country_codes=country_codes),
                id=f"{DAILY_SCHEDULE_ID}-run",
                task_queue=application_settings.temporal_task_queue,
            ),
            spec=ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=timedelta(hours=interval_hours))]
            ),
            state=ScheduleState(
                note=f"Taxora recurring refresh for: {', '.join(country_codes)}"
            ),
        ),
    )
    print(
        f"\nCreated schedule '{DAILY_SCHEDULE_ID}' — refreshes "
        f"{', '.join(country_codes)} every {interval_hours}h.\n"
    )


async def _delete_schedule() -> None:
    from temporalio.client import Client

    from taxora.configuration.settings import application_settings

    client = await Client.connect(application_settings.temporal_address)
    handle = client.get_schedule_handle(DAILY_SCHEDULE_ID)
    await handle.delete()
    print(f"\nDeleted schedule '{DAILY_SCHEDULE_ID}'.\n")


def main() -> None:
    parser = argparse.ArgumentParser(prog="taxora.cli", description="Taxora admin CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    key_parser = subparsers.add_parser("create-key", help="Create a new API key")
    key_parser.add_argument("--name", required=True, help="Descriptive name for the key")
    key_parser.add_argument(
        "--superuser", action="store_true", help="Grant superuser privileges"
    )

    create_parser = subparsers.add_parser(
        "create-schedule", help="Create the recurring BatchIngestWorkflow schedule"
    )
    create_parser.add_argument(
        "--countries", nargs="+", required=True, help="Country codes to refresh, e.g. IS VA"
    )
    create_parser.add_argument(
        "--interval-hours", type=int, default=24, help="Refresh interval in hours"
    )

    subparsers.add_parser("delete-schedule", help="Delete the recurring schedule")

    args = parser.parse_args()

    if args.command == "create-key":
        asyncio.run(_create_key(name=args.name, is_superuser=args.superuser))
    elif args.command == "create-schedule":
        asyncio.run(_create_schedule(args.countries, args.interval_hours))
    elif args.command == "delete-schedule":
        asyncio.run(_delete_schedule())
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
