"""
geo.py - Geo operations benchmark runner

Handles: GEOADD, GEORADIUS, GEODIST, GEOPOS, GEOHASH, GEOSEARCH
Tool: redis-py

Use cases: Location services, store locators, delivery tracking, geofencing
"""

import random
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))
from workload_loader import CommandCategory, WorkloadConfig

from runners.base import BenchmarkResult, register_runner
from runners.redis_py_base import RedisPyRunner


@register_runner(CommandCategory.GEO)
class GeoRunner(RedisPyRunner):
    """Geo operations runner using redis-py.

    Supports:
    - GEOADD: Add locations (longitude, latitude, member)
    - GEORADIUS: Query locations within radius
    - GEODIST: Distance between two members
    - GEOPOS: Get position of member
    - GEOHASH: Get geohash of member
    - GEOSEARCH: Advanced geo queries (Redis 6.2+)
    """

    name = "geo"
    version = "1.0.0"
    category = CommandCategory.GEO

    def setup_data(self, client: Any, workload: WorkloadConfig, count: int) -> None:
        """Pre-populate geo indexes with locations."""
        points_per_index = workload.geo.points_per_index
        initial_points = min(1000, points_per_index)

        for i in range(min(count, 100)):
            key = self._generate_key(workload, i)

            # Generate random points (simple random distribution)
            pipe = client.pipeline(transaction=False)
            for j in range(initial_points):
                # Random coordinates (roughly covering continental US)
                lon = random.uniform(-125, -70)
                lat = random.uniform(25, 50)
                member = f"location:{j}"
                pipe.geoadd(key, (lon, lat, member))

                if (j + 1) % 100 == 0:
                    pipe.execute()
                    pipe = client.pipeline(transaction=False)

            pipe.execute()

    def get_operations(
        self,
        workload: WorkloadConfig,
    ) -> dict[str, Callable[[Any, str, int], None]]:
        """Get geo operation functions."""
        points_per_index = workload.geo.points_per_index
        radius_km = workload.geo.radius_km

        def geoadd(client: Any, key: str, index: int) -> None:
            lon = random.uniform(-125, -70)
            lat = random.uniform(25, 50)
            member = f"location:{index % points_per_index}"
            client.geoadd(key, (lon, lat, member))

        def georadius(client: Any, key: str, index: int) -> None:
            # Query from random center point
            lon = random.uniform(-125, -70)
            lat = random.uniform(25, 50)
            # Note: GEORADIUS is deprecated in Redis 6.2+ in favor of GEOSEARCH
            # but we use it for broader compatibility
            client.georadius(key, lon, lat, radius_km, unit="km", count=10)

        def geodist(client: Any, key: str, index: int) -> None:
            member1 = f"location:{random.randint(0, points_per_index - 1)}"
            member2 = f"location:{random.randint(0, points_per_index - 1)}"
            client.geodist(key, member1, member2, unit="km")

        def geopos(client: Any, key: str, index: int) -> None:
            member = f"location:{random.randint(0, points_per_index - 1)}"
            client.geopos(key, member)

        def geohash(client: Any, key: str, index: int) -> None:
            member = f"location:{random.randint(0, points_per_index - 1)}"
            client.geohash(key, member)

        def geosearch(client: Any, key: str, index: int) -> None:
            # GEOSEARCH (Redis 6.2+) - search by radius from random coordinates
            lon = random.uniform(-125, -70)
            lat = random.uniform(25, 50)
            try:
                client.geosearch(
                    key,
                    longitude=lon,
                    latitude=lat,
                    radius=radius_km,
                    unit="km",
                    count=10,
                )
            except AttributeError:
                # Fallback to GEORADIUS for older Redis versions
                client.georadius(key, lon, lat, radius_km, unit="km", count=10)
            except Exception:
                # GEOSEARCH may not be supported
                pass

        return {
            "GEOADD": geoadd,
            "GEORADIUS": georadius,
            "GEODIST": geodist,
            "GEOPOS": geopos,
            "GEOHASH": geohash,
            "GEOSEARCH": geosearch,
        }

    def run(
        self,
        workload: WorkloadConfig,
        output_dir: Path,
        requests: int = 100000,
        warmup: bool = True,
    ) -> BenchmarkResult:
        """Run the benchmark."""
        return super().run(workload, output_dir, requests, warmup)


def main():
    """Test the geo runner."""
    runner = GeoRunner()

    print(f"Runner: {runner.name} v{runner.version}")
    print(f"Available: {runner.is_available()}")

    if runner.is_available():
        from workload_loader import GeoConfig, KeysConfig, WorkloadConfig

        workload = WorkloadConfig(
            name="test-geo",
            commands={"GEOADD": 20, "GEOSEARCH": 60, "GEODIST": 20},
            keys=KeysConfig(space_size=10, prefix="geo:"),
            geo=GeoConfig(points_per_index=100000, radius_km=10.0),
        )

        ops = runner.get_operations(workload)
        print(f"\nWorkload: {workload.name}")
        print(f"Points per index: {workload.geo.points_per_index}")
        print(f"Radius: {workload.geo.radius_km} km")
        print(f"Implemented operations: {list(ops.keys())}")


if __name__ == "__main__":
    main()
