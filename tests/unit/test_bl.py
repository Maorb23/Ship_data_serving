import math

import pandas as pd

from bl.bl import Bl


def test_calc_enrichments_uses_lookback_and_wraparound_rot():
    lookback_df = pd.DataFrame(
        [
            {
                "ship_name": "Ship_A",
                "time": 1.0,
                "lat": 0.0,
                "lon": 0.0,
                "sog": 10.0,
                "cog": 350.0,
            }
        ]
    )

    clean_df = pd.DataFrame(
        [
            {
                "ship_name": "Ship_A",
                "time": 2.0,
                "lat": 0.0,
                "lon": 1.0,
                "sog": 12.0,
                "cog": 10.0,
            },
            {
                "ship_name": "Ship_A",
                "time": 3.0,
                "lat": 0.0,
                "lon": 2.0,
                "sog": 13.0,
                "cog": None,
            },
        ]
    )

    enriched = Bl.calc_enrichments(clean_df, lookback_df)

    assert len(enriched) == 2

    first = enriched.iloc[0]
    assert math.isclose(first["rot"], 20.0, rel_tol=1e-6)
    assert math.isclose(first["acceleration"], 2.0, rel_tol=1e-6)
    assert math.isclose(first["distance_traveled"], 60.04, rel_tol=0.02)

    second = enriched.iloc[1]
    assert pd.isna(second["rot"])
    assert math.isclose(second["acceleration"], 1.0, rel_tol=1e-6)
    assert math.isclose(second["distance_traveled"], 60.04, rel_tol=0.02)
