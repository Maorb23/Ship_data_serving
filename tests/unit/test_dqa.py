import pandas as pd

from elt_utils.transform.dqa import Dqa


BASE_CONFIG = {
    "dqa": {
        "lat_min": -90.0,
        "lat_max": 90.0,
        "lon_min": -180.0,
        "lon_max": 180.0,
        "sog_min": 0.0,
        "sog_max": 100.0,
        "max_sog_diff": 40.0,
        "cog_min": 0.0,
        "cog_max": 360.0,
    }
}


def test_dqa_mixed_policy_rejects_critical_and_nulls_noncritical():
    delta_df = pd.DataFrame(
        [
            {
                "ship_name": "Ship_A",
                "time": 1.0,
                "lat": 10.0,
                "lon": 20.0,
                "sog": 250.0,
                "cog": 90.0,
            },
            {
                "ship_name": "Ship_B",
                "time": 2.0,
                "lat": None,
                "lon": 21.0,
                "sog": 8.0,
                "cog": 120.0,
            },
            {
                "ship_name": "",
                "time": 3.0,
                "lat": 5.0,
                "lon": 25.0,
                "sog": 10.0,
                "cog": 30.0,
            },
        ]
    )

    clean_df, rejects_df = Dqa.dqa(delta_df, BASE_CONFIG)

    assert len(clean_df) == 1
    assert len(rejects_df) == 2

    clean_row = clean_df.iloc[0]
    assert clean_row["ship_name"] == "Ship_A"
    assert pd.isna(clean_row["sog"])
    assert clean_row["cog"] == 90.0

    reasons = "|".join(rejects_df["dq_reason"].tolist())
    assert "lat_missing" in reasons
    assert "ship_name_missing" in reasons


def test_dqa_rejects_gps_zero_coordinates():
    delta_df = pd.DataFrame(
        [
            {
                "ship_name": "Ship_G",
                "time": 10.0,
                "lat": 0.0,
                "lon": 0.0,
                "sog": 8.0,
                "cog": 120.0,
            }
        ]
    )

    clean_df, rejects_df = Dqa.dqa(delta_df, BASE_CONFIG)

    assert clean_df.empty
    assert len(rejects_df) == 1
    assert "gps_zero_coordinates" in rejects_df.iloc[0]["dq_reason"]


def test_dqa_rejects_time_not_increasing_within_ship():
    delta_df = pd.DataFrame(
        [
            {
                "ship_name": "Ship_T",
                "time": 100.0,
                "lat": 1.0,
                "lon": 2.0,
                "sog": 9.0,
                "cog": 20.0,
            },
            {
                "ship_name": "Ship_T",
                "time": 100.0,
                "lat": 1.1,
                "lon": 2.1,
                "sog": 9.5,
                "cog": 25.0,
            },
        ]
    )

    clean_df, rejects_df = Dqa.dqa(delta_df, BASE_CONFIG)

    assert len(clean_df) == 1
    assert len(rejects_df) == 1
    assert "time_not_increasing" in rejects_df.iloc[0]["dq_reason"]


def test_dqa_rejects_extreme_sog_diff_within_ship():
    delta_df = pd.DataFrame(
        [
            {
                "ship_name": "Ship_S",
                "time": 1.0,
                "lat": 10.0,
                "lon": 20.0,
                "sog": 8.0,
                "cog": 120.0,
            },
            {
                "ship_name": "Ship_S",
                "time": 2.0,
                "lat": 10.1,
                "lon": 20.1,
                "sog": 58.5,
                "cog": 121.0,
            },
        ]
    )

    clean_df, rejects_df = Dqa.dqa(delta_df, BASE_CONFIG)

    assert len(clean_df) == 1
    assert len(rejects_df) == 1
    assert "sog_extreme_diff" in rejects_df.iloc[0]["dq_reason"]
