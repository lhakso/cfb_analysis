import pandas as pd
from pathlib import Path

from .api import cfbd_get

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def load_transfer_portal(year: int, use_cache: bool = True) -> pd.DataFrame:
    """
    Load transfer portal data for a given year.
    - If cached CSV exists and use_cache=True, load it.
    - Otherwise call API, save CSV, and return the DataFrame.
    """
    csv_path = RAW_DIR / f"transfer_portal_{year}.csv"

    if use_cache and csv_path.exists():
        return pd.read_csv(csv_path)

    # fetch from API
    data = cfbd_get("/player/portal", {"year": year})
    df = pd.json_normalize(data)

    # persist for future runs
    df.to_csv(csv_path, index=False)
    return df


def load_transfer_portal_multi(
    start_year: int, end_year: int, use_cache: bool = True
) -> pd.DataFrame:
    """
    Get portal data for a range of years, concatenated into one DataFrame.
    """
    dfs = []
    for year in range(start_year, end_year + 1):
        df = load_transfer_portal(year, use_cache=use_cache)
        if df.empty:
            print(f"[warn] No data for year {year}")
            continue
        df["year"] = year
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def load_team_talent(use_cache: bool = True) -> pd.DataFrame:
    csv_path = RAW_DIR / "team_talent.csv"

    if use_cache and csv_path.exists():
        return pd.read_csv(csv_path)

    data = cfbd_get("/talent", {})
    df = pd.json_normalize(data)
    df.to_csv(csv_path, index=False)
    return df


def load_team_season_stats(year: int, use_cache: bool = True) -> pd.DataFrame:
    csv_path = RAW_DIR / f"team_season_stats_{year}.csv"

    if use_cache and csv_path.exists():
        return pd.read_csv(csv_path)

    data = cfbd_get("/stats/season/team", {"year": year})
    df = pd.json_normalize(data)
    df.to_csv(csv_path, index=False)
    return df


def load_player_season_stats(year: int, use_cache: bool = True) -> pd.DataFrame:
    """
    Load aggregated player season stats for a given year.

    Uses the CFBD endpoint:
        GET /stats/player/season?year=YYYY

    - If cached CSV exists and use_cache=True, load it.
    - Otherwise call API, save CSV, and return the DataFrame.
    """
    csv_path = RAW_DIR / f"player_season_stats_{year}.csv"

    if use_cache and csv_path.exists():
        return pd.read_csv(csv_path)

    # fetch from API
    data = cfbd_get("/stats/player/season", {"year": year})
    df = pd.json_normalize(data)

    # persist for future runs
    df.to_csv(csv_path, index=False)
    return df


def load_player_season_stats_multi(
    start_year: int, end_year: int, use_cache: bool = True
) -> pd.DataFrame:
    """
    Get player season stats for a range of years, concatenated into one DataFrame.

    Adds a 'year' column so you can easily do before/after transfer comparisons.
    """
    dfs = []
    for year in range(start_year, end_year + 1):
        df = load_player_season_stats(year, use_cache=use_cache)
        if df.empty:
            print(f"[warn] No player season stats for year {year}")
            continue
        # some responses already include 'year', but this keeps it explicit
        if "year" not in df.columns:
            df["year"] = year
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def load_sp_plus_ratings(year: int, use_cache: bool = True) -> pd.DataFrame:
    """
    Load SP+ ratings for a given year.
    """
    csv_path = RAW_DIR / f"sp_plus_{year}.csv"

    if use_cache and csv_path.exists():
        return pd.read_csv(csv_path)

    data = cfbd_get("/ratings/sp", {"year": year})
    df = pd.json_normalize(data)
    df.to_csv(csv_path, index=False)
    return df


def load_sp_plus_multi(start_year: int, end_year: int, use_cache: bool = True) -> pd.DataFrame:
    """
    Get SP+ ratings for a range of years, concatenated into one DataFrame.
    """
    dfs = []
    for year in range(start_year, end_year + 1):
        df = load_sp_plus_ratings(year, use_cache=use_cache)
        if df.empty:
            print(f"[warn] No SP+ data for year {year}")
            continue
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def load_team_records(year: int, use_cache: bool = True) -> pd.DataFrame:
    """
    Load team win-loss records for a given year.
    """
    csv_path = RAW_DIR / f"team_records_{year}.csv"

    if use_cache and csv_path.exists():
        return pd.read_csv(csv_path)

    data = cfbd_get("/records", {"year": year})
    df = pd.json_normalize(data)
    df.to_csv(csv_path, index=False)
    return df


def attach_player_ids(df_transfers):
    results = []

    for _, row in df_transfers.iterrows():
        name = f"{row.firstName} {row.lastName}"
        search = cfbd_get("/player/search", {"search": name})

        if not search:
            row["playerId"] = None
            results.append(row)
            continue

        candidates = pd.json_normalize(search)

        # Try matching by origin team if possible
        origin = str(row.origin).lower()

        if origin != "nan":
            match = candidates[candidates["team"].str.lower() == origin]
            if not match.empty:
                row["playerId"] = match.iloc[0]["id"]
                results.append(row)
                continue

        # fallback: first result
        row["playerId"] = candidates.iloc[0]["id"]
        results.append(row)

    return pd.DataFrame(results)
