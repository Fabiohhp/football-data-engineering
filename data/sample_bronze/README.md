# Recorded Bronze: 2025-11-01

The raw responses of one match date, as the extractors stored them, under the same keys they
have in the Bronze bucket. `python -m src.extract.seed_bronze` loads them, so Silver and Gold can
run with no API token and with no call to either source. The main
[README](../../README.md#reproduce-a-real-date) has the commands.

| Dataset | Files | Source |
|---|---|---|
| `football_data/matches` | 5, one per league | Football-Data.org |
| `football_data/teams` | 5, one per league | Football-Data.org |
| `understat/league_data` | 5, one per league | Understat |
| `understat/match_data` | 23, one per match | Understat |

Football data provided by the Football-Data.org API. Expected-goals, player and shot data come
from [Understat](https://understat.com/). The data belongs to its providers, under their terms,
and is not covered by this repository's MIT License.
