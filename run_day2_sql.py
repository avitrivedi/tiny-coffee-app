import duckdb, os, pathlib

os.makedirs("data/marts", exist_ok=True)
con = duckdb.connect("warehouse.duckdb")
con.execute("CREATE SCHEMA IF NOT EXISTS coffee;")

# 1) Load events from CSV
con.execute("DROP TABLE IF EXISTS coffee.events;")
con.execute("""
CREATE TABLE coffee.events AS
SELECT * FROM read_csv_auto('data/events.csv', header=true);
""")

# 2) Daily DAU (distinct users per day)
con.execute("DROP TABLE IF EXISTS coffee.daily_dau;")
con.execute("""
CREATE TABLE coffee.daily_dau AS
SELECT event_date, COUNT(DISTINCT user_id) AS dau
FROM coffee.events
GROUP BY event_date
ORDER BY event_date;
""")

# 3) 7-day moving average (smooth the daily noise)
con.execute("DROP TABLE IF EXISTS coffee.daily_dau_ma7;")
con.execute("""
CREATE TABLE coffee.daily_dau_ma7 AS
WITH d AS (SELECT event_date, dau FROM coffee.daily_dau)
SELECT
  event_date,
  dau,
  AVG(dau) OVER (ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS dau_ma7
FROM d
ORDER BY event_date;
""")

# 4) Weekly signups (sum of the 0/1 flag)
con.execute("DROP TABLE IF EXISTS coffee.weekly_signups;")
con.execute("""
CREATE TABLE coffee.weekly_signups AS
SELECT DATE_TRUNC('week', event_date) AS week_start,
       SUM(CASE WHEN is_signup = 1 THEN 1 ELSE 0 END) AS signups
FROM coffee.events
GROUP BY 1
ORDER BY 1;
""")

# 5) Weekly activation (users with >=50 points in the week)
con.execute("DROP TABLE IF EXISTS coffee.weekly_activation;")
con.execute("""
CREATE TABLE coffee.weekly_activation AS
WITH user_week AS (
  SELECT
    DATE_TRUNC('week', event_date) AS week_start,
    user_id,
    SUM(points_earned) AS pts
  FROM coffee.events
  GROUP BY 1,2
)
SELECT week_start, COUNT(*) AS activated_users
FROM user_week
WHERE pts >= 50
GROUP BY 1
ORDER BY 1;
""")

# 6) Export for charts
con.execute("COPY coffee.daily_dau          TO 'data/marts/daily_dau.csv'          (HEADER, DELIMITER ',');")
con.execute("COPY coffee.daily_dau_ma7      TO 'data/marts/daily_dau_ma7.csv'      (HEADER, DELIMITER ',');")
con.execute("COPY coffee.weekly_signups     TO 'data/marts/weekly_signups.csv'     (HEADER, DELIMITER ',');")
con.execute("COPY coffee.weekly_activation  TO 'data/marts/weekly_activation.csv'  (HEADER, DELIMITER ',');")

print('Done. Files written to data/marts/.')
