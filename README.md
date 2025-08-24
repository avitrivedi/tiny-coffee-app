# tiny-coffee-app
trying to simulate a virtual coffee shop
Entities & grain

events (one row = a user action at a time)

users (one row = a user)

We’ll compute daily metrics → daily grain (one row per date in the output)

Columns you’ll collect

events: event_date, user_id, points_earned, is_signup (0/1)

users: user_id, signup_date, preferred_plan (free/pro)

Metric definitions

DAU: distinct users with ≥1 event on that date

Weekly signups: count of is_signup=1 grouped by week

Activation: a user is “activated” in a week if their weekly points ≥ 50
