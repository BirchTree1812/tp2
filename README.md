# README

## Proof of success

![1st Screenshot](<images/Screenshot from 2025-11-06 16-51-05.png>)

This is the proof of the 3 queries' outputs.

![2nd Screenshot](<images/Screenshot from 2025-11-09 11-50-01.png>)

This is the proof of the constraints on the database.

![3rd Screenshot](<images/Screenshot from 2025-11-09 13-24-12.png>)

This is the proof that the `curl "http://localhost:8000/health"` command works.

## Recommendation strategy:

I would add the item-based collaborative filtering strategy, since it's a straightforward, yet efficient method. If customers tend to interact with products A and B, then the customer who is looking for A will be also recommended B. I could extend this strategy further by including behaviors into the recommendation algorithm, such as views and clicks.

## Improvements for production-readiness

One improvement to production would be running the ETL on schedule, which would save time compared to manually running it. It should also output logs about nodes/edges that it created and error alerts each time it's run.
Another improvement would be API hardening. This means protecting the recommendations data from third parties by authentication methods like API keys.
