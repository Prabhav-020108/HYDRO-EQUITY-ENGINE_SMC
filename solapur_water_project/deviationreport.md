Person: C | N2 | Part 3
Bible says: "poll /mobile/alerts every 5 seconds so new acknowledged alerts appear without manual refresh"
I did: Added a _firstPoll flag that suppresses the "New task assigned" toast on the very first load. Without this flag, every alert present at login would trigger the toast immediately, which would be confusing. The polling behaviour after first load is exactly as specified.
Files changed outside my column: No.
Impact on A/B: None. No endpoints changed, no shared files touched.
Status: Done.



