# CircleDB

My attempts at understanding how immutable (datalog) databases work and
implementing them by working through 
[this book chapter](https://aosabook.org/en/500L/an-archaeology-inspired-database.html). 
The goal of this project is a "complete" db with high quality code and tests, as
well as a strong understanding of how it works.

## Attempts
### Attempt 1
Read through the article. High-level understanding of how it works. Stuck at the
`add-entity` function. No idea how the query engine works. Code barely works,
db transactions work, query engine is still semi-broken.

### Attempt 2
Added tests for the core db functions to understand how they work better. Got a
decent understanding of it now, and found some edge cases. Might change the core
apis to match xtdb's since that's what I'm most familiar with. Now reading more
on the transaction macros. Still no idea how the query engine works.
