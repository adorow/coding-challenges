Redis(-light) server solution in Rust
=====================================

## Overview

This is my solution to the ["Build your own Redis Server" coding challenge](https://codingchallenges.fyi/challenges/challenge-redis/).

I expanded a little bit over the basic requirements. I implemented the following commands:
- PING
- GET
- SET (with optional EX for expiry)
- DEL
- EXISTS
- MGET
- MSET (with optional EX for expiry)
- TTL

## Improvement checklist

Still learning the language, so there's a lot of suboptimal code.
Not everything needs to be super optimal, but there's some obvious things that merit improvement once I learn more/know better:

- Testing time-based functionality (TTL/expiry)
- Understand and make better usage of lifetimes
- Make more efficient parsing of input (avoid a bunch of expensive 'clone()' calls)
- Learn and apply better Rust concurrency controls
- Make better interfaces (e.g. use &str instead of String or &String when possible) - requires better understanding of the language
