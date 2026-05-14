# Splector

I needed a way to gather job updates from thousands of government portals every single day. I don't have an enterprise budget, I don't have a team, and I really don't want to pay for a server.

Hence this project.

This isn't a standard, heavy-duty scraping cluster. It’s a quiet, zero-cost, serverless data pipeline. It is designed to be highly concurrent, fault-tolerant, and completely invisible, running entirely on free-tier infrastructure.

## The Blueprint

The goal is simple: extract clean, structured data from messy, rate-limited websites without spending a penny.

My CRON server is Github Actions, proxy is handled by Cloudflare Workers, database is managed at Turso and my colleagues are *Antigravity* and *Codex*.

## Current Status

**Work in Progress.** Just getting started. Truly I'm not going to push 100 files 10 times a day just because AI is with me, I want to do things on my own with the certainty of an Intelligent Tool is there to back me up.

I'm just building a massive pipeline the only way I know how: with absolute resourcefulness. If you are here looking for a massive Kubernetes cluster, you are in the wrong place. If you want to see how to duct-tape free tools into a real system, stick around.
