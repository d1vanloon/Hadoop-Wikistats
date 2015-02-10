# Hadoop-Wikistats
Project 1 for CS5621 at the University of Minnesota, Duluth

## Deadlines
* Confirm data access by 2/13
* Finalize job interfaces by 2/13 (see below)
* Basic project structure in place by 2/16

## Architecture
Two MapReduce jobs:
* Job 1: Identify largest spike for each page (David, Eric)
* Job 2: Identify N pages from the top M languages with the largest spikes (Bai, Stephen)

## Interfaces

<table>
<tr><td>Job</td><td>Output of Map (Input of Reduce)</td><td>Output of Job (Input of Next Job)</td></tr>
<tr><td>1</td><td>Key: language + page<br/>Value: date + hour + pageviews</td><td>Key: language + page<br/>Value: spike</td></tr>
<tr><td>2</td><td>Key: language<br/>Value: page + spike</td><td>TBD</td></tr>
</table>

## Data Access
Shared WikiStats data location:
  /lustre/wikistats/...
