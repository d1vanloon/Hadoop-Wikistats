# Hadoop-Wikistats
Project 1 for CS5621 at the University of Minnesota, Duluth

## Deadlines
* Basic project structure in place by 2/16
* ~~Confirm data access by 2/13~~
* ~~Finalize job interfaces by 2/13 (see below)~~

## Architecture
Two MapReduce jobs:
* Job 1: Identify largest spike for each page (David, Eric)
* Job 2: Identify N pages from the top M languages with the largest spikes (Bai, Stephen)

## Interfaces

<table>
<tr><td>Job</td><td>Output of Map (Input of Reduce)</td><td>Output of Job (Input of Next Job)</td></tr>
<tr>
    <td>1</td>
    <td>Key: language + page
        <br/>The language is a two-character string. The page name is a string of characters.
        <br/>The language and page are separated by a space.
        <br/>Example: "en Main_Page"
        <br/>Value: date + hour + pageviews
        <br/>The date is an 8-character string. The hour is a two-character string. Pageviews is a string of characters.
        <br/>The date, hour, and pageviews are separated by spaces.
        <br/>Example: "20140601 00 156"</td>
    <td>Key: language + page<br/>Value: spike:
        <br/>spike will be in form of "<StartDateofSpike> <endDateOfSpike> <valueOfSpike>" </td>
    </tr>
<tr><td>2</td><td>Key: language<br/>Value: page + spike</td><td>TBD</td></tr>
</table>

## Data Access
Shared WikiStats data location:

    /lustre/vanlo013/project1/data/wikistats/pageviews/dumps.wikimedia.org/other/pagecounts-raw/2014

All data from June-August 2014 is in subfolders (e.g. 2014-06 to 2014-08). All files from WikiStats are included (both pagecounts and projectcounts data). I've added permissions for the group to traverse the directories and read the data and can add additional permissions as required. Email me at <vanlo013@d.umn.edu> with questions.
