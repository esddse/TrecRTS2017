# PKUICST at TREC 2017 Real-Time Summarization Track

TREC RTS homepage: [http://trecrts.github.io/](http://trecrts.github.io/).

## Result
* **Scenario A**: 8/41(mobile), 6/41(NIST) 
* **Scenario B**: 2/41

## Source Code

* Codes for Senario A are in directory [A](A).
* Codes for Senario B are in directory [B](B). 
* The Twitter stream listening and preliminary filtering codes are in [A/tool/twitter-tools/twitter-tools-core/src/main/python/twittertools/stream](A/tool/twitter-tools/twitter-tools-core/src/main/python/twittertools/stream).

More detailed informations are in the directories.

## Build

We built our 3 runs on 3 different servers.

For Scenario A, each server runs 3 python scripts:
* **a twitter tool**: fetching tweets and serve as the filtering module.
* **a "RUN_*.py" script**: the judging module.
* **a "SUBMIT.py" script**: the submitting module.

## Paper

The LaTeX source codes are in directory [paper_trec17](paper_trec17), and you can read the [PDF version](paper_trec17/trec2017.pdf)