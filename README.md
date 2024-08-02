# deltaman
<img src="images/deltaman.png" alt="icon" width="150"/>

A python library for batch quantitative analysis of JSON payloads (and their deltas).

## Installation

```sh
pip install .
deltaman --help
```

## Usecases
1. You have 2 directories of JSON payloads (let's say `dir1/` and `dir2/`), and you want to compare the 2 directories.

Choice 1: through cli

```sh
deltaman collections dir1/ dir2/
```

Choice 2: through python

```sh
from deltaman.samplecollection import JSONSampleCollection
sc = JSONSampleCollection.from_directory("dir1/")
sc2 = JSONSampleCollection.from_directory("dir2/")
sc.diff(sc2)
```

Both snippets above will produce the diff between the two JSON payload directories and output the response in a JSON format.