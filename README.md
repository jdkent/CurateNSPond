
## Idea

Please provide an outline for a pipeline where that has (at least) two parts.

The first is a search utility through pubmed, where folders are organized by search terms/date and within the folders is a text file containing all the pmids.

The second utility will take in a list of PMIDS and use semantic scholar/pubmed entrez/crossref/etc. to get PMCID and/or doi for every pmid in the list, store this as a jsonl file with each json entry looking like {"pmid": <value>, "pmcid": <value>, "doi": <value>} where the value can be none if is isn't found, store each jsonl in a folder using a hash of the input pmids, make this second utility flexible, if it's input with a list of dois or pmcids, try to fill out the rest of the fields as best as possible storing jsonls under a hashed folder name.

the third utility can take in one or more jsonls and deduplicate them (e.g., if one entry has doi and pmid, and another entry has the same pmid and a pmcid, then combine them into one entry). Output into another jsonl where the hash is determined by the contents of the input jsonls. leave a metadata.json showing the paths where the jsonls came from.

the fourth utility will use pubget (https://github.com/neuroquery/pubget) or ACE (https://github.com/neurosynth/ACE)
to download the full text of the papers. for all IDs in which PMCID is available, try to extract full text using pubget, for the studies that pubget could not extract (either pmcid was not open-access or pmcid did not exist), forward all pmids to ACE. Only try to scrape the html/get the full text. Do not try to extract the coordinates. If the text is retrievable, use semantic scholar/pubmed entrez (depending on availability of pmid) to retrieve title, abstract, authors, year, journal. 

The fifth utility will try to identify if the full text contains any numeric tables and uses an llm using openai API calling pattern to take in the full text and identify if those tables report (x,y,z) coordinates and map the reported coordinates to a contrast or a group, or what best described the grouping of coordinates. multiple groups of coordinates may be contained within a single table.


## Strategy

Use test driven development to build out each utility one at a time.

## Implementation

I want this to be implemented in python, using best data science practices, with type hints, docstrings, and unit tests. Use uv for project management, black for formatting, and isort for import sorting, and pytest for testing.

### Optional dependencies

The full-text fetching utility requires the `fulltext` extra, which installs [`pubget`](https://pypi.org/project/pubget/) and [`ACE`](https://github.com/neurosynth/ACE). Install with:

```
uv pip install ".[fulltext]"
```
