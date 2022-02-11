# Parser for Wikipedia XML Dumps

A small tool for parsing Wikipedia XML dumps into rows of page ID, page title and raw page text.

A fork of [TaherAmlaki/ParsingWikipediaXml](https://github.com/TaherAmlaki/ParsingWikipediaXml)

Example output:
```JSON
{"id": 12, "title": "Anarchism", "text": "{{short description|Political philosophy and movement ..."}
{"id": 25, "title": "Autism", "text": "{{Short description|Neurodevelopmental disorder involving social communication difficulties and repetitive behavior ..."}
{"id": 39, "title": "Albedo", "text": "{{Short description|Ratio of reflected radiation to incident radiation ..."}
```

You may have noticed that many Page ID's are missing in the ouput. Those pages are redirects and are filtered out. 

Perhaps you have also noticed that the text consists of some gibberish. This is MediaWiki's text markup which this parser is not attempting to clean. You may use the markup to automatically compile datasets for machine learning applications. For cleaning the markup you may use [mwparserfromhell](https://github.com/earwig/mwparserfromhell) or [other parsers](https://www.mediawiki.org/wiki/Alternative_parsers). You could also use [WikiExtractor](https://github.com/attardi/wikiextractor) to parse Wikipedia XML dumps into clean text.

You can deduce a page URL from the page ID by the following schema: `https://en.wikipedia.org/?curid={ID}`


# Getting started
Prerequisites: 
* Python>=3.x 

For the instruction below, I assume you are using Bash and hope you have fast internet.

1. Download the you Wikipedia dump of choice. You can download English Wikipedia dumps from [here](https://dumps.wikimedia.org/enwiki/), German Wikipedia dumps from [here](https://dumps.wikimedia.org/dewiki/) and so on. As an example, we use the latest Englisch Wikipedia dump (approx. 18 GB compressed and 80 GB unzipped, but there is no need to unzip it). Multistream is said to be faster, but I have never measrued it.

    ```bash
    wget -c "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles-multistream.xml.bz2"
    ```
    Don't try to open the whole file! It won't fit into memory. The parser will stream through it.
    
    You can have a quick look at the head of the file with:
    ```bash
    bzcat enwiki-latest-pages-articles.xml.bz2 | head -n 50
    ```

2. Run the parser (change the paths depending on your setup):
    ```bash
    python -m parse_wikipedia_dump --dump_path "./../enwiki-latest-pages-articles-multistream.xml.bz2" --outfile "./parsed_wikipedia_dump.json" --nbr_outfiles 1
    ```
    Again the file (or files) is too large to load into memory. Thus, we will just have a quick look at the first lines:
    ```bash
    head -n 3 parsed_wikipedia_dump.json > head.txt
    ```

