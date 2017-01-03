# extract_wikilinks

This is a MapReduce Hadoop program which helps in creating an adjacency graph based on the wikilinks present in different pages of Wikipedia. It takes the input in the form of an XML, parses it and extracts the wikilinks. Facetious wikilinks (wikilinks without actual pages) are removed and the adjacency graph is created.
