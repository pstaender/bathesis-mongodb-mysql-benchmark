Wikipedia Benchmark
===================

A java console application to make a performance test with wikipedia on MySQL and MongoDB.

Requirements
------------

You have to use the [Wikipedia2MongoDB dumper](https://github.com/philipp1982/wikipedia2mongodbd) (it's a modified version of the [mwdumper][1] which is used to import XML-dumps to MySQL) to import the wikipedia to MySQL and to MongoDB. The version of the db's ain't important.

Run
---

After generating a jar, simple run with

	java -jar DatabasePerformance.jar -h
	
to get a list of all available parameters:

* `-v` verbose output (shows sql / mongodb statements)
* `--mongodb` search in MongoDB
* `--mysql` search in MySQL
* `--regex` serach with regular expressions
* `--exact` or search for the exact term
* `-l100` search recursive for more articles
* `-tCoca_Cola` search for `Coca Cola` in title
* `-sHistory` search for `History` in subtitle
* `-cAlfred_Hitchcock` search for `Alfred Hitchcock` in content
* `--silent` only display the time measures
* `--formatCSV` output format, if you need the result to be processed

### Translation

For now everything is in german, sorry... but feel free to translate :)

[1]: http://svn.wikimedia.org/svnroot/mediawiki/trunk/mwdumper/