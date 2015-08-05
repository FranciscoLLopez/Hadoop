#!/usr/bin/awk -f
# Counts the number of words. Words comes sorted.
BEGIN {
	FS = "\t"
	getline
	word = $1
	count = 1
}
{
	if (word != $0) {
		print word, count
		word = $0
		count = 1
	} else {
		count ++
	}
}
END {
		print word, count
}
