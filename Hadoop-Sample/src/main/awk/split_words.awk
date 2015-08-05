#!/usr/bin/awk -f
# Splits lines into words
 {
	for(i=1; i<=NF; i++) {
		print $i
	}
}
