
import sys

inputfile = sys.argv[1]
outputfile = sys.argv[2]

i = 0
f1=open(outputfile, 'w+')
with open(inputfile) as fp:
    for line in fp:
        s = line.split("\t")
		t = i+"\t"+s[1]+"\t"+s[2]+"\t"+s[3]+"\n"
		f1.write(t)
		i = i + 1
