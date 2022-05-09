from pyspark import SparkContext
import sys, os

sc = SparkContext()

test = "/user/training/"+ sys.argv[1] +"/*"

data = sc.textFile(test)

arrMin = [0, 0, 0, 0, 0, 0, 0, 0]

arrMax = [0, 0, 0, 0, 0, 0, 0, 0]

for line in data.take(data.count()):

	values = line.split("\t")[1].split("||||")

	arrVal = [0, 0, 0, 0, 0, 0, 0, 0]

	for x in range(1, 9):
		arrVal[x-1] = float(values[x])

	for y in range(0, len(arrVal)):
		if (arrVal[y] < arrMin[y]):
			arrMin[y] = arrVal[y]
		if (arrVal[y] > arrMax[y]):
			arrMax[y] = arrVal[y]

total = 0
for line in data.take(data.count()):
	total = total + 1

norm = [None] * total

count = 0
for line in data.take(data.count()):

	values = line.split("\t")[1].split("||||")
	arrVal = [0, 0, 0, 0, 0, 0, 0, 0]
	for x in range(1, 9):
		arrVal[x-1] = float(values[x])

	newLine = line.split("\t")[0] + "\t" + values[0]
	for z in range(0, len(values)-1):
		newLine = newLine + "||||" + str(float(values[z+1]) / (arrMax[z] - arrMin[z]))

	norm[count] = newLine
	count = count + 1

rdd = sc.parallelize(norm)
rdd.saveAsTextFile("/user/training/normalized")


























	
