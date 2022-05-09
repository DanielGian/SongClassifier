from pyspark import SparkContext
import sys

sc = SparkContext()

test = "/user/training/"+ sys.argv[1] +"/*"
test2 = "/user/training/"+ sys.argv[2] +"/*"

data = sc.textFile(test)
data2 = sc.textFile(test2)

count = 0
arrVal = [0, 0, 0, 0, 0, 0, 0, 0]
arrName = ["", "", "", "", "", "", "", ""]
for line in data.take(data.count()):
	name = line.split(",")[0].split("\'")[1]
	val = line.split(",")[1].split("]")[0]
	arrVal[count] = float(val)
	arrName[count] = name
	count = count + 1

arrWeight = {}
tempWeight = {}
initWeight = 100.0
for z in range(0, len(arrVal)):
	arrWeight[arrName[z]] = initWeight
	tempWeight[arrName[z]] = initWeight
	initWeight = initWeight - 10.0

arrMin = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
arrMax = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
for line in data2.take(data2.count()):
	values = line.split("\t")[1].split("||||")
	atts = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

	for x in range(1, 9):
		atts[x-1] = float(values[x])
	for y in range(0, len(atts)):
		if (atts[y] < arrMin[y]):
			arrMin[y] = atts[y]
		if (atts[y] > arrMax[y]):
			arrMax[y] = atts[y]

loops = 0
prevScore = 0.0
while (loops < 3):
	for i in range(0, len(arrVal)):
		j = arrWeight[arrName[i]]
		while (j > 0):
			arrWeight[arrName[i]] = float(j) / 1.0
			prevTotal = 0
			prevRank = 0
			score = 0.0
			scoreHope = 0.0
			for line in data2.take(data2.count()):
				total = 0
				rank = float(line.split("\t")[0])
				temp = line.split("\t")[1].split("||||")
				del temp[0]
				arrChar = {}
				arrChar["danceImpact"] = float(temp[0])
				arrChar["energyImpact"] = float(temp[1])
				arrChar["loudnessImpact"] = float(temp[2])
				arrChar["speechinessImpact"] = float(temp[3])
				arrChar["acousticnessImpact"] = float(temp[4])
				arrChar["livnessImpact"] = float(temp[5])
				arrChar["valenceImpact"] = float(temp[6])
				arrChar["tempoImpact"] = float(temp[7])
				for x in range(0, len(arrWeight)):
					total = total + arrChar[arrName[x]] * arrWeight[arrName[x]]
				if ((total > prevTotal and rank > prevRank) or (total < prevTotal and rank < prevRank)):
					score = score + (rank - prevRank)
				if (rank != prevRank):
					scoreHope = scoreHope + (rank - prevRank)
				prevTotal = total
				prevRank = rank
			if (score > prevScore):
				prevScore = score
				tempWeight[arrName[i]] = float(j)
			j = j - 1
		arrWeight[arrName[i]] = tempWeight[arrName[i]]
	loops = loops + 1

weights = [[prevScore, scoreHope], [arrName[0], arrWeight[arrName[0]]], [arrName[1], arrWeight[arrName[1]]], [arrName[2], arrWeight[arrName[2]]], [arrName[3], arrWeight[arrName[3]]], [arrName[4], arrWeight[arrName[4]]], [arrName[5], arrWeight[arrName[5]]], [arrName[6], arrWeight[arrName[6]]], [arrName[7], arrWeight[arrName[7]]]]

rdd = sc.parallelize(weights)

rdd.saveAsTextFile("/user/training/weights")


















