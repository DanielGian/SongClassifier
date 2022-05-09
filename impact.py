from pyspark import SparkContext
import sys

sc = SparkContext()

test = "/user/training/"+ sys.argv[1] +"/*"

data = sc.textFile(test)

danceImpact = 0
enImpact = 0
loudImpact = 0
speechImpact = 0
acoustImpact = 0
liveImpact = 0
valenceImpact = 0
tempoImpact = 0

prevRank = 1
prevDance = 0  
prevEn = 0
prevLoud = 0 
prevSpeech = 0
prevAcoust = 0
prevLive = 0
prevValence = 0
prevTempo = 0

for line in data.take(data.count()):

    rank = float(line.split("\t")[0])
    dance = float(line.split("\t")[1].split("||||")[1])
    energy = float(line.split("\t")[1].split("||||")[2])
    loud = float(line.split("\t")[1].split("||||")[3])
    speech = float(line.split("\t")[1].split("||||")[4])
    acoust = float(line.split("\t")[1].split("||||")[5])
    live = float(line.split("\t")[1].split("||||")[6])
    valence = float(line.split("\t")[1].split("||||")[7])
    tempo = float(line.split("\t")[1].split("||||")[8])

    if (rank > prevRank):	
        if (dance > prevDance):
            danceImpact = danceImpact + (rank - prevRank)
        if (energy > prevEn):
            enImpact = enImpact + (rank - prevRank)
        if (loud > prevLoud):
            loudImpact = loudImpact + (rank - prevRank)
        if (speech > prevSpeech):
            speechImpact = speechImpact + (rank - prevRank)
        if (acoust > prevAcoust):
            acoustImpact = acoustImpact + (rank - prevRank)
        if (live > prevLive):
            liveImpact = liveImpact + (rank - prevRank)
        if (valence > prevValence):
            valenceImpact = valenceImpact + (rank - prevRank)
        if (tempo > prevTempo):
            tempoImpact = tempoImpact + (rank - prevRank)
    '''else:
        if (dance < prevDance):
            danceImpact = danceImpact + abs(rank - prevRank)
        if (energy < prevEn):
            enImpact = enImpact + abs(rank - prevRank)
        if (loud < prevLoud):
            loudImpact = loudImpact + abs(rank - prevRank)
        if (speech < prevSpeech):
            speechImpact = speechImpact + abs(rank - prevRank)
        if (acoust < prevAcoust):
            acoustImpact = acoustImpact + abs(rank - prevRank)
        if (live < prevLive):
            liveImpact = liveImpact + abs(rank - prevRank)
        if (valence < prevValence):
            valenceImpact = valenceImpact + abs(rank - prevRank)
        if (tempo < prevTempo):
            tempoImpact = tempoImpact + abs(rank - prevRank)'''
    prevRank = rank
    prevDance = dance
    prevEn = energy
    prevLoud = loud
    prevSpeech = speech
    prevAcoust = acoust
    prevLive = live
    prevValence = valence
    prevTempo = tempo

def secVal(e):
	return e[1]

impactArray = [ ["danceImpact", danceImpact], ["energyImpact", enImpact], ["loudnessImpact", loudImpact], ["speechinessImpact", speechImpact], ["acousticnessImpact", acoustImpact], ["livnessImpact", liveImpact], ["valenceImpact", valenceImpact], ["tempoImpact", tempoImpact]]

impactArray.sort(key=secVal,reverse=True)

rdd = sc.parallelize(impactArray)

rdd.saveAsTextFile("/user/training/output")



    


