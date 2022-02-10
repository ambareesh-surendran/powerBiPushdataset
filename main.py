from pyspark.sql import SparkSession
import Re
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
   streamCount = 10

   groupCols = ["Red", "Blue", "Green", "Yellow", "Purple", "White"]

   for x in range(1,streamCount):
      streamEvent = {}
      streamEvent["Color"] = groupCols[random.randint(0,5)]
      streamEvent["value"] = random.randint(6,10)
      print(str(streamEvent))
