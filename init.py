from pyspark.sql.types import StructType,ArrayType
from pyspark.sql.functions import col,explode_outer
from pyspark.sql import SparkSession


streamCount = 10

groupCols = ["Red","Blue","Green","Yellow","Purple","White"]
sourceLoc = "C:\Ambareesh\Projects\pushDatasetPOC\onPremTest\SourceLoc"
checkPointLoc = "C:\Ambareesh\Projects\pushDatasetPOC\onPremTest\CheckPointLoc"
destLoc = "C:\Ambareesh\Projects\pushDatasetPOC\onPremTest\destLoc"
testDestLoc = "C:\Ambareesh\Projects\pushDatasetPOC\onPremTest\\testDestLoc"
tempLoc= "C:\Ambareesh\Projects\pushDatasetPOC\onPremTest\\tempLoc"


def flattenNestedData(nestedDF):
  try:
       fieldNames = dict([(field.name, field.dataType) for field in nestedDF.schema.fields if type(field.dataType) == ArrayType or type(field.dataType) == StructType]) 
       while len(fieldNames)!=0:
         fieldName=list(fieldNames.keys())[0]
         #print ("Processing :"+fieldName+" Type : "+str(type(fieldNames[fieldName])))
         if type(fieldNames[fieldName]) == StructType:
           extractedFields = [col(fieldName +'.'+ innerColName).alias(fieldName+"_"+innerColName) for innerColName in [ colName.name for colName in fieldNames[fieldName]]]
           nestedDF=nestedDF.select("*", *extractedFields).drop(fieldName)
    
         elif type(fieldNames[fieldName]) == ArrayType: ##If we enable the ArrayType in Line 2 & 15, we end up having multiple duplicate records. Each array column value will create new record, which is worst.
           nestedDF=nestedDF.withColumn(fieldName,explode_outer(fieldName))
    
         fieldNames = dict([(field.name, field.dataType) for field in nestedDF.schema.fields if type(field.dataType) == ArrayType or type(field.dataType) == StructType]) ##Add if you want to explode ArrayType --> type(field.dataType) == ArrayType or  
       return nestedDF
    
  except Exception as err:
    raise Exception("Error Occured at while flattening the dataframe : " + str(err))

