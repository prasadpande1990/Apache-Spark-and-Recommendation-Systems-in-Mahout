Question 3.1

-- Getting the item-similarity matrix using mahout

mahout spark-itemsimilarity --input /psp150030/data/review3.csv --inDelim "::" --output /psp150030/output-mahout -rc 8 -ic 2 -f1 4 -fc 20 -m 5

Question 3.2

val reviewData = sc.textFile("hdfs://cshadoop1/psp150030/data/review3.csv")

val userId = "Ai0THfXbaxY-Jm6ygqn1MQ"

val businessIds = reviewData.filter(line => line.contains(userId)).map(line => line.split("::")).filter(word => word(20).contains("4")).map(word => word(2)).collect()
/*
 
	Output

e_8TvfKT6QT81snfrqYYTw
liWxota5DH7Roo-iv0pTmw
LhmoZDDMBYRgHdWUw0L3tw
Mg1CS5aRT_eV4qaGpEvnIQ

*/

Question 3.3

val similarityData = sc.textFile("hdfs://cshadoop1//psp150030/output-mahout/indicator-matrix/part-00000")
val similarityDataArray = similarityData.toArray
var trav = 0
var temp = 0
var bussArray = new Array[(String, String)](businessIds.size)
while(trav < similarityDataArray.size)
{ 
	var similarity_businessId = similarityDataArray(trav).split("\t")
	if(similarity_businessId.size>1 && (businessIds contains similarity_businessId(0)) ) 
	{ 
		bussArray(temp)=(similarity_businessId(0),similarity_businessId(1)) 
		temp = temp + 1 
	} 
	trav = trav + 1 
	} 
var i = 0 
while(i < bussArray.size) 
{ 
	var businessId: String = bussArray(i)._1 
	var similar_businessIds = bussArray(i)._2.split(" ") 
	println("\n--------------------------\n") 
	print(businessId+"  ") 
	var temp  = 0
	while(temp < similar_businessIds.size) 
	{ 
		print(similar_businessIds(temp).split(":")(0)+"  ") 
		temp = temp + 1
	} 
	println("\n") 
	i = i + 1 
} 

/* 

	Output

liWxota5DH7Roo-iv0pTmw  LhmoZDDMBYRgHdWUw0L3tw  Mg1CS5aRT_eV4qaGpEvnIQ  e_8TvfKT6QT81snfrqYYTw  

Mg1CS5aRT_eV4qaGpEvnIQ  4ERMTfKvWdANjLdyjiCcDA  o7BQ5ZQDWbplY1nKKl_FEg  liWxota5DH7Roo-iv0pTmw  cwwAJ7myWVugAqOqUZUYSg  cWUiqqkS4cpi_StZWkk-Qw  

LhmoZDDMBYRgHdWUw0L3tw  liWxota5DH7Roo-iv0pTmw  fdXxq4s1aS3-PrlHoVUEQw  WIPyIGaZodZUzvEtJzPvJw  H7K1YI4ECl8oIY3Zt8qEsg  M72BkiwGCnNzprwFwJSazQ  

e_8TvfKT6QT81snfrqYYTw  1CBs84C-a-cuA3vncXVSAw  VJW_lfjWxofdtpuVRPY0yQ  QfpwAp53h_DIfzph0IMIbQ  R109PJPOP1puLUHQWeWwcg  XzWUVPs5VcCTEVtWYZVYRg  


*/
