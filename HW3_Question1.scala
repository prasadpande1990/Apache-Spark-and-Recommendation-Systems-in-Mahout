val inputFile = "hdfs://cshadoop1/psp150030/data/business3.csv" 
val inputData = sc.textFile(inputFile, 2).cache()
inputData.map(line => line.split("::")).map(word => (word(2),word(3))).filter{case(key,value) => value.contains("Stanford")}.map{case(key,value) => key}.collect.foreach(println)


