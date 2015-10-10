-- To start Stardalone Standalone mode
spark-shell -master local[*]

-- to start Yarn mode
spark-shell --master yarn-client --executor-memory 4G --executor-cores 7 --num-executors 6

-- Code
val reviewFile = "hdfs://cshadoop1/psp150030/data/review3.csv" 
val reviewData = sc.textFile(reviewFile, 2)
val mapReviewData = reviewData.map(line => line.split("::")).map(word => (word(2),(word(20).toDouble,1.0)))
val avgReviewData = mapReviewData.reduceByKey((x,y) => ((x._1+y._1)/(x._2+y._2),1.0)).map{case(key,value) => (key,value._1.toInt)}
val finalDataset = avgReviewData.map(keyvalue => keyvalue.swap).sortByKey(false,1).map(keyvalue => keyvalue.swap).take(10).foreach(println)
  
-- Execution Performance

Standalone Mode: 2.148308749 s
Yarn Mode: 5.01478377 s


