import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._ 
import org.apache.spark.storage.StorageLevel

val spark = SparkSession.builder().master("local[*]").appName("sparsel1").getOrCreate()
val s1 = spark.read.format("csv").option("inferschema","true").option("sep"," ").load("dbfs:/FileStore/tables/cavignal_cen.csv")

//wide dataframe to long dataframess
def toLong(df: DataFrame, by: String): DataFrame = { 
  val kvs = explode(array(df.columns.map(c => struct(lit(c).alias("key"), col(c).alias("top"))): _*)) 
  val listofcols = List(col(by)) :+ kvs.alias("_kvs")
  df.select(listofcols: _*).select(List(col(by)) ++ Seq(col("_kvs.key"), col("_kvs.top")): _*).repartitionByRange(100, $"key")//.cache()
}

//get unique positive lambda for specified jhat
def uniquebplus(df:DataFrame,e:String)={ 
  val total = df.withColumn("total",abs(col(e))).agg(sum("total")).first().getDouble(0)
  val f1 = toLong(df, e).withColumn("ratio", col("top") / col(e))
  val f2 = f1.withColumn("rank",row_number.over(Window.partitionBy(col("key")).orderBy(col("ratio"))))
  val f3 = f2.withColumn("cumsum",sum(abs(col(e))).over(Window.partitionBy(col("key")).orderBy(col("rank"))))
  val f4 = f3.withColumn("lambda_upp",when(col("ratio") > 0,lit(total)-col("cumsum")*2+abs(col(e))*2)
    .otherwise(col("cumsum")*2-lit(total)))
  val bplus = f4.filter(col("lambda_upp") > 0.0 && col("top") =!= col(e)).select("lambda_upp")//.dropDuplicates("lambda_upp")//.rdd.map(_(0))
  bplus//.collect.toList
}


//cal zjhat given lambda
def sparsel1(df:DataFrame,e:String,l:Double):DataFrame ={
  val total = df.withColumn("total",abs(col(e))).agg(sum("total")).first().getDouble(0)//df.select(col(e)).rdd.map(_(0).asInstanceOf[Double]).reduce(math.abs(_)+math.abs(_))
  val f1 = toLong(df, e).withColumn("ratio", col("top") / col(e))
  val f2 = f1.withColumn("rank",row_number.over(Window.partitionBy(col("key")).orderBy(col("ratio"))))
  val f3 = f2.withColumn("cumsum",sum(abs(col(e))).over(Window.partitionBy(col("key")).orderBy(col("rank"))))
  val f4 = f3.withColumn("lambda_low",when(col("ratio") > 0,lit(total)-col("cumsum")*2)
    .otherwise(-lit(total)+col("cumsum")*2-abs(col(e))*2))
  val f5 = f4.withColumn("lambda_upp",when(col("ratio") > 0,lit(total)-col("cumsum")*2+abs(col(e))*2)
    .otherwise(col("cumsum")*2-lit(total))).repartitionByRange(100, $"key").cache()
  val f6 = f5.withColumn("vjhat",when(col("key") === e && col("rank") === 1,1)
    .otherwise(when(col("lambda_low") < l && col("lambda_upp") >= l && col("key") =!= e,'ratio).otherwise(null))) 
  f6.sort(col("key").asc,col("rank").asc) 
}

//seperate regualization and error terms for jhat
  def beta(e:String,lambda:Double) = {
    val cc = Window.partitionBy(col("key")).orderBy(col("vjhat").desc)
    val g2 = sparsel1(s1,e,lambda).repartitionByRange(2, $"key").cache()
    val norm = abs(col("top")-when((first(col("vjhat")).over(cc)).isNull,0).otherwise(first(col("vjhat")).over(cc))*col(e))
    val pena = abs(when(col("vjhat").isNull,0).otherwise(col("vjhat")))
    val g3 = g2.withColumn("l1norm", norm).withColumn("l1regu",pena)
    (g3.agg(sum(col("l1norm"))).first().getDouble(0),g3.agg(sum(col("l1regu"))).first().getDouble(0))
  }
  
  val lambda =0.00001
var zstar = Double.PositiveInfinity 
var vstar = spark.emptyDataFrame
val a = "_c0"
for (e <- Seq(a)) {
val cc = Window.partitionBy('key).orderBy('vjhat.desc)
val g2 = sparsel1(s1,e,lambda)
val l1norm = abs('top-when((first('vjhat).over(cc)).isNull,0).otherwise(first('vjhat).over(cc))*col(e))
val l1regu = lit(lambda)*abs(when('vjhat.isNull,0).otherwise('vjhat))
val g3 = l1norm + l1regu
val g4 = g2.select('*,g3 as 'zjhat)
val zjhat = g4.agg(sum('zjhat)).first().getDouble(0)
  if(zjhat < zstar){
    zstar= zjhat
    vstar =g4.groupBy('key).agg(max('vjhat) as "vstar")
  }
(vstar,zstar)
} 
val sss =math.sqrt(vstar.withColumn("std",abs(col("vstar"))).agg(sum(col("std")*col("std"))).first().getDouble(0))
//display(vstar.orderBy('key).withColumn(s"$zstar/$lambda",col("vstar")/sss))
vstar.orderBy('key).withColumn(s"$zstar/$lambda",col("vstar")/sss).rdd.coalesce(1).saveAsTextFile("/FileStore/tables/export_cen.csv")
