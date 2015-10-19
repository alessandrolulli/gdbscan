package util.converter.mail

import util.CCProperties
import util.CCUtil
import util.CCProperties
import java.io.FileNotFoundException


object ConverterTwitter {

    def main( args : Array[String] ) : Unit = 
    {
        val propertyLoad = (new CCProperties("CONVERTER_TWITTER", args(0))).load();
		val property = propertyLoad.getImmutable;
		val feature = propertyLoad.get("feature", "text")
		val id = propertyLoad.get("idToUse", "id_str")
        val coordinates = propertyLoad.get("coordinates", "coordinates")
		
		//TWITTER = id_str,text,coordinates
		
		if(property.outputFile.isEmpty())
		{
		    throw new FileNotFoundException("OUTPUT FILE (outputFile) MUST BE SPECIFIED")
		}
		
		val util = new CCUtil(property);
		val sc = util.getSparkContext();

		val sqlContext = new org.apache.spark.sql.SQLContext(sc)

		val mail = sqlContext.jsonFile(property.dataset)

		mail.registerTempTable("mail")
		val result = sqlContext.sql("SELECT "+id+","+feature+","+coordinates+" FROM mail")

		val resultRDD = result.map(t =>
	    {
	        try 
	        {
                if(!t.isNullAt(2))
                {
    				val u = t.toSeq
    				if(u.size >= 2)
    					(u(0).toString,u(2).toString.replace("[ArrayBuffer(", "").replace("),Point]", "").replace(", ", "\t"),u(1).toString)
    				else
    				    ("EMPTY","EMPTY","EMPTY")
                } else
                    ("EMPTY","EMPTY","EMPTY")
			} catch 
			{
				case e : Exception => ("EMPTY","EMPTY","EMPTY")
			}
	    })
	    
	    val toPrint = resultRDD	.filter(t => !t._1.equals("EMPTY"))
	    						.map(t => t._1+"\t"+t._2+"\t"+t._3.replace("\n", " "))
	    						.coalesce(1, true)
	    toPrint.saveAsTextFile(property.outputFile)
    }

}