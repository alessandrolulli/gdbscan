package util.converter.mail

import util.CCProperties
import util.CCUtil
import util.CCProperties
import java.io.FileNotFoundException


object ConverterMailSubject {

    def main( args : Array[String] ) : Unit = 
    {
        val propertyLoad = (new CCProperties("CONVERTER_MAIL_SUBJECT", args(0))).load();
		val property = propertyLoad.getImmutable;
		
		if(property.outputFile.isEmpty())
		{
		    throw new FileNotFoundException("OUTPUT FILE (outputFile) MUST BE SPECIFIED")
		}
		
		val util = new CCUtil(property);
		val sc = util.getSparkContext();

		val sqlContext = new org.apache.spark.sql.SQLContext(sc)

		val mail = sqlContext.jsonFile(property.dataset)

		mail.registerTempTable("mail")
		val result = sqlContext.sql("SELECT _id,subject_ta FROM mail")

		val resultRDD = result.map(t =>
	    {
	        try 
	        {
				val u = t.toSeq
				if(u.size >= 2)
					(u(0).toString,u(1).toString)
				else
				    ("EMPTY","EMPTY")
			} catch 
			{
				case e : Exception => ("EMPTY","EMPTY")
			}
	    })
	    
	    val toPrint = resultRDD	.filter(t => !t._1.equals("EMPTY"))
	    						//.map(t => (t._1.replaceAll("[", "").replaceAll("]",""), t._2.replaceAll("[", "").replaceAll("]","")))
	    						.map(t => t._1+"\t"+t._2)
	    						.coalesce(1, true)
	    toPrint.saveAsTextFile(property.outputFile)
    }

}