package edu.berkeley.cs.amplab.sparkr

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext}
import org.apache.spark.sql.types.{StructType}

object sparkRSQL {

	def createSQLContext(sc: SparkContext): SQLContext = {
		new SQLContext(sc)
	}
	
	def getFieldsFromStructType(st: StructType): Array[(String, String, String)] = {
		val names = st.fields.map(_.name)
		val types = st.fields.map(_.dataType.typeName)
		val nullable = st.fields.map(_.nullable.toString)

		(names, types, nullable).zipped.toArray  //Returns a tuple3 containing the contents of each structField
	}
}