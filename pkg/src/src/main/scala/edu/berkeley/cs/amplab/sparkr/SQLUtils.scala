package edu.berkeley.cs.amplab.sparkr

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types.{StructType}

import edu.berkeley.cs.amplab.sparkr.SerDe._

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream

object SQLUtils {
  def createSQLContext(sc: SparkContext): SQLContext = {
    new SQLContext(sc)
  }

  def getColNames(df: DataFrame): Array[String] = {
    val names = df.schema.fields.map(_.name)

    names
  }

  def getFieldsFromStructType(st: StructType): Array[(String, String, String)] = {
    val names = st.fields.map(_.name)
    val types = st.fields.map(_.dataType.typeName)
    val nullable = st.fields.map(_.nullable.toString)

    (names, types, nullable).zipped.toArray  //Returns a tuple3 containing the contents of each structField
  }

  def dfToRDD(df: DataFrame): Array[Array[Byte]] = {
    val colRDDs = convertRowsToColumns(df)
    val dfOut = colRDDs.map { col =>
      colToRBytes(col)
    }
    dfOut
  }

  def convertRowsToColumns(df: DataFrame): Array[RDD[Any]] = {
    val numCols = df.schema.fields.length
    val colRDDs = (0 until numCols).map { colIdx =>
       df.map { row =>
         row(colIdx)
       }
    }
    colRDDs.toArray
  }

  def colToRBytes(col: RDD[Any]): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)
    val numRows = col.count.toInt
    val collectedRows = col.collect

    SerDe.writeInt(dos, numRows)
    (0 until numRows).map { rowIdx =>
      val obj: Object = collectedRows.apply(rowIdx).asInstanceOf[Object]
      val colArr = SerDe.writeObject(dos, obj)
    }
    bos.toByteArray()
  }
}
