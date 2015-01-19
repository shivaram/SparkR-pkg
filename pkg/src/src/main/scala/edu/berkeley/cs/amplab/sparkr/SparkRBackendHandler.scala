package edu.berkeley.cs.amplab.sparkr

import scala.collection.mutable.HashMap
import java.io._

import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler

import edu.berkeley.cs.amplab.sparkr.SerializeJavaR._

/**
 * Handler for SparkRBackend
 * TODO: This is marked as sharable to get a handle to SparkRBackend. Is it safe to re-use
 * this across connections ?
 */
@Sharable
class SparkRBackendHandler(server: SparkRBackend) extends SimpleChannelInboundHandler[Array[Byte]] {

  val objMap = new HashMap[String, Object]

  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]) {
    val bis = new ByteArrayInputStream(msg)
    val dis = new DataInputStream(bis)

    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    // First bit is isStatic
    val isStatic = readBoolean(dis)
    val objId = readString(dis)
    val methodName = readString(dis)
    val numArgs = readInt(dis)

    // System.err.println(s"Handling $methodName on $objId")

    if (objId == "SparkRHandler") {
      methodName match {
        case "stopBackend" => {
          // dos.write(0)
          writeInt(dos, 0)
          writeString(dos, "character")
          writeString(dos, "void")
          server.close()
        }
        case _ => dos.writeInt(-1)
      }
    } else {
      handleMethodCall(isStatic, objId, methodName, numArgs, dis, dos)
    }

    val reply = bos.toByteArray
    ctx.write(reply)
  }
  
  override def channelReadComplete(ctx: ChannelHandlerContext) {
    ctx.flush()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    // Close the connection when an exception is raised.
    cause.printStackTrace()
    ctx.close()
  }

  def handleMethodCall(isStatic: Boolean, objId: String, methodName: String,
    numArgs: Int, dis: DataInputStream, dos: DataOutputStream) {

    var obj: Object = null
    var cls: Option[Class[_]] = None
    try {
      if (isStatic) {
        cls = Some(Class.forName(objId))
      } else {
        objMap.get(objId) match {
          case None => throw new IllegalArgumentException("Object not found " + objId)
          case Some(o) => {
            cls = Some(o.getClass)
            obj = o
          }
        }
      }

      val methods = cls.get.getMethods()
      // TODO: We only use first method with the same name
      val selectedMethods = methods.filter(m => m.getName() == methodName)
      if (selectedMethods.length > 0) {
        val selectedMethod = selectedMethods.filter { x => 
          x.getParameterTypes().length == numArgs
        }.head
        val argTypes = selectedMethod.getParameterTypes()
        val args = parseArgs(argTypes, dis)

        val ret = selectedMethod.invoke(obj, args:_*)

        // Write status bit
        writeInt(dos, 0)
        writeObject(dos, ret.asInstanceOf[AnyRef], objMap)
      } else if (methodName == "new") {
        // methodName should be "new" for constructor
        // TODO: We only use the first constructor ?
        val ctor = cls.get.getConstructors().filter { x =>
          x.getParameterTypes().length == numArgs
        }.head

        val argTypes = ctor.getParameterTypes()
        val params = parseArgs(argTypes, dis)
        val obj = ctor.newInstance(params:_*)

        writeInt(dos, 0)
        writeObject(dos, obj.asInstanceOf[AnyRef], objMap)
      } else {
        throw new IllegalArgumentException("invalid method " + methodName + " for object " + objId)
      }
    } catch {
      case e: Exception => {
        System.err.println(s"$methodName on $objId failed with " + e)
        e.printStackTrace()
        writeInt(dos, -1)
      }
    }
  }

  def parseArgs(argTypes: Array[Class[_]], dis: DataInputStream): Array[java.lang.Object] =  {
    // TODO: Check each parameter type to the R provided type
    argTypes.map { arg =>
      readObject(dis, objMap)
    }.toArray
  }

}