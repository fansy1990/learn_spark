/**
 * Created by fansy on 2016/2/2.
 */
object SimpleTest extends  App{
  val nodes ="node1\tnode2\tnode3"
  val node2=nodes.substring(nodes.indexOf('\t'))
  var testSeq :Seq[String]=node2.split("\t")
  println(nodes.substring(0,nodes.indexOf('\t')))
  testSeq.foreach(println(_))





}
