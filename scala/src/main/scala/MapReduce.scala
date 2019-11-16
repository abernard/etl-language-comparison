import java.io.{BufferedWriter, File, FileWriter}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source

object MapReduce {

  import scala.concurrent.ExecutionContext.Implicits.global

  def mapreduce(inputDir: String, outputDir: String): Unit = {
    reduce(map(inputDir), outputDir)
  }

  private def loadInputs(dir: String): Seq[File] = {
    (new File(dir)).listFiles.filter(f => !f.isDirectory)
  }

  private val pattern = "(?i)knicks".r

  private def process(input: File): Future[Map[String, Int]] = Future {
    val counts = mutable.Map.empty[String, Int]
    val lines = Source.fromFile(input.getAbsolutePath).getLines
    lines.foreach { line =>
      val tokens = line.split('\t')
      val rval = pattern findFirstIn tokens(3)
      val key = tokens(1)
      rval match {
        case Some(_) => counts.update(key, 1 + counts.getOrElse(key, 0))
        case None =>
      }
    }
    counts.toMap
  }

  private def map(inputDir: String): Seq[Map[String, Int]] = {
    val futures = loadInputs(inputDir).map(process)
    Await.result(Future.sequence(futures), Duration.Inf)
  }

  private def reduce(mappings: Seq[Map[String, Int]], destination: String): Unit = {
    val finalCounts = mutable.Map.empty[String, Int]
    mappings.flatten.foreach { t =>
      finalCounts.update(t._1, t._2 + finalCounts.getOrElse(t._1, t._2))
    }
    write(finalCounts.toMap, destination)
  }

  private def write(counts: Map[String, Int], destination: String): Unit = {
    val file = new File(destination)
    val writer = new BufferedWriter(new FileWriter(file))
    val sorted = counts.toSeq.sortBy(-_._2) // descending

    for ((hood, count) <- sorted) {
      writer.write(s"${hood}\t$count\n")
    }
    writer.close()
  }
}
