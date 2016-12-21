package spark.mllib.feature.selector

import java.util.Arrays

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

/**
  * 说明：本目录下的测试均是针对特征抽取
  *
  * VectorSlicer is a transformer that takes a feature vector and outputs a new feature vector with a sub-array of the original features.
  * It is useful for extracting features from a vector column.
  * VectorSlicer accepts a vector column with specified indices, then outputs a new vector column whose values are selected via those indices.
  * There are two types of indices,  有两种类型的索引
  *   1，Integer indices that represent the indices into the vector, setIndices().
  *   2，String indices that represent the names of features into the vector, setNames(). This requires the vector column to have an AttributeGroup
  *      since the implementation matches on the name field of an Attribute.
  *
  * Specification by integer and string are both acceptable. Moreover, you can use integer index and string name simultaneously. At least one feature
  * must be selected. Duplicate features are not allowed, so there can be no overlap between selected indices and names. Note that if names of features
  * are selected, an exception will be thrown if empty input attributes are encountered.
  * 两种方式可以同时使用，但是不允许重叠。
  *
  * Examples
  * Suppose that we have a DataFrame with the column userFeatures:
  *   userFeatures
  *   ------------------
  *   [0.0, 10.0, 0.5]
  * userFeatures is a vector column that contains three user features. Assume that the first column of userFeatures are all zeros, so we want to remove it
  * and select only the last two columns. The VectorSlicer selects the last two elements with setIndices(1, 2) then produces a new vector column named features:
  *   userFeatures     | features
  *   ------------------|-----------------------------
  *   [0.0, 10.0, 0.5] | [10.0, 0.5]
  * Suppose also that we have potential input attributes for the userFeatures, i.e. ["f1", "f2", "f3"], then we can use setNames("f2", "f3") to select them.
  *   userFeatures     | features
  *   ------------------|-----------------------------
  *   [0.0, 10.0, 0.5] | [10.0, 0.5]
  *   ["f1", "f2", "f3"] | ["f2", "f3"]
  *
  * Created by liangjian on 2016/12/21.
  */
object VectorSlicerTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DCTTest").master("local").getOrCreate()

    val data = Arrays.asList(Row(Vectors.dense(-2.0, 2.3, 0.0)))

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val dataset = spark.createDataFrame(data, StructType(Array(attrGroup.toStructField())))

    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

    slicer.setIndices(Array(1)).setNames(Array("f3"))
    // or slicer.setIndices(Array(1, 2)), or slicer.setNames(Array("f2", "f3"))

    val output = slicer.transform(dataset)
    println(output.select("userFeatures", "features").first())
  }
}
