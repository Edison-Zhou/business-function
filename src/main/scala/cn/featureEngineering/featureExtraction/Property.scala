package cn.featureEngineering.featureExtraction

/**
  * Created by cheng_huan on 2018/11/19.
  */
@deprecated
class Property(name:String,
               index:Int,
               numericalType1:String,
               value1:String,
               rating1:Double,
               dataType1:String) {
  //FieldName {特征的域名}
  val fieldName = this.name

  //FieldIndex {特征在该field中的序列号}
  val fieldIndex = this.index

  //FeatureType {continue, discrete, embedding三种类型}
  val numericalType = this.numericalType1

  //FeatureValue {特征的具体值，比如演员具体到“姜文”}
  val value = this.value1

  //FeatureValue {特征的程度，具体为一个0到1之间的数值}
  val rating = this.rating1

  val dataType = this.dataType1
}
