package com.edu.hust.MLlib.Hanzi

/**
  * 记录
  * @param label  文档标签
  * @param text   文档内容
  * Created by liangjian on 2016/12/20.
  */
case class RawDataRecord(label: String, text: String)

/**
  * 记录
  * @param label  文档标签
  * @param title  文档主题
  * @param text   文档内容
  * Created by liangjian on 2017/01/12.
  */
case class RawDataRecordAdv(label: String, title: String, text: String)
