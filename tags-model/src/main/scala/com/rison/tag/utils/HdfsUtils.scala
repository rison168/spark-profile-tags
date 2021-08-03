package com.rison.tag.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * @author : Rison 2021/8/3 上午9:49
 * 操作HDFS文件系统工具类
 */
object HdfsUtils {
  /**
   * 判断路径是否存在
   * @param conf Configurtaion 实例对象
   * @param path 模型路径
   * @return
   */
  def exists(conf: Configuration, path: String): Boolean = {
    //判断文件系统
    val dfs: FileSystem = FileSystem.get(conf)
    //判断路径是否存在
    dfs.exists(new Path(path))
  }

  /**
   * 删除路径
   * @param conf
   * @param path
   */
  def delete(conf: Configuration, path: String): Unit ={
    // a. 获取文件系统
    val dfs: FileSystem = FileSystem.get(conf)
    // b. 如果路径存在就删除
    dfs.deleteOnExit(new Path(path))
  }

}
