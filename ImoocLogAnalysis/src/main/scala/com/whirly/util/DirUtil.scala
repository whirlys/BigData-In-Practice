package com.whirly.util

import java.io.File

import com.whirly.Constants

/**
  * 删除本地文件夹
  */
object DirUtil {
  //遍历目录
  def subdirs(dir: File): Iterator[File] = {
    val children = dir.listFiles.filter(_.isDirectory())
    children.toIterator ++ children.toIterator.flatMap(subdirs _)

  }

  //删除目录和文件
  def dirDel(path: File) {
    if (!path.exists()) {
      return
    }
    else if (path.isFile()) {
      path.delete()
      println(path + ":  文件被删除")
      return
    }

    val file: Array[File] = path.listFiles()
    for (d <- file) {
      dirDel(d)
    }

    path.delete()
    println(path + ":  目录被删除")
  }

  def deleteDir(pathStr: String) {
    val path: File = new File(pathStr)
    dirDel(path)
  }

  def main(args: Array[String]) {
    deleteDir(Constants.tempOut)
  }
}
