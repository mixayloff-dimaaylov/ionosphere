package com.infocom.examples.spark

import java.io.OutputStream

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.IOException
import java.util.zip.{ Deflater, ZipEntry, ZipOutputStream }

import org.apache.hadoop.io.IOUtils

object HDFSArchiver {
  private def relativize(path: Path): String = {
    val relativePath = path.toUri.getPath

    if (relativePath.startsWith("/")) {
      relativePath.substring(1)
    } else {
      relativePath
    }
  }

  @throws[IOException]
  private def handleDirectory(relativePath: String, fs: FileSystem, path: Path, zipOutput: ZipOutputStream): Unit = {
    val entry = new ZipEntry(relativePath + '/')
    zipOutput.putNextEntry(entry)
    zipOutput.closeEntry()

    for (stat <- fs.listStatus(path)) {
      traverse(fs, stat.getPath, zipOutput)
    }
  }

  @throws[IOException]
  private def handleFile(relativePath: String, fs: FileSystem, path: Path, zipOutput: ZipOutputStream): Unit = {
    val entry = new ZipEntry(relativePath)
    val inputStream = fs.open(path)

    zipOutput.putNextEntry(entry)
    IOUtils.copyBytes(inputStream, zipOutput, fs.getConf, false)
    inputStream.close()
    zipOutput.closeEntry()
  }

  @throws[IOException]
  private def traverse(fs: FileSystem, path: Path, zipOutput: ZipOutputStream): Unit = {
    val relativePath = relativize(path)
    val pathStatus = fs.getFileStatus(path)

    if (pathStatus.isDirectory) {
      handleDirectory(relativePath, fs, path, zipOutput)
    } else {
      handleFile(relativePath, fs, path, zipOutput)
    }
  }

  @throws[IOException]
  def zip(fs: FileSystem, src: Path, out: OutputStream): Unit = {
    val zipOutput = new ZipOutputStream(out)
    zipOutput.setLevel(Deflater.BEST_SPEED)
    traverse(fs, src, zipOutput)
    zipOutput.close()
  }
}
