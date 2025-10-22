package org.mule.weave.clinative


import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.IOUtils
import org.mule.weave.v2.matchers.WeaveMatchers.matchBin
import org.mule.weave.v2.matchers.WeaveMatchers.matchJson
import org.mule.weave.v2.matchers.WeaveMatchers.matchProperties
import org.mule.weave.v2.matchers.WeaveMatchers.matchString
import org.mule.weave.v2.matchers.WeaveMatchers.matchXml
import org.mule.weave.v2.utils.StringHelper.toStringTransformer
import org.scalactic.AbstractStringUniformity
import org.scalactic.Uniformity
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.nio.charset.StandardCharsets
import jakarta.mail.internet.MimeMultipart
import jakarta.mail.util.ByteArrayDataSource
import scala.io.BufferedSource
import scala.io.Source

object AssertionHelper extends Matchers {
  val whiteSpaceNormalised: Uniformity[String] =
    new AbstractStringUniformity {

      /** Returns the string with all consecutive white spaces reduced to a single space. */
      def normalized(s: String): String = {
        val result: String = s.replaceAll("\\s+", " ").replaceAll(" ", "")
        result
      }

      override def toString: String = "whiteSpaceNormalised"
    }

  def doAssert(actualFile: File, expectedFile: File, maybeEncoding: Option[String] = None): Assertion = {
    val bytes: Array[Byte] = IOUtils.toByteArray(new FileInputStream(actualFile))
    val encoding = maybeEncoding.getOrElse("UTF-8")
    val extension = FilenameUtils.getExtension(expectedFile.getName)
    extension match {
      case "json" =>
        val actual: String = new String(bytes, encoding)
        val actualNormalized = actual.stripMarginAndNormalizeEOL.replace("\\r\\n", "\\n")
        actualNormalized should matchJson(readFile(expectedFile, encoding))
      case "xml" =>
        val actual: String = new String(bytes, encoding)
        actual.stripMarginAndNormalizeEOL should matchXml(readFile(expectedFile, encoding))
      case "dwl" =>
        val actual: String = new String(bytes, "UTF-8")
        actual should matchString(readFile(expectedFile, encoding))(after being whiteSpaceNormalised)
      case "csv" =>
        val actual: String = new String(bytes, encoding).trim
        val actualNormalized = actual.stripMarginAndNormalizeEOL
        val expected = readFile(expectedFile, encoding).trim
        val expectedNormalized = expected.stripMarginAndNormalizeEOL
        actualNormalized should matchString(expectedNormalized)
      case "txt" =>
        val actual: String = new String(bytes, encoding).trim
        val actualNormalized = actual.stripMarginAndNormalizeEOL
        val expected = readFile(expectedFile, encoding).trim
        val expectedNormalized = expected.stripMarginAndNormalizeEOL
        actualNormalized should matchString(expectedNormalized)
      case "bin" =>
        assertBinaryFile(bytes, expectedFile)
      case "urlencoded" =>
        val actual: String = new String(bytes, "UTF-8")
        actual should matchString(readFile(expectedFile, encoding).trim)
      case "properties" =>
        val actual: String = new String(bytes, "UTF-8")
        actual should matchProperties(readFile(expectedFile, encoding).trim)

      case "multipart" =>
        matchMultipart(expectedFile, bytes)

      case "yml" | "yaml" =>
        val actual: String = new String(bytes, "UTF-8")
        actual.trim should matchString(readFile(expectedFile, encoding).trim)
    }
  }

  private def assertBinaryFile(result: Array[Byte], expectedFile: File): Assertion = {
    result should matchBin(expectedFile)
  }

  private def matchMultipart(output: File, result: Array[Byte]): Assertion = {
    val expected = new MimeMultipart(new ByteArrayDataSource(new FileInputStream(output), "multipart/form-data"))
    val actual = new MimeMultipart(new ByteArrayDataSource(new ByteArrayInputStream(result), "multipart/form-data"))
    actual.getPreamble should matchString(expected.getPreamble)
    actual.getCount shouldBe expected.getCount

    var i = 0
    while (i < expected.getCount) {
      val expectedBodyPart = expected.getBodyPart(i)
      val actualBodyPart = actual.getBodyPart(i)
      actualBodyPart.getContentType should matchString(expectedBodyPart.getContentType)
      actualBodyPart.getDisposition should matchString(expectedBodyPart.getDisposition)
      actualBodyPart.getFileName should matchString(expectedBodyPart.getFileName)

      val actualContent = actualBodyPart.getContent
      val expectedContent = expectedBodyPart.getContent

      val actualContentString = actualContent match {
        case is: InputStream => IOUtils.toString(is, StandardCharsets.UTF_8)
        case _ => String.valueOf(actualContent);
      }

      val expectedContentString = expectedContent match {
        case is: InputStream => IOUtils.toString(is, StandardCharsets.UTF_8)
        case _ =>
          String.valueOf(expectedContent);
      }

      val actualContentNormalized = actualContentString.stripMarginAndNormalizeEOL
      val expectedContentNormalized = expectedContentString.stripMarginAndNormalizeEOL
      actualContentNormalized shouldBe expectedContentNormalized

      i = i + 1
    }
    assert(true)
  }

  private def readFile(expectedFile: File, charset: String): String = {
    val expectedText: String = {
      if (expectedFile.getName endsWith ".bin")
        ""
      else {
        var value1: BufferedSource = null
        try {
          value1 = Source.fromFile(expectedFile, charset)
          value1.mkString
        } finally {
          value1.close()
        }
      }

    }
    expectedText
  }

}
