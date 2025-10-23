package org.mule.weave.clinative

import java.io.File


case class TestSuite(name: String, zipFile: File)

case class Scenario(name: String, testFolder: File, inputs: Array[File], transform: File, output: File, configProperty: Option[File])