package com.gravity.goose.images
/**
 * Licensed to Gravity.com under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Gravity.com licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.gravity.goose.text.string
import com.gravity.goose.{Article, Configuration}
import org.apache.http.client.HttpClient
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements

import java.net.{MalformedURLException, URL}
import java.util
import java.util.regex.{Matcher, Pattern}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.matching.Regex
import scala.util.parsing.json.{JSON, JSONObject}

/**
* Created by Jim Plush
* User: jim
* Date: 9/22/11
*/

class UpgradedImageIExtractor(httpClient: HttpClient, article: Article, config: Configuration) extends ImageExtractor {

  import UpgradedImageIExtractor._

  /**
  * What's the minimum bytes for an image we'd accept is
  */
  private val minBytesForImages: Int = 4000

  /**
  * the webpage url that we're extracting content from
  */
  val targetUrl: String = article.finalUrl
  /**
  * stores a hash of our url for reference and image processing
  */
  val linkhash: String = article.linkhash

  /**
  * this lists all the known bad button names that we have
  */
  val matchBadImageNames: Matcher = {
    val sb = new StringBuilder
    // create negative elements
    sb.append(".html|.gif|.ico|button|twitter.jpg|facebook.jpg|ap_buy_photo|digg.jpg|digg.png|delicious.png|facebook.png|reddit.jpg|doubleclick|diggthis|diggThis|adserver|/ads/|ec.atdmt.com")
    sb.append("|mediaplex.com|adsatt|view.atdmt")
    Pattern.compile(sb.toString()).matcher(string.empty)
  }

  def getBestImage(doc: Document, topNode: Element): Image = {
    checkForKnownElements() match {
      case Some(image) => return image
      case None =>
    }

    checkForLargeImages(topNode, 0, 0) match {
      case Some(image) => return image
      case None =>
    }

    checkForMetaTag match {
      case Some(image) => return image
      case None =>
    }

    new Image
  }

  /**
   * Scrape thumbnail image metadata from MSN.com's wcvideoplayer.
   *
   * @return Option[String] The image url that was found, or None.
   */
  private def checkForMsnVideoHeadlineImage: Option[String] = {
    // This DOM path may be too restrictive but it's performing quite well at time of writing.
    val dataConfig: String  =
      article.rawDoc.select("div#maincontent div#main div.primary-playback-region div.wcvideoplayer[data-metadata]").attr("data-metadata")

    JSON.parseRaw(dataConfig) match {
      case Some(JSONObject(metadataMap)) =>
        metadataMap.get("headlineImage") match {
          case Some(JSONObject(headlineImageMap)) =>
            headlineImageMap.get("url").map(_.toString)
          case _ => None
        }
      case _ => None
    }
  }

  private def checkForMetaTag: Option[Image] = {
    checkForLinkTag match {
      case Some(image) => return Some(image)
      case None =>
    }

    checkForOpenGraphTag match {
      case Some(image) => return Some(image)
      case None =>
    }

    None
  }


  /**
  * although slow the best way to determine the best image is to download them and check the actual dimensions of the image when on disk
  * so we'll go through a phased approach...
  * 1. get a list of ALL images from the parent node
  * 2. filter out any bad image names that we know of (gifs, ads, etc..)
  * 3. do a head request on each file to make sure it meets our bare requirements
  * 4. any images left over let's do a full GET request, download em to disk and check their dimensions
  * 5. Score images based on different factors like height/width and possibly things like color density
  *
  * @param node
  */
  private def checkForLargeImages(node: Element, parentDepthLevel: Int, siblingDepthLevel: Int): Option[Image] = {
    getImageCandidates(node) match {
      case Some(goodImages) =>
        val scoredImages = downloadImagesAndGetResults(goodImages, parentDepthLevel)
        scoredImages.sortBy(-_._2).take(1).headOption match {
          case Some(highScoreImage) =>
            val mainImage = new Image
            mainImage.imageSrc = highScoreImage._1.imgSrc
            mainImage.imageBase64 = s"data:image/${highScoreImage._1.mimeType};width=${highScoreImage._1.width};height=${highScoreImage._1.height};base64,${highScoreImage._1.base64}"
            return Some(mainImage)
          case None =>
            getDepthLevel(node, parentDepthLevel, siblingDepthLevel) match {
              case Some(depthObj) =>
                return checkForLargeImages(depthObj.node, depthObj.parentDepth, depthObj.siblingDepth)
              case None =>
            }
        }
      case None =>
        getDepthLevel(node, parentDepthLevel, siblingDepthLevel) match {
          case Some(depthObj) =>
            return checkForLargeImages(depthObj.node, depthObj.parentDepth, depthObj.siblingDepth)
          case None =>
        }
    }

    None
  }

  case class DepthTraversal(node: Element, parentDepth: Int, siblingDepth: Int)

  def getDepthLevel(node: Element, parentDepth: Int, siblingDepth: Int): Option[DepthTraversal] = {
    if (node == null) return None

    val MAX_PARENT_DEPTH = 2
    if (parentDepth > MAX_PARENT_DEPTH) {
      None
    } else {
      val siblingNode = node.previousElementSibling()
      if (siblingNode == null) {
        Some(DepthTraversal(node.parent, parentDepth + 1, 0))
      } else {
        Some(DepthTraversal(siblingNode, parentDepth, siblingDepth + 1))
      }
    }
  }

  /**
  * download the images to temp disk and set their dimensions
  * <p/>
  * we're going to score the images in the order in which they appear so images higher up will have more importance,
  * we'll count the area of the 1st image as a score of 1 and then calculate how much larger or small each image after it is
  * we'll also make sure to try and weed out banner type ad blocks that have big widths and small heights or vice versa
  * so if the image is 3rd found in the dom it's sequence score would be 1 / 3 = .33 * diff in area from the first image
  *
  * @param images
  * @return
  */
  private def downloadImagesAndGetResults(images: util.ArrayList[Element], depthLevel: Int): ListBuffer[(LocallyStoredImage, Float)] = {
    val imageResults = new ListBuffer[(LocallyStoredImage, Float)]()
    var initialArea: Float = 0
    var cnt = 1.0f
    val MIN_WIDTH = 50
    val MIN_HEIGHT = 0

    images.take(30).foreach((image: Element) => {
      for {
        locallyStoredImage <- getLocallyStoredImage(buildImagePath(image.attr("src")))

        width = locallyStoredImage.width
        if width > MIN_WIDTH

        height = locallyStoredImage.height
        if height > MIN_HEIGHT

        if (depthLevel >= 1 && locallyStoredImage.width > 300) || depthLevel < 1
        if !isBannerDimensions(width, height)
      } {
        val sequenceScore: Float = 1.0f / cnt
        val area: Float = width * height
        var totalScore: Float = 0
        if (initialArea == 0) {
          // give the initial image a little area boost as well
          initialArea = area * 1.48f
          totalScore = 1
        }
        else {
          val areaDifference: Float = area / initialArea
          totalScore = sequenceScore * areaDifference
        }
        cnt += 1

        imageResults += locallyStoredImage -> totalScore
        cnt += 1
      }
    })



    imageResults
  }

  def getAllImages: util.ArrayList[Element] = {
    null
  }

  /**
  * returns true if we think this is kind of a bannery dimension
  * like 600 / 100 = 6 may be a fishy dimension for a good image
  *
  * @param width
  * @param height
  */
  private def isBannerDimensions(width: Int, height: Int): Boolean = {
    if (width == height) {
      return false
    }
    if (width > height) {
      val diff: Float = width.toFloat / height.toFloat
      if (diff > 5) {
        return true
      }
    }
    if (height > width) {
      val diff: Float = height.toFloat / width.toFloat
      if (diff > 5) {
        return true
      }
    }
    false
  }

  def getImagesFromNode(node: Element): Option[Elements] = {
    val images: Elements = node.select("img")

    if (images == null || images.isEmpty) {
      None
    } else {
      Some(images)
    }
  }

  /**
  * takes a list of image elements and filters out the ones with bad names
  *
  * @param images
  * @return
  */
  private def filterBadNames(images: Elements): Option[util.ArrayList[Element]] = {
    val goodImages: util.ArrayList[Element] = new util.ArrayList[Element]
    for (image <- images) {
      if (this.isOkImageFileName(image)) {
        goodImages.add(image)
      }
      else {
        image.remove()
      }
    }
    if (goodImages == null || goodImages.isEmpty) None else Some(goodImages)
  }

  /**
  * will check the image src against a list of bad image files we know of like buttons, etc...
  *
  * @return
  */
  private def isOkImageFileName(imageNode: Element): Boolean = {
    val imgSrc: String = imageNode.attr("src")
    if (string.isNullOrEmpty(imgSrc)) {
      return false
    }
    matchBadImageNames.reset(imgSrc)
    if (matchBadImageNames.find) {
      if (logger.isDebugEnabled) {
        logger.debug("Found bad filename for image: " + imgSrc)
      }
      return false
    }
    true
  }

  def getImageCandidates(node: Element): Option[util.ArrayList[Element]] = {

    for {
      n <- getNode(node)
      images <- getImagesFromNode(node)
      filteredImages <- filterBadNames(images)
      goodImages <- findImagesThatPassByteSizeTest(filteredImages)
    } {
      return Some(filteredImages)
    }
    None

  }

  /**
  * loop through all the images and find the ones that have the best bytez to even make them a candidate
  *
  * @param images
  * @return
  */
  private def findImagesThatPassByteSizeTest(images: util.ArrayList[Element]): Option[util.ArrayList[Element]] = {
    var cnt: Int = 0
    val MAX_BYTES_SIZE: Int = 15728640
    val goodImages: util.ArrayList[Element] = new util.ArrayList[Element]
    images.foreach(image => {
      try {
        if (cnt > 30) {
          return Some(goodImages)
        }
        val imageSrc = image.attr("src")
        getLocallyStoredImage(buildImagePath(imageSrc)) match {
          case Some(locallyStoredImage) =>
            val bytes = locallyStoredImage.bytes
            if ((bytes == 0 || bytes > minBytesForImages) && bytes < MAX_BYTES_SIZE) {
              goodImages.add(image)
            } else {
              image.remove()
            }
          case None =>
        }

      } catch {
        case e: Exception => warn(e, e.toString)
      }
      cnt += 1
    })

    if (goodImages == null || goodImages.isEmpty) None else Some(goodImages)

  }

  def getNode(node: Element): Option[Element] = {
    Option(node)
  }

  /**
  * checks to see if we were able to find open graph tags on this page
  *
  * @return
  */
  private def checkForLinkTag: Option[Image] = {
    if (article.rawDoc == null) return None

    try {
      val meta: Elements = article.rawDoc.select("link[rel~=image_src]")
      for (item <- meta) {
        val href = item.attr("href")
        if (href.isEmpty) {
          return None
        }
        val mainImage = new Image
        mainImage.imageSrc = buildImagePath(href)
        getLocallyStoredImage(mainImage.imageSrc) match {
          case Some(locallyStoredImage) =>
            mainImage.imageBase64 = locallyStoredImage.base64
          case None =>
        }
        return Some(mainImage)
      }
      None
    }
    catch {
      case e: Exception =>
        warn("Unexpected exception caught in checkForLinkTag. Handled by returning None.", e)
        None
    }

  }

  /**
  * checks to see if we were able to find open graph tags on this page
  *
  * @return
  */
  private def checkForOpenGraphTag: Option[Image] = {
    try {
      val meta: Elements = article.rawDoc.select("meta[property~=og:image]")

      for (item <- meta) {
        if (item.attr("content").length < 1) {
          return None
        }
        val imagePath: String = this.buildImagePath(item.attr("content"))
        val mainImage = new Image
        mainImage.imageSrc = imagePath
        getLocallyStoredImage(mainImage.imageSrc) match {
          case Some(locallyStoredImage) =>
            mainImage.imageBase64 = locallyStoredImage.base64
          case None =>
        }
        return Some(mainImage)
      }
      None
    }
    catch {
      case e: Exception =>
        warn(e, e.toString)
        None
    }
  }


  /**
  * returns the bytes of the image file on disk
  */
  def getLocallyStoredImage(imageSrc: String): Option[LocallyStoredImage] = ImageUtils.storeImageToLocalFile(httpClient, linkhash, imageSrc, config)

  def getCleanDomain: String = {
    // just grab the very end of the domain
    dotRegex.split(article.domain).takeRight(2).mkString(".")
  }

  /**
  * in here we check for known image contains from sites we've checked out like yahoo, techcrunch, etc... that have
  * known  places to look for good images.
  * //todo enable this to use a series of settings files so people can define what the image ids/classes are on specific sites
  */
  def checkForKnownElements(): Option[Image] = {
    if (article.rawDoc == null) return None

    val domain = getCleanDomain
    customSiteMapping.get(domain).foreach(classes => {
      subDelimRegex.split(classes).foreach(c => KNOWN_IMG_DOM_NAMES += c)
    })

    var knownImageElem: Element = null
    for (knownName <- KNOWN_IMG_DOM_NAMES; if knownImageElem == null) {
      var known: Element = article.rawDoc.getElementById(knownName)
      if (known == null) {
        known = article.rawDoc.getElementsByClass(knownName).first
      }
      if (known != null) {
        val mainImageElem: Element = known.getElementsByTag("img").first

        if (mainImageElem != null) {
          knownImageElem = mainImageElem
        }
      }
    }

    val knownImgSrcOpt  =
      if (knownImageElem != null)
        Option(knownImageElem.attr("src"))
      else
        checkForMsnVideoHeadlineImage

    knownImgSrcOpt match {
      case Some(knownImgSrc) if knownImgSrc != "" =>
        val mainImage = new Image
        mainImage.imageSrc = buildImagePath(knownImgSrc)
        getLocallyStoredImage(buildImagePath(mainImage.imageSrc)).foreach(locallyStoredImage => {
          mainImage.imageBase64 = locallyStoredImage.base64
        })

        Some(mainImage)

      case _ =>
        None
    }
  }

  /**
  * This method will take an image path and build out the absolute path to that image
  * using the initial url we crawled so we can find a link to the image if they use relative urls like ../myimage.jpg
  *
  * @param imageSrc
  * @return
  */
  private def buildImagePath(imageSrc: String): String = {

    try {
      val pageURL = new URL(this.targetUrl)
      return new URL(pageURL, ImageUtils.cleanImageSrcString(imageSrc)).toString
    }
    catch {
      case _: MalformedURLException =>
        warn("Unable to get Image Path: " + imageSrc)
    }

    imageSrc
  }

}

object UpgradedImageIExtractor {
  val delimRegex: Regex = """\^""".r
  val dotRegex: Regex = """\.""".r
  val subDelimRegex: Regex = """\|""".r

  // custom site mapping is for major sites that we know what they generally
  // place images into, allows for higher accuracy of image extraction
  lazy val customSiteMapping: Map[String, String] = {
    val lines = Source.fromInputStream(getClass.getResourceAsStream("/com/gravity/goose/images/known-image-css.txt")).getLines()
    (for (line <- lines) yield {
      val Array(domain, css) = delimRegex.split(line)
      domain -> css
    }).toMap
  }

  val KNOWN_IMG_DOM_NAMES = ListBuffer("yn-story-related-media", "cnn_strylccimg300cntr", "big_photo", "ap-smallphoto-a")

}