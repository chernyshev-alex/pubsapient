package com.acme.spark.batch
//import com.amazon.deequ.VerificationSuite
//import com.amazon.deequ.checks.{Check, CheckLevel}
import org.apache.spark.{LocalSparkContext, SparkFunSuite}

/**
 * Data quality control example.
 * It must be the first step in the data processing pipeline
 */
class DataQualitySpec  extends SparkFunSuite with LocalSparkContext  {

//  val spark = SparkSession.builder().master("local")
//    .config("spark.ui.enabled", "false")
//    .appName("PubSapient").getOrCreate()
//  spark.sparkContext.setLogLevel("WARN")

  test("CampaignDataSet validation") {
//    val template = MediaCampaign(campaign = "C1", date = "2022-01-03T10:15:30+02:00", site = "P1",
//      videoViews = 100, videoCompletes = 10, impressions = 200)
//
//    val campaigns = Datasets.campaignDataSet(spark, template,
//      template.copy(site = "P2", videoViews = 100, videoCompletes = 8, impressions = 400),
//      template.copy(site = "P3", videoViews = 100, videoCompletes = 5, impressions = 600),
//      template.copy(site = "P4", videoViews = 100, videoCompletes = 4, impressions = 800),
//      template.copy(site = "P5", videoViews = 100, videoCompletes = 3, impressions = 1000),
//      template.copy(site = "P6", videoViews = 100, videoCompletes = 2, impressions = 1400))

//      val verificationResult = VerificationSuite()
//        .onData(campaigns.toDF())
//        .addCheck(
//          Check(CheckLevel.Error, "unit testing my data")
//            .hasSize(_ == 10000) // we expect 5 rows
//            .isComplete("site") // should never be NULL
//
//            .isComplete("videoCompletes") // should never be NULL
//            .isPositive("impressions")
//            .isPositive("videoViews")
//
//            .hasApproxQuantile("numViews", 0.5, _ <= 10))
//        .run()
    }
  }
