package com.acme.spark.batch
import com.acme.model.MediaCampaign
import org.apache.spark.sql.functions.{avg, month}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.{LocalSparkContext, SparkFunSuite}

import scala.collection.immutable.ArraySeq

class CampaignsBatchSpec extends SparkFunSuite with LocalSparkContext {

    val spark = SparkSession.builder().master("local")
          .config("spark.ui.enabled", "false")
          .appName("PubSapient").getOrCreate()
     spark.sparkContext.setLogLevel("FATAL")

    import spark.implicits._

  /** Top 5 Performing Partners
   * Metrics :
   * VCR (Video Completion Rate) [Hint: VCR = Video Views / Video Completion]
   * VTR (Video Through Rate) [Hint: Video Views / Impressions]
   *
   * select site, sum(videoViews/videoCompletes) VCR, sum(videoViews/impressions) VTR
   * from MediaCampaign
   * group by site
   * order by VCR desc, VTR desc
   * limit 5
   */
  test("Top 5 Performing Partners.VCR,VTR") {
    val template = MediaCampaign(campaign ="C1", date = "2022-01-03T10:15:30+02:00", site = "P1",
          videoViews = 100, videoCompletes = 10, impressions = 200)
    val campaigns = Datasets.campaignDataSet(spark,  template,
      template.copy(site = "P2", videoViews = 100, videoCompletes = 8, impressions = 400),
      template.copy(site = "P3", videoViews = 100, videoCompletes = 5, impressions = 600),
      template.copy(site = "P4", videoViews = 100, videoCompletes = 4, impressions = 800),
      template.copy(site = "P5", videoViews = 100, videoCompletes = 3, impressions = 1000),
      template.copy(site = "P6", videoViews = 100, videoCompletes = 2, impressions = 1400))

    val result = campaigns.filter(MediaCampaign.isValid(_))
          .filter(e => e.videoCompletes > 0 && e.impressions >0)
          // may use rank or window with partition by, but group produces the same result
          .groupBy($"site").agg(
              functions.sum($"videoViews" / $"videoCompletes").as("VCR")
            , functions.sum($"videoViews" / $"impressions").as("VTR"))
          .orderBy($"VCR".desc, $"VTR".desc)
          .limit(5)

    result.show()
    val topPartners = result.map(row => row.getString(0)).collect()
    val expected = ArraySeq("P6", "P5", "P4", "P3", "P2")
    assertResult(expected)(topPartners)

  }

  /** For each campaign,partner,device:
   *    CTR,% = 0.01 * Clicks / Impressions
   *    CPC = Total Cost of Clicks / Total Clicks
   *    You may also calculate it from CPM and CTR: CPC = (CPM / 1000) / (CTR / 100) = 0.1 * CPM / CTR.
   *    where CPM = (cost per 1,000 impressions), e.g 10000 imps. = 2$ = campaign cost 20$
   *
   * select campaign,site,device, avg($"clicks" / $"impressions") CTR,
   *    avg($"actualizedSpend" / $"clicks" / $"impressions") CPC
   * ??? actualizedSpend = CPM ???
   * from MediaCampaign
   * group by campaign,site,device
   * order by CTR, CPC desc
   * limit 5
   */
  test("For each campaign,partner,device : CTR, CPC") {
    val template = MediaCampaign(campaign ="C1", date = "2022-01-03T10:15:30+02:00", site = "P1"
          , videoViews = 100, videoCompletes = 10, impressions = 200, clicks= 20, device = 1)
    val campaigns = Datasets.campaignDataSet(spark,  template,
        template.copy(impressions = 100, clicks= 5, device = 1),
        template.copy(impressions = 500, clicks= 10, device = 2),
        template.copy(campaign ="C2", site = "P1", impressions = 100, clicks= 20, device = 2),
        template.copy(campaign ="C2", site = "P1", impressions = 300, clicks= 20, device = 2),
        template.copy(campaign ="C3", site = "P3", impressions = 1000, clicks= 20, device = 3),
      template.copy(campaign ="C4", site = "P3", impressions = 1000, clicks= 20, device = 3))

    // CPC = CPM(actualizedSpend??) / CTR
    val result = campaigns.filter(MediaCampaign.isValid(_))
          .filter(e => e.videoCompletes > 0 && e.impressions >0)
      .groupBy($"campaign", $"site", $"device").agg(
          functions.avg($"clicks" / $"impressions").as("CTR")
        , functions.avg($"actualizedSpend" / $"clicks" / $"impressions").as("CPC"))
      .orderBy($"CTR".desc, $"CPC".desc)

    result.show()
  }

  test("Best Month to launch a campaign (max CTC, CTR)") {
    val template = MediaCampaign(campaign ="C1", date = "2022-01-01T10:15:30+02:00", site = "P1"
      , videoViews = 100, videoCompletes = 10, impressions = 200, clicks= 4)
    val campaigns = Datasets.campaignDataSet(spark,  template,
      template.copy(date = "2022-02-01T10:15:30+02:00", impressions = 100, clicks= 5),
      template.copy(date = "2022-03-01T10:15:30+02:00", impressions = 100, clicks= 10),
      template.copy(date = "2022-04-01T10:15:30+02:00", impressions = 100, clicks= 10),
      template.copy(date = "2022-05-01T10:15:30+02:00", impressions = 100, clicks= 20), // best CTR
      template.copy(date = "2022-06-01T10:15:30+02:00", impressions = 100, clicks= 5),
      template.copy(date = "2022-07-01T10:15:30+02:00", impressions = 100, clicks= 10))

    val result = campaigns.filter(MediaCampaign.isValid(_))
        .filter(e => e.impressions > 0 && e.clicks >0)
      .withColumn("BestMonth", month($"date"))
      .withColumn("CTR", $"clicks" / $"impressions")
      .withColumn("CPC", $"actualizedSpend" / $"clicks" / $"impressions")
      .orderBy($"CTR".desc)
      .limit(1)

    //result.show()
    val bestMonth = result.map(row => row.getAs[Int]("BestMonth")).collect()
    assertResult(5)(bestMonth(0))
  }

  /**
   * select campaign, partner, sum(videoCompletes) / sum(videoViews) * 100
   * from MediaCampaign
   * group by campaign, partner
   */
  test("% of people has started watching the video but did not completed") {
    val template = MediaCampaign(campaign ="C1", date = "2022-01-01T10:15:30+02:00", site = "P1"
      , videoViews = 100, videoCompletes = 10, impressions = 100, clicks= 10) // 10%
    val campaigns = Datasets.campaignDataSet(spark,  template)

    val result = campaigns.filter(MediaCampaign.isValid(_))
      .groupBy($"campaign", $"site").agg(
        functions.sum($"videoCompletes").as("totalVideoCompleted")
      , functions.sum($"videoViews").as("totalVideoViews"))
      .withColumn("NotCompletedPercent",
          $"totalVideoCompleted" / $"totalVideoViews" * 100)

    //result.show()
    val notCompletedPercent = result.map(row => row.getAs[Double]("NotCompletedPercent")).collect()
    assertResult(10.0)(notCompletedPercent(0))
  }

  /**
   * Compare ER (Engagement Rate) for Video channels v/s Non-Video Channels? [Engagement Rate = Engagement / Impressions]
   *
   * select channel, avg(Engagement / Impressions) as ER
   * from MediaCampaign
   * group by channel
   */
  test("Compare ER for Video channels v/s Non-Video Channels?") {
    val template = MediaCampaign(campaign ="C1", date = "2022-01-01T10:15:30+02:00", site = "P1"
      , engagements = 10 , impressions = 1000, channel = "V") //   video channel 0.01
    val campaigns = Datasets.campaignDataSet(spark,  template,
        template.copy(engagements = 40 , impressions = 2000, channel = "I")) // image channel 0.01

    val result = campaigns.groupBy($"channel")
          .agg(avg($"engagements" / $"impressions").alias("er"))
          .orderBy($"er".desc)

    //result.show()
    val channelsER = result.collect()
    assertResult(ArraySeq("I", 0.02).toSeq)(channelsER(0).toSeq)
    assertResult(ArraySeq("V", 0.01).toSeq)(channelsER(1).toSeq)
  }
}