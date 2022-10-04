package com.acme.spark.batch

import com.acme.model.MediaCampaign
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

  case object Datasets {
    def campaignDataSet(session: SparkSession, campaigns: MediaCampaign*): Dataset[MediaCampaign] = {
      val rdd = session.sparkContext.parallelize(campaigns)
      session.createDataset(rdd)(Encoders.product[MediaCampaign])
    }
}
