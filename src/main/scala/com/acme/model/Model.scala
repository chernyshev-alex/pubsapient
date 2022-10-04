package com.acme.model

import java.time.ZonedDateTime
import scala.util.Try

//1. Media_Campaign
//2. Paid_Search

case class MediaCampaign(campaign : String
                         , date : String       //  campaign date
                         , site: String        // media partner
                         // the amount of money a marketing department spends on activities
                         // Probably this is CPM ? cost,$ per 1,000 impressions
                         // Found Quality Score in PaidSearch entity,
                         // but the key to join the media campaign is not clear
                         , actualizedSpend : BigDecimal = 1.0  // 10 cents per
                         , videoViews: Long = 0
                         , videoCompletes: Long = 0
                         , impressions: Long = 0
                         , clicks: Long = 0
                         , device :  Int = 0
                         , rate :  BigDecimal = 0.0
                         , engagements : Long = 0
                         , channel : String = "V"
                        )

object MediaCampaign {
  // Minimal sanitary checks
  // Supposed data quality phase has been passed before with amazon deequ library
  def isValid(c: MediaCampaign): Boolean = {
    var v = Try(ZonedDateTime.parse(c.date.trim)).isSuccess
    v &&= c.site != null && c.site.trim.length > 0
    // add more  rules here ..
    // v &&= c.impressions >0 && c.videoCompletes > 0
    v
  }
}

case class PaidSearch(originalKeyword: String
                      , impressions: Long = 0
                      , clicks: Long = 0
                      , qualityScore : Double = 0.0)

object PaidSearch {  // TODO  isValid
  def isValid(c: PaidSearch) : Boolean = true
}


