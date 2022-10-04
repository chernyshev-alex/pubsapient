
-- KSQL scripts
-- Deploy scripts to Confluent to get real-time reports
-- Just example

DEFINE TS_FORMAT = '''yyyy-MM-ddTHH:mm:ss.SSSSSS''';
DEFINE PAR_DEFAULT = '5';  -- default partitions
DEFINE REPL_DEFAULT = '3'; -- default replication

-- main input topic schema
CREATE OR REPLACE STREAM MediaCampaign (
           CAMPAIGN             STRING
         , DATE                 STRING
         , SITE                 STRING
         , ACTUALIZEDSPEND      DECIMAL(38, 0)
         , VIDEOVIEWS           INTEGER
         , VIDEOCOMPLETES       INTEGER
         , IMPRESSIONS          INTEGER
         , CLICKS               INTEGER
         , DEVICE               INTEGER
         , RATE                 DECIMAL(38, 2)
         , ENGAGEMENTS          INTEGER
         , CHANNEL              STRING
WITH (KAFKA_TOPIC ='MediaCampaignTopic', VALUE_FORMAT='AVRO', PARTITIONS=${PAR_DEFAULT});

--- End topics definition

--  Public output endpoint : Real-time output streams definitions

-- **** Top 5 Performing Partners *****

CREATE OR REPLACE STREAM TOP5_PERFORMING_PARTNERS_JSON
 WITH (KAFKA_TOPIC ='TOP5_PERFORMING_PARTNERS_JSON' , VALUE_FORMAT='JSON', PARTITIONS=${PAR_DEFAULT})
    AS
     SELECT SITE,
        TOPK(5, SUM(VIDEOVIEWS / VIDEOCOMPLETES))   AS VCR
        TOPK(5, SUM(VIDEOVIEWS/IMPRESSIONS))        AS VTR
       FROM MediaCampaign WINDOW TUMBLING (SIZE 1 HOUR)
       GROUP BY SITE
    EMIT CHANGES
    ;

 -- *** % of people has started watching the video but did not completed ***

CREATE OR REPLACE STREAM PEOPLE_WATCHED_BUT_NOT_COMPLITED_JSON
 WITH (KAFKA_TOPIC ='PEOPLE_WATCHED_BUT_NOT_COMPLITED_JSON' , VALUE_FORMAT='JSON', PARTITIONS=${PAR_DEFAULT})
    AS
      select campaign, partner
            , sum(videoCompletes)  AS totalVideoCompleted
            , sum(videoViews) AS totalVideoViews

            , 100 * totalVideoCompleted / totalVideoViews AS NotCompletedPercent

       from MediaCampaign WINDOW TUMBLING (SIZE 1 HOUR)
       group by campaign, partner

    EMIT CHANGES
    ;

