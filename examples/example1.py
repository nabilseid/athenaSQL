from athenaSQL import Athena, TempTable
import athenaSQL.functions as F
from athenaSQL.functions import col
from athenaSQL.queries import withTable
from athenaSQL.data_type import DataType

merged_tracking_ttd = Athena('adludio_curated_zone').table('merged_tracking_ttd')

is_event = lambda _col : F.when(col(_col).isNotNull(), 1).otherwise(0)

cleaned_data = merged_tracking_ttd.select(
    col('date'),
    F.coalesce(F.replace(col('advertiser_id'), '�', ''), col('advertiserid')).alias('_advertiser_id'),
    F.coalesce(F.replace(col('campaign_id'), '�', ''), col('campaignid')).alias('_campaign_id'),
    F.coalesce(F.replace(col('creative_id'), '�', ''), col('creativeid')).alias('_creative_id'),
    F.coalesce(F.replace(col('line_item_id'), '�', ''), col('adgroupid')).alias('_line_item_id'),
    F.replace(col('game_key'), '�', '').alias('game_key'),
    F.lower(F.coalesce(F.replace(col('geo_city'), '�', ''), col('city'))).alias('city'),
    F.lower(F.coalesce(F.replace(col('geo_region'), '�', ''), col('region'))).alias('region'),
    F.lower(F.coalesce(F.replace(col('geo_country'), '�', ''), col('countrylong'))).alias('country'),
    F.concat(col('width'), 'x', col('height')).alias('ad_format'),
    F.replace(col('environment'), '�', '').alias('environment'),
    F.coalesce(col('browser'), 'Other').alias('browser'),
    (F.when(col('platform_os') == '1', 'Other')
    .when(col('platform_os') == '2', 'WINDOWS')
    .when(col('platform_os') == '3', 'Mac OS X')
    .when(col('platform_os') == '4', 'Linux')
    .when(col('platform_os') == '5', 'iOS')
    .when(col('platform_os') == '6', 'Android')
    .when(col('platform_os') == '7', 'Windows')
    .otherwise(F.coalesce(col('platform_os'), 'Other'))
    .alias('os')),
    F.coalesce(col('device_make'), 'Other').alias('device_make'),
    F.coalesce(col('device_type'), 'Other').alias('device_type'),
    F.coalesce(col('renderingcontext'), 'Other').alias('rendering_context'),
    
    (F.when(F.substring(col('site_name'), 1, 5) == 'http:', F.url_extract_host(col('site_name')))
    .when(F.substring(col('site_name'), 1, 6) == 'https:', F.url_extract_host(col('site_name')))
    .when(F.regexp_like(
        F.concat('https://', col('site_name')),
        '^https?:\/\/(?:www\.|[^/]*\.)?[a-zA-Z0-9.-]{1,253}\.[a-zA-Z]{2,20}(?:[-a-zA-Z0-9()@:%_+~#?&/=]*)$'
        ), F.url_extract_host(F.concat('https://', col('site_name'))))
    .when(F.regexp_like(col('site_name'), '^[a-zA-Z][a-zA-Z0-9_]*(\.[a-zA-Z][a-zA-Z0-9_]*)+$'),
        col('site_name'))
    .when(F.regexp_like(col('site_name'), '^[0-9]+'), col('site_name'))
    .otherwise('Other')
    .alias('site')),
    
    is_event('impression').alias('impression'),
    is_event('first_dropped').alias('engagement'),
    is_event('click_through').alias('click'),
    is_event('video_start').alias('video_start'),
    is_event('video_end').alias('video_end'),
    
    F.when(col('creativewasviewable') == 'True', 1).otherwise(0).alias('viewable'),
    F.when(col('creativeistrackable') == 'True', 1).otherwise(0).alias('trackable'),
    
    F.round(F.date_diff(
        'millisecond',
        F.date_trunc('millisecond', F.from_iso8601_timestamp(col('rendered'))),
        F.date_trunc('millisecond', F.from_iso8601_timestamp(col('impression'))))
    ).alias('rendered_duration_milliseconds'),
    
    F.round(F.date_diff(
        'second',
        F.date_trunc('second', F.from_iso8601_timestamp(col('impression'))),
        F.date_trunc('second', F.from_iso8601_timestamp(col('first_dropped'))))
    ).alias('eng_duration'),
    
    F.round(F.date_diff(
        'second',
        F.date_trunc('second', F.from_iso8601_timestamp(col('impression'))),
        F.date_trunc('second', F.from_iso8601_timestamp(col('click_through_event'))))
    ).alias('click_duration'),
    
    F.round(F.date_diff(
        'second',
        F.date_trunc('second', F.from_iso8601_timestamp(col('first_dropped'))),
        F.date_trunc('second', F.from_iso8601_timestamp(col('click_through_event'))))
    ).alias('dropped_click_duration'),
    
    (F.when(F.try_cast(col('matchedfoldposition'), DataType.INT) == 1, 'Any')
    .when(F.try_cast(col('matchedfoldposition'), DataType.INT) == 2, 'Above')
    .when(F.try_cast(col('matchedfoldposition'), DataType.INT) == 3, 'Below')
    .when(F.try_cast(col('matchedfoldposition'), DataType.INT) == 4, 'Unknown')
    .otherwise('Unknown')
    .alias('matchedfoldposition')),

    col('advertisercurrencyexchangeratefromusd'),
    col('advertisercostinusdollars')
)

clean_id = lambda x : F.ifTrue(
    F.regexp_like(col(x), '^[A-Za-z0-9]+$'),
    col(x),
    'other')

cleaned_unknown = TempTable('cleaned_data').select(
    col('*'),
    clean_id('_advertiser_id').alias('advertiser_id'),
    clean_id('_campaign_id').alias('campaign_id'),
    clean_id('_line_item_id').alias('line_item_id'),
    clean_id('_creative_id').alias('creative_id'),
)

aggregated_data = TempTable('cleaned_unknown').select(
    'advertiser_id',
    'campaign_id',
    'creative_id',
    'line_item_id',
    'game_key',
    'city',
    'region',
    'country',
    'ad_format',
    'environment',
    'browser',
    'os',
    'device_make',
    'device_type',
    'rendering_context',
    'matchedfoldposition',
    'site',
    F.sum('impression').alias('impressions'),
    F.sum('engagement').alias('engagements'),
    F.sum('click').alias('clicks'),
    F.sum('video_start').alias('video_starts'),
    F.sum('video_end').alias('video_ends'),
    F.sum('viewable').alias('viewable'),
    F.sum('trackable').alias('trackable'),

    F.sum(
        F.when(col('rendered_duration_milliseconds').between(0, 600000), 1).otherwise(0)
    ).alias('quality_impressions'),
    
    F.sum(
        F.when(col('eng_duration').between(0, 30), 1).otherwise(0)
    ).alias('quality_engagements'),

    F.sum(
        F.when(col('eng_duration').between(0, 30) & 
        col('click_duration').between(0, 600), 1).otherwise(0)
    ).alias('quality_checks'),

    F.sum(
        F.when(
            col('eng_duration').between(0, 30) & 
            col('click_duration').between(0, 600) &
            col('dropped_click_duration').between(0, 600),
        1).otherwise(0)
    ).alias('quality_dropped_click'),

    F.sum(F.when(col('eng_duration').between(0, 600), 1).otherwise(0)).alias('standard_engagements'),

    F.sum(
        F.when(
            col('eng_duration').between(0, 600) &
            col('click_duration').between(0, 600),
            1).otherwise(0)
    ).alias('standard_clicks'),
    
    F.sum(
        F.when(
            col('eng_duration').between(0, 600) &
            col('click_duration').between(0, 600) &
            col('dropped_click_duration').between(0, 600)
            , 1).otherwise(0)
    ).alias('standard_dropped_click'),

    F.sum(
        F.ifTrue(
            col('rendered_duration_milliseconds') < 0,
            0,
            col('rendered_duration_milliseconds')
        )
    ).alias('otal_rendered_duration_milliseconds'),
    
    F.sum(F.ifTrue(col('eng_duration') < 0, 0, col('eng_duration'))).alias('total_eng_duration'),

    F.sum(F.ifTrue(col('click_duration') < 0, 0, col('click_duration'))).alias('total_click_duration'),
    
    F.sum(F.ifTrue(col('dropped_click_duration') < 0, 0, col('dropped_click_duration'))).alias('total_dropped_click_duration'),
    
    F.sum(F.when(col('eng_duration').between(0, 30), col('eng_duration')).otherwise('0')).alias('quality_eng_duration'),
    
    F.sum(F.when(col('eng_duration').between(0, 30) & col('click_duration').between(0, 600), col('click_duration')).otherwise(0)).alias('quality_click_duration'),
    
    F.sum(F.when(col('eng_duration').between(0, 30) & col('click_duration').between(0, 600) & col('dropped_click_duration').between(0, 600), col('eng_duration')).otherwise(0)).alias('quality_dropped_click_duration'),
    
    F.sum(F.when(col('eng_duration').between(0, 600), col('eng_duration')).otherwise(0)).alias('standard_engagements_duration'),
    
    F.sum(F.when(col('eng_duration').between(0, 600) & col('click_duration').between(0, 600), col('click_duration')).otherwise(0)).alias('standard_clicks_duration'),
    F.sum(F.when(col('eng_duration').between(0, 600) & col('click_duration').between(0, 600) & col('dropped_click_duration').between(0, 600), col('eng_duration')).otherwise(0)).alias('standard_dropped_click_duration'),
    F.sum(F.try_cast(col('advertisercurrencyexchangeratefromusd'), DataType.DOUBLE)).alias('max_currency_rate'),
    F.sum(F.try_cast(col('advertisercostinusdollars'), DataType.DOUBLE)).alias('max_advertiser_cost_usd'),
    col('date')
)

# TODO
# doc coalesce
# fix when condition
# support prentesis enclosed artimetic opertations
# get col from table ***
# changelog
# release

cta_query = (withTable('cleaned_data', cleaned_data)
                .withTable('cleaned_unknown', cleaned_unknown)
                .withTable('aggregated_data', aggregated_data)
                .table('aggregated_data').select()
            )

analytics_sanitized = Athena('adludio_curated_zone').table('analytics_sanitized')

analytics_sanitized.insert(cta_query).show_query()
