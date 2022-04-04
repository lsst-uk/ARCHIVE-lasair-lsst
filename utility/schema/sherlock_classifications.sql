CREATE TABLE IF NOT EXISTS sherlock_classifications(
`objectId` varchar(16) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
`classification` varchar(16),
`association_type` varchar(16),
`catalogue_table_name` varchar(80),
`catalogue_object_id` varchar(80),
`catalogue_object_type` varchar(16),
`raDeg` double,
`decDeg` double,
`separationArcsec` float,
`northSeparationArcsec` float,
`eastSeparationArcsec` float,
`physical_separation_kpc` float,
`direct_distance` float,
`distance` float,
`z` float,
`photoZ` float,
`photoZErr` float,
`Mag` float,
`MagFilter` varchar(16),
`MagErr` float,
`classificationReliability` int,
`major_axis_arcsec` float,
`annotator` varchar(80),
`additional_output` varchar(80),
`description` text,
`summary` varchar(80),
PRIMARY KEY (`objectId`)
)
