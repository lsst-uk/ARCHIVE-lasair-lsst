USE ztf;
CREATE TABLE IF NOT EXISTS area_hits(
`objectId` varchar(16) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
`ar_id` int,
PRIMARY KEY (`objectId`)
)