CREATE TABLE watchlist_hits(
`objectId` varchar(16) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
`wl_id` int,
`cone_id` int(11),
`arcsec` float,
`name` varchar(80),
PRIMARY KEY (`objectId`, `cone_id`)
)