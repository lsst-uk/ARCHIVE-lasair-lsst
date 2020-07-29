CREATE TABLE sherlock_classifications(
`id` int NOT NULL AUTO_INCREMENT,
`objectId` varchar(16) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
`classification` varchar(16),
`description` text,
`summary` varchar(80),
`separation` float,
`z` float,
`catalogue_object_type` varchar(16),
PRIMARY KEY (`id`),
KEY `key_objectId` (`objectId`)
)
