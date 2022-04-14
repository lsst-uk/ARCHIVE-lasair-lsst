CREATE TABLE IF NOT EXISTS annotations(
`annotationID` int NOT NULL AUTO_INCREMENT,
`objectId` varchar(16) NOT NULL,
`topic` varchar(16),
`version` varchar(16) DEFAULT 0.1,
`timestamp` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
`classification` varchar(16) NOT NULL,
`explanation` text,
`classdict` JSON,
`url` text DEFAULT NULL,
PRIMARY KEY    (annotationID),
UNIQUE KEY     one_per_object (objectId, topic)
)
