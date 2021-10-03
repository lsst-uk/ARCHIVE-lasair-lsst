USE ztf;
CREATE TABLE IF NOT EXISTS crossmatch_tns(
`ra` double,
`decl` double,
`tns_name` varchar(16),
`tns_prefix` varchar(16),
`disc_mag` float,
`disc_mag_filter` varchar(16),
`type` varchar(16),
`z` float,
`hostz` float,
`host_name` varchar(16),
`disc_instruments` varchar(16),
`sender` varchar(16),
`associated_groups` varchar(16),
`classifying_groups` varchar(16),
`discovering_groups` varchar(16),
`class_instrument` varchar(16),
`disc_int_name` varchar(16),
`ext_catalogs` varchar(16),
`disc_date` datetime(6)
)