DROP TABLE IF EXISTS `obs_queue`;
CREATE TABLE `obs_queue` (
  `obs_datetime` datetime NOT NULL,
  `value_numeric` double DEFAULT NULL,
  `value_text` text,
  `observation_uuid` char(38) NOT NULL,
  `concept_uuid` char(38) NOT NULL,
  `patient_uuid` char(38) NOT NULL,
  `encounter_uuid` char(38) NOT NULL,
  `src` char(16) NOT NULL,
  `row_id` integer() NOT NULL
);

