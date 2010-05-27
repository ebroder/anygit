DROP TABLE IF EXISTS `git_object_repositories`;
CREATE TABLE `git_object_repositories` (
  `git_object_id` binary(40) NOT NULL,
  `repository_id` binary(40) NOT NULL,
  PRIMARY KEY `git_object_id` (`git_object_id`,`repository_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS `repository_remote_heads`;
CREATE TABLE `repository_remote_heads` (
  `repository_id` binary(40) NOT NULL,
  `remote_head_id` binary(40) NOT NULL,
  PRIMARY KEY `repository_id` (`repository_id`,`remote_head_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


DROP TABLE IF EXISTS `blob_trees`;
CREATE TABLE `blob_trees` (
  `blob_id` binary(40) NOT NULL,
  `tree_id` binary(40) NOT NULL,
  `name` varchar(3000) COLLATE utf8_bin NOT NULL,
  `mode` int(11) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY `blob_id` (`blob_id`,`tree_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS `blob_tags`;
CREATE TABLE `blob_tags` (
  `blob_id` binary(40) NOT NULL,
  `tag_id` binary(40) NOT NULL,
  PRIMARY KEY `blob_id` (`blob_id`,`tag_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS `tree_parent_trees`;
CREATE TABLE `tree_parent_trees` (
  `tree_id` binary(40) NOT NULL,
  `parent_tree_id` binary(40) NOT NULL,
  `name` varchar(3000) COLLATE utf8_bin NOT NULL,
  `mode` int(11) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY `tree_id` (`tree_id`,`parent_tree_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS `tree_commits`;
CREATE TABLE `tree_commits` (
  `tree_id` binary(40) NOT NULL,
  `commit_id` binary(40) NOT NULL,
  PRIMARY KEY `tree_id` (`tree_id`,`commit_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS `tree_tags`;
CREATE TABLE `tree_tags` (
  `tree_id` binary(40) NOT NULL,
  `tag_id` binary(40) NOT NULL,
  PRIMARY KEY `tree_id` (`tree_id`,`tag_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS `commit_parent_commits`;
CREATE TABLE `commit_parent_commits` (
  `commit_id` binary(40) NOT NULL,
  `parent_commit_id` binary(40) NOT NULL,
  PRIMARY KEY `commit_id` (`commit_id`,`parent_commit_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS `commit_trees`;
CREATE TABLE `commit_trees` (
  `commit_id` binary(40) NOT NULL,
  `tree_id` binary(40) NOT NULL,
  `name` varchar(3000) COLLATE utf8_bin NOT NULL,
  `mode` int(11) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY `commit_id` (`commit_id`,`tree_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS `commit_tags`;
CREATE TABLE `commit_tags` (
  `commit_id` binary(40) NOT NULL,
  `tag_id` binary(40) NOT NULL,
  PRIMARY KEY `commit_id` (`commit_id`,`tag_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS `tag_parent_tags`;
CREATE TABLE `tag_parent_tags` (
  `tag_id` binary(40) NOT NULL,
  `parent_tag_id` binary(40) NOT NULL,
  PRIMARY KEY `tag_id` (`tag_id`,`parent_tag_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


DROP TABLE IF EXISTS `git_objects`;
CREATE TABLE `git_objects` (
  `id` binary(40) NOT NULL,
  `dirty` tinyint(1),
  `type` varchar(20) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


DROP TABLE IF EXISTS `repositories`;
CREATE TABLE `repositories` (
  `id` binary(40) NOT NULL,
  `url` varchar(3000) COLLATE utf8_bin NOT NULL,
  `been_indexed` tinyint(1),
  `last_index` datetime,
  `indexing` tinyint(1) NOT NULL DEFAULT 0,
  `approved` varchar(20) NOT NULL DEFAULT 'spidered',
  `count` int(11) NOT NULL DEFAULT 0,
  `_remote_heads` MEDIUMTEXT,
  `_new_remote_heads` MEDIUMTEXT,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


DROP TABLE IF EXISTS `git_object_repositories`;
CREATE TABLE `git_object_repositories` (
  `git_object_id` binary(40) NOT NULL,
  `repository_id` binary(40) NOT NULL,
  PRIMARY KEY (`git_object_id`, `repository_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS `commit_parent_commits`;
CREATE TABLE `commit_parent_commits` (
  `commit_id` binary(40) NOT NULL,
  `parent_commit_id` binary(40) NOT NULL,
  PRIMARY KEY (`commit_id`, `parent_commit_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS `aggregate`;
CREATE TABLE `aggregate` (
    `id` VARCHAR(20) NOT NULL,
    `indexed_repository_count` INT(11) DEFAULT 0,
    `blob_count`  INT(11) DEFAULT 0,
    `tree_count` INT(11) DEFAULT 0,
    `commit_count` INT(11) DEFAULT 0,
    `tag_count` INT(11) DEFAULT 0,
    PRIMARY KEY `id` (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
