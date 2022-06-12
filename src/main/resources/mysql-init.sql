-- Creates test user info and grants privileges.
CREATE USER 'reader'@'localhost' IDENTIFIED BY 'password';
GRANT SELECT, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'reader' IDENTIFIED BY 'password';
FLUSH PRIVILEGES;

CREATE USER 'inserter'@'localhost' IDENTIFIED BY 'password';
GRANT SELECT, UPDATE, INSERT, DELETE ON *.* TO 'inserter' IDENTIFIED BY 'password';
FLUSH PRIVILEGES;

-- Create the `example` database.
DROP
    DATABASE IF EXISTS `example_db`;
CREATE
    DATABASE `example_db` CHARSET = utf8;

-- Uses `example_db` database.
USE `example_db`;

-- Create test tables.
DROP TABLE IF EXISTS `Customers`;
CREATE TABLE `Customers`
(
    `c_id`   BIGINT NOT NULL,
    `c_name` VARCHAR(200),
    PRIMARY KEY (`c_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

INSERT INTO Customers
VALUES (1, 'Alice');
INSERT INTO Customers
VALUES (2, 'Bob');
INSERT INTO Customers
VALUES (3, 'Kyle');

DROP TABLE IF EXISTS `Transactions`;
CREATE TABLE `Transactions`
(
    `t_id`          BIGINT NOT NULL,
    `t_amount`      BIGINT NOT NULL,
    `t_customer_id` BIGINT NOT NULL,
    PRIMARY KEY (`t_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

INSERT INTO Transactions
VALUES (1, 200, 1);
INSERT INTO Transactions
VALUES (2, 89, 1);
INSERT INTO Transactions
VALUES (3, 56, 2);