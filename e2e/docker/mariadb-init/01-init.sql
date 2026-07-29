USE mes_test;

-- Grant replication privileges to the user created by MARIADB_USER env var
CREATE USER IF NOT EXISTS 'repl_user'@'%' IDENTIFIED BY 'test_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_user'@'%';
GRANT SELECT ON mes_test.* TO 'repl_user'@'%';

-- User with empty password (MariaDB uses mysql_native_password by default)
CREATE USER IF NOT EXISTS 'empty_pass_user'@'%' IDENTIFIED BY '';
GRANT SELECT ON mes_test.* TO 'empty_pass_user'@'%';

-- User with no replication privileges
CREATE USER IF NOT EXISTS 'no_repl_user'@'%' IDENTIFIED BY 'no_repl_pass';
GRANT SELECT ON mes_test.* TO 'no_repl_user'@'%';

FLUSH PRIVILEGES;

-- Test table with various column types (same schema as MySQL)
CREATE TABLE IF NOT EXISTS users (
    id BIGINT NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    age INT,
    balance DECIMAL(10, 2),
    score DOUBLE,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    bio TEXT,
    avatar BLOB,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    INDEX idx_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Simple table for basic tests
CREATE TABLE IF NOT EXISTS items (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    value INT NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Table for DDL tests
CREATE TABLE IF NOT EXISTS ddl_test (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    val VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Table for large data tests
CREATE TABLE IF NOT EXISTS large_data (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    big_text LONGTEXT,
    big_blob LONGBLOB
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Exercises TABLE_MAP SIGNEDNESS bit accounting: YEAR must consume a bit
-- before the signed and unsigned BIGINT columns.
CREATE TABLE IF NOT EXISTS signedness_values (
    y YEAR NOT NULL,
    signed_value BIGINT NOT NULL,
    unsigned_value BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (y)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS charset_values (
    id INT NOT NULL PRIMARY KEY,
    binary_value BINARY(16) NOT NULL,
    text_value LONGTEXT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS type_mapping_values (
    id INT NOT NULL PRIMARY KEY,
    enum_value ENUM('first', 'second') NOT NULL,
    set_value SET('a', 'b', 'c') NOT NULL,
    bit_value BIT(8) NOT NULL,
    unsigned_value BIGINT UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS extended_type_values (
    id INT NOT NULL PRIMARY KEY,
    tiny_unsigned TINYINT UNSIGNED NOT NULL,
    small_unsigned SMALLINT UNSIGNED NOT NULL,
    medium_unsigned MEDIUMINT UNSIGNED NOT NULL,
    int_unsigned INT UNSIGNED NOT NULL,
    year_value YEAR NOT NULL,
    date_value DATE NOT NULL,
    time_value TIME(6) NOT NULL,
    float_value FLOAT NOT NULL,
    wide_char CHAR(255) NOT NULL,
    binary_value VARBINARY(16) NOT NULL,
    geometry_value GEOMETRY NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
