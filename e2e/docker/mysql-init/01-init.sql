USE mes_test;

-- Ensure root uses mysql_native_password for PyMySQL compatibility
ALTER USER 'root'@'%' IDENTIFIED WITH mysql_native_password BY 'test_root_password';
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'test_root_password';
FLUSH PRIVILEGES;

-- Create replication user
CREATE USER IF NOT EXISTS 'repl_user'@'%' IDENTIFIED WITH mysql_native_password BY 'test_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_user'@'%';
GRANT SELECT ON mes_test.* TO 'repl_user'@'%';
FLUSH PRIVILEGES;

-- Test table with various column types
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
